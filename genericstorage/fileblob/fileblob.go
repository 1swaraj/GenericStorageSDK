package fileblob

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/genericstorage"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/escape"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
)

const defaultPageSize = 1000

func init() {
	genericstorage.DefaultURLMux().RegisterBucket(Scheme, &URLOpener{})
}

const Scheme = "file"

type URLOpener struct {
	Options Options
}

func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*genericstorage.Bucket, error) {
	path := u.Path

	if u.Host == "." || os.PathSeparator != '/' {
		path = strings.TrimPrefix(path, "/")
	}
	opts, err := o.forParams(ctx, u.Query())
	if err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, err)
	}
	return OpenBucket(filepath.FromSlash(path), opts)
}

func (o *URLOpener) forParams(ctx context.Context, q url.Values) (*Options, error) {
	for k := range q {
		if k != "create_dir" && k != "base_url" && k != "secret_key_path" {
			return nil, fmt.Errorf("invalid query parameter %q", k)
		}
	}
	opts := new(Options)
	*opts = o.Options

	if q.Get("create_dir") != "" {
		opts.CreateDir = true
	}
	baseURL := q.Get("base_url")
	keyPath := q.Get("secret_key_path")
	if (baseURL == "") != (keyPath == "") {
		return nil, errors.New("must supply both base_url and secret_key_path query parameters")
	}
	if baseURL != "" {
		burl, err := url.Parse(baseURL)
		if err != nil {
			return nil, err
		}
		sk, err := ioutil.ReadFile(keyPath)
		if err != nil {
			return nil, err
		}
		opts.URLSigner = NewURLSignerHMAC(burl, sk)
	}
	return opts, nil
}

type Options struct {
	URLSigner URLSigner

	CreateDir bool
}

type bucket struct {
	dir  string
	opts *Options
}

func openBucket(dir string, opts *Options) (driver.Bucket, error) {
	if opts == nil {
		opts = &Options{}
	}
	absdir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to convert %s into an absolute path: %v", dir, err)
	}
	info, err := os.Stat(absdir)

	if err != nil && opts.CreateDir && os.IsNotExist(err) {
		err = os.MkdirAll(absdir, os.ModeDir)
		if err != nil {
			return nil, fmt.Errorf("tried to create directory but failed: %v", err)
		}
		info, err = os.Stat(absdir)
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", absdir)
	}
	return &bucket{dir: absdir, opts: opts}, nil
}

func OpenBucket(dir string, opts *Options) (*genericstorage.Bucket, error) {
	drv, err := openBucket(dir, opts)
	if err != nil {
		return nil, err
	}
	return genericstorage.NewBucket(drv), nil
}

func (b *bucket) Close() error {
	return nil
}

func escapeKey(s string) string {
	s = escape.HexEscape(s, func(r []rune, i int) bool {
		c := r[i]
		switch {
		case c < 32:
			return true

		case os.PathSeparator != '/' && c == os.PathSeparator:
			return true

		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true

		case i > 0 && c == '/' && r[i-1] == '/':
			return true

		case c == '/' && i == len(r)-1:
			return true

		case os.PathSeparator == '\\' && (c == '>' || c == '<' || c == ':' || c == '"' || c == '|' || c == '?' || c == '*'):
			return true
		}
		return false
	})

	if os.PathSeparator != '/' {
		s = strings.Replace(s, "/", string(os.PathSeparator), -1)
	}
	return s
}

func unescapeKey(s string) string {
	if os.PathSeparator != '/' {
		s = strings.Replace(s, string(os.PathSeparator), "/", -1)
	}
	s = escape.HexUnescape(s)
	return s
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case os.IsNotExist(err):
		return gcerrors.NotFound
	default:
		return gcerrors.Unknown
	}
}

func (b *bucket) path(key string) (string, error) {
	path := filepath.Join(b.dir, escapeKey(key))
	if strings.HasSuffix(path, attrsExt) {
		return "", errAttrsExt
	}
	return path, nil
}

func (b *bucket) forKey(key string) (string, os.FileInfo, *xattrs, error) {
	path, err := b.path(key)
	if err != nil {
		return "", nil, nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, nil, err
	}
	if info.IsDir() {
		return "", nil, nil, os.ErrNotExist
	}
	xa, err := getAttrs(path)
	if err != nil {
		return "", nil, nil, err
	}
	return path, info, &xa, nil
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {

	var pageToken string
	if len(opts.PageToken) > 0 {
		pageToken = string(opts.PageToken)
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	var lastPrefix string

	root := b.dir
	if i := strings.LastIndex(opts.Prefix, "/"); i > -1 {
		root = filepath.Join(root, opts.Prefix[:i])
	}

	var result driver.ListPage
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {

			return nil
		}

		if strings.HasSuffix(path, attrsExt) {
			return nil
		}

		if path == b.dir {
			return nil
		}

		prefixLen := len(b.dir)

		if b.dir != "/" {
			prefixLen++
		}
		path = path[prefixLen:]

		key := unescapeKey(path)

		if info.IsDir() {
			key += "/"

			if len(key) > len(opts.Prefix) && !strings.HasPrefix(key, opts.Prefix) {
				return filepath.SkipDir
			}

			if lastPrefix != "" && strings.HasPrefix(key, lastPrefix) {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasPrefix(key, opts.Prefix) {
			return nil
		}
		var md5 []byte
		if xa, err := getAttrs(path); err == nil {

			md5 = xa.MD5
		}
		asFunc := func(i interface{}) bool {
			p, ok := i.(*os.FileInfo)
			if !ok {
				return false
			}
			*p = info
			return true
		}
		obj := &driver.ListObject{
			Key:     key,
			ModTime: info.ModTime(),
			Size:    info.Size(),
			MD5:     md5,
			AsFunc:  asFunc,
		}

		if opts.Delimiter != "" {

			keyWithoutPrefix := key[len(opts.Prefix):]

			if idx := strings.Index(keyWithoutPrefix, opts.Delimiter); idx != -1 {
				prefix := opts.Prefix + keyWithoutPrefix[0:idx+len(opts.Delimiter)]

				if prefix == lastPrefix {
					return nil
				}

				obj = &driver.ListObject{
					Key:    prefix,
					IsDir:  true,
					AsFunc: asFunc,
				}
				lastPrefix = prefix
			}
		}

		if pageToken != "" && obj.Key <= pageToken {
			return nil
		}

		if len(result.Objects) == pageSize {
			result.NextPageToken = []byte(result.Objects[pageSize-1].Key)
			return io.EOF
		}
		result.Objects = append(result.Objects, obj)
		return nil
	})
	if err != nil && err != io.EOF {
		return nil, err
	}
	return &result, nil
}

func (b *bucket) As(i interface{}) bool {
	p, ok := i.(*os.FileInfo)
	if !ok {
		return false
	}
	fi, err := os.Stat(b.dir)
	if err != nil {
		return false
	}
	*p = fi
	return true
}

func (b *bucket) ErrorAs(err error, i interface{}) bool {
	if perr, ok := err.(*os.PathError); ok {
		if p, ok := i.(**os.PathError); ok {
			*p = perr
			return true
		}
	}
	return false
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	_, info, xa, err := b.forKey(key)
	if err != nil {
		return nil, err
	}
	return &driver.Attributes{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		ContentType:        xa.ContentType,
		Metadata:           xa.Metadata,

		ModTime: info.ModTime(),
		Size:    info.Size(),
		MD5:     xa.MD5,
		ETag:    fmt.Sprintf("\"%x-%x\"", info.ModTime().UnixNano(), info.Size()),
		AsFunc: func(i interface{}) bool {
			p, ok := i.(*os.FileInfo)
			if !ok {
				return false
			}
			*p = info
			return true
		},
	}, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	path, info, xa, err := b.forKey(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(func(i interface{}) bool {
			p, ok := i.(**os.File)
			if !ok {
				return false
			}
			*p = f
			return true
		}); err != nil {
			return nil, err
		}
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	r := io.Reader(f)
	if length >= 0 {
		r = io.LimitReader(r, length)
	}
	return &reader{
		r: r,
		c: f,
		attrs: driver.ReaderAttributes{
			ContentType: xa.ContentType,
			ModTime:     info.ModTime(),
			Size:        info.Size(),
		},
	}, nil
}

type reader struct {
	r     io.Reader
	c     io.Closer
	attrs driver.ReaderAttributes
}

func (r *reader) Read(p []byte) (int, error) {
	if r.r == nil {
		return 0, io.EOF
	}
	return r.r.Read(p)
}

func (r *reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) As(i interface{}) bool {
	p, ok := i.(*io.Reader)
	if !ok {
		return false
	}
	*p = r.r
	return true
}

func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	path, err := b.path(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return nil, err
	}
	f, err := ioutil.TempFile(filepath.Dir(path), "fileblob")
	if err != nil {
		return nil, err
	}
	if opts.BeforeWrite != nil {
		if err := opts.BeforeWrite(func(i interface{}) bool {
			p, ok := i.(**os.File)
			if !ok {
				return false
			}
			*p = f
			return true
		}); err != nil {
			return nil, err
		}
	}
	var metadata map[string]string
	if len(opts.Metadata) > 0 {
		metadata = opts.Metadata
	}
	attrs := xattrs{
		CacheControl:       opts.CacheControl,
		ContentDisposition: opts.ContentDisposition,
		ContentEncoding:    opts.ContentEncoding,
		ContentLanguage:    opts.ContentLanguage,
		ContentType:        contentType,
		Metadata:           metadata,
	}
	w := &writer{
		ctx:        ctx,
		f:          f,
		path:       path,
		attrs:      attrs,
		contentMD5: opts.ContentMD5,
		md5hash:    md5.New(),
	}
	return w, nil
}

type writer struct {
	ctx        context.Context
	f          *os.File
	path       string
	attrs      xattrs
	contentMD5 []byte

	md5hash hash.Hash
}

func (w *writer) Write(p []byte) (n int, err error) {
	if _, err := w.md5hash.Write(p); err != nil {
		return 0, err
	}
	return w.f.Write(p)
}

func (w *writer) Close() error {
	err := w.f.Close()
	if err != nil {
		return err
	}

	defer func() {
		_ = os.Remove(w.f.Name())
	}()

	if err := w.ctx.Err(); err != nil {
		return err
	}

	md5sum := w.md5hash.Sum(nil)
	w.attrs.MD5 = md5sum

	if err := setAttrs(w.path, w.attrs); err != nil {
		return err
	}

	if err := os.Rename(w.f.Name(), w.path); err != nil {
		_ = os.Remove(w.path + attrsExt)
		return err
	}
	return nil
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {

	srcPath, _, xa, err := b.forKey(srcKey)
	if err != nil {
		return err
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	wopts := driver.WriterOptions{
		CacheControl:       xa.CacheControl,
		ContentDisposition: xa.ContentDisposition,
		ContentEncoding:    xa.ContentEncoding,
		ContentLanguage:    xa.ContentLanguage,
		Metadata:           xa.Metadata,
		BeforeWrite:        opts.BeforeCopy,
	}

	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := b.NewTypedWriter(writeCtx, dstKey, xa.ContentType, &wopts)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	if err != nil {
		cancel()
		w.Close()
		return err
	}
	return w.Close()
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	path, err := b.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if err != nil {
		return err
	}
	if err = os.Remove(path + attrsExt); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (b *bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	if b.opts.URLSigner == nil {
		return "", gcerr.New(gcerr.Unimplemented, nil, 1, "fileblob.SignedURL: bucket does not have an Options.URLSigner")
	}
	if opts.BeforeSign != nil {
		if err := opts.BeforeSign(func(interface{}) bool { return false }); err != nil {
			return "", err
		}
	}
	surl, err := b.opts.URLSigner.URLFromKey(ctx, key, opts)
	if err != nil {
		return "", err
	}
	return surl.String(), nil
}

type URLSigner interface {
	URLFromKey(ctx context.Context, key string, opts *driver.SignedURLOptions) (*url.URL, error)

	KeyFromURL(ctx context.Context, surl *url.URL) (string, error)
}

type URLSignerHMAC struct {
	baseURL   *url.URL
	secretKey []byte
}

func NewURLSignerHMAC(baseURL *url.URL, secretKey []byte) *URLSignerHMAC {
	if len(secretKey) == 0 {
		panic("creating URLSignerHMAC: secretKey is required")
	}
	uc := new(url.URL)
	*uc = *baseURL
	return &URLSignerHMAC{
		baseURL:   uc,
		secretKey: secretKey,
	}
}

func (h *URLSignerHMAC) URLFromKey(ctx context.Context, key string, opts *driver.SignedURLOptions) (*url.URL, error) {
	sURL := new(url.URL)
	*sURL = *h.baseURL

	q := sURL.Query()
	q.Set("obj", key)
	q.Set("expiry", strconv.FormatInt(time.Now().Add(opts.Expiry).Unix(), 10))
	q.Set("method", opts.Method)
	if opts.ContentType != "" {
		q.Set("contentType", opts.ContentType)
	}
	q.Set("signature", h.getMAC(q))
	sURL.RawQuery = q.Encode()

	return sURL, nil
}

func (h *URLSignerHMAC) getMAC(q url.Values) string {
	signedVals := url.Values{}
	signedVals.Set("obj", q.Get("obj"))
	signedVals.Set("expiry", q.Get("expiry"))
	signedVals.Set("method", q.Get("method"))
	if contentType := q.Get("contentType"); contentType != "" {
		signedVals.Set("contentType", contentType)
	}
	msg := signedVals.Encode()

	hsh := hmac.New(sha256.New, h.secretKey)
	hsh.Write([]byte(msg))
	return base64.RawURLEncoding.EncodeToString(hsh.Sum(nil))
}

func (h *URLSignerHMAC) KeyFromURL(ctx context.Context, sURL *url.URL) (string, error) {
	q := sURL.Query()

	exp, err := strconv.ParseInt(q.Get("expiry"), 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return "", errors.New("retrieving genericstorage key from URL: key cannot be retrieved")
	}

	if !h.checkMAC(q) {
		return "", errors.New("retrieving genericstorage key from URL: key cannot be retrieved")
	}
	return q.Get("obj"), nil
}

func (h *URLSignerHMAC) checkMAC(q url.Values) bool {
	mac := q.Get("signature")
	expected := h.getMAC(q)

	return hmac.Equal([]byte(mac), []byte(expected))
}
