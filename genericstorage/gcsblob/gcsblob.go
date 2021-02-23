package gcsblob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/storage"
	"github.com/google/wire"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/swaraj1802/GenericStorageSDK/genericstorage"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/gcp"
	"github.com/swaraj1802/GenericStorageSDK/internal/escape"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
	"github.com/swaraj1802/GenericStorageSDK/internal/useragent"
)

const defaultPageSize = 1000

func init() {
	genericstorage.DefaultURLMux().RegisterBucket(Scheme, new(lazyCredsOpener))
}

var Set = wire.NewSet(
	wire.Struct(new(URLOpener), "Client"),
)

func readDefaultCredentials(credFileAsJSON []byte) (AccessID string, PrivateKey []byte) {

	var contentVariantA struct {
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
	}
	if err := json.Unmarshal(credFileAsJSON, &contentVariantA); err == nil {
		AccessID = contentVariantA.ClientEmail
		PrivateKey = []byte(contentVariantA.PrivateKey)
	}
	if AccessID != "" {
		return
	}

	var contentVariantB struct {
		Name           string `json:"name"`
		PrivateKeyData string `json:"privateKeyData"`
	}
	if err := json.Unmarshal(credFileAsJSON, &contentVariantB); err == nil {
		nextFieldIsAccessID := false
		for _, s := range strings.Split(contentVariantB.Name, "/") {
			if nextFieldIsAccessID {
				AccessID = s
				break
			}
			nextFieldIsAccessID = s == "serviceAccounts"
		}
		PrivateKey = []byte(contentVariantB.PrivateKeyData)
	}

	return
}

type lazyCredsOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *lazyCredsOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*genericstorage.Bucket, error) {
	o.init.Do(func() {
		var opts Options
		var creds *google.Credentials
		if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
			creds, _ = google.CredentialsFromJSON(ctx, []byte(`{"type": "service_account", "project_id": "my-project-id"}`))
		} else {
			var err error
			creds, err = gcp.DefaultCredentials(ctx)
			if err != nil {
				o.err = err
				return
			}

			opts.GoogleAccessID, opts.PrivateKey = readDefaultCredentials(creds.JSON)

			if opts.GoogleAccessID == "" && metadata.OnGCE() {
				mc := metadata.NewClient(nil)
				opts.GoogleAccessID, _ = mc.Email("")
			}
		}

		if len(opts.PrivateKey) <= 0 && opts.GoogleAccessID != "" {
			iam := new(credentialsClient)

			ctx := context.Background()
			opts.MakeSignBytes = iam.CreateMakeSignBytesWith(ctx, opts.GoogleAccessID)
		}

		client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), creds.TokenSource)
		if err != nil {
			o.err = err
			return
		}
		o.opener = &URLOpener{Client: client, Options: opts}
	})
	if o.err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, o.err)
	}
	return o.opener.OpenBucketURL(ctx, u)
}

const Scheme = "gs"

type URLOpener struct {
	Client *gcp.HTTPClient

	Options Options
}

func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*genericstorage.Bucket, error) {
	opts, err := o.forParams(ctx, u.Query())
	if err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, err)
	}
	return OpenBucket(ctx, o.Client, u.Host, opts)
}

func (o *URLOpener) forParams(ctx context.Context, q url.Values) (*Options, error) {
	for k := range q {
		if k != "access_id" && k != "private_key_path" {
			return nil, fmt.Errorf("invalid query parameter %q", k)
		}
	}
	opts := new(Options)
	*opts = o.Options
	if accessID := q.Get("access_id"); accessID != "" && accessID != opts.GoogleAccessID {
		opts.GoogleAccessID = accessID
		opts.PrivateKey = nil

		opts.MakeSignBytes = nil
	}
	if keyPath := q.Get("private_key_path"); keyPath != "" {
		pk, err := ioutil.ReadFile(keyPath)
		if err != nil {
			return nil, err
		}
		opts.PrivateKey = pk
	} else if _, exists := q["private_key_path"]; exists {

		opts.PrivateKey = nil
	}
	return opts, nil
}

type Options struct {
	GoogleAccessID string

	PrivateKey []byte

	SignBytes func([]byte) ([]byte, error)

	MakeSignBytes func(requestCtx context.Context) SignBytesFunc
}

type SignBytesFunc func([]byte) ([]byte, error)

func openBucket(ctx context.Context, client *gcp.HTTPClient, bucketName string, opts *Options) (*bucket, error) {
	if client == nil {
		return nil, errors.New("gcsblob.OpenBucket: client is required")
	}
	if bucketName == "" {
		return nil, errors.New("gcsblob.OpenBucket: bucketName is required")
	}

	clientOpts := []option.ClientOption{option.WithHTTPClient(useragent.HTTPClient(&client.Client, "genericstorage"))}
	if host := os.Getenv("STORAGE_EMULATOR_HOST"); host != "" {
		clientOpts = []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint("http://" + host + "/storage/v1/"),
			option.WithHTTPClient(http.DefaultClient),
		}
	}

	c, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = &Options{}
	}
	return &bucket{name: bucketName, client: c, opts: opts}, nil
}

func OpenBucket(ctx context.Context, client *gcp.HTTPClient, bucketName string, opts *Options) (*genericstorage.Bucket, error) {
	drv, err := openBucket(ctx, client, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return genericstorage.NewBucket(drv), nil
}

type bucket struct {
	name   string
	client *storage.Client
	opts   *Options
}

var emptyBody = ioutil.NopCloser(strings.NewReader(""))

type reader struct {
	body  io.ReadCloser
	attrs driver.ReaderAttributes
	raw   *storage.Reader
}

func (r *reader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *reader) Close() error {
	return r.body.Close()
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

func (r *reader) As(i interface{}) bool {
	p, ok := i.(**storage.Reader)
	if !ok {
		return false
	}
	*p = r.raw
	return true
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	if err == storage.ErrObjectNotExist || err == storage.ErrBucketNotExist {
		return gcerrors.NotFound
	}
	if gerr, ok := err.(*googleapi.Error); ok {
		switch gerr.Code {
		case http.StatusForbidden:
			return gcerrors.PermissionDenied
		case http.StatusNotFound:
			return gcerrors.NotFound
		case http.StatusPreconditionFailed:
			return gcerrors.FailedPrecondition
		case http.StatusTooManyRequests:
			return gcerrors.ResourceExhausted
		}
	}
	return gcerrors.Unknown
}

func (b *bucket) Close() error {
	return nil
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	bkt := b.client.Bucket(b.name)
	query := &storage.Query{
		Prefix:    escapeKey(opts.Prefix),
		Delimiter: escapeKey(opts.Delimiter),
	}
	if opts.BeforeList != nil {
		asFunc := func(i interface{}) bool {
			p, ok := i.(**storage.Query)
			if !ok {
				return false
			}
			*p = query
			return true
		}
		if err := opts.BeforeList(asFunc); err != nil {
			return nil, err
		}
	}
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	iter := bkt.Objects(ctx, query)
	pager := iterator.NewPager(iter, pageSize, string(opts.PageToken))
	var objects []*storage.ObjectAttrs
	nextPageToken, err := pager.NextPage(&objects)
	if err != nil {
		return nil, err
	}
	page := driver.ListPage{NextPageToken: []byte(nextPageToken)}
	if len(objects) > 0 {
		page.Objects = make([]*driver.ListObject, len(objects))
		for i, obj := range objects {
			toCopy := obj
			asFunc := func(val interface{}) bool {
				p, ok := val.(*storage.ObjectAttrs)
				if !ok {
					return false
				}
				*p = *toCopy
				return true
			}
			if obj.Prefix == "" {

				page.Objects[i] = &driver.ListObject{
					Key:     unescapeKey(obj.Name),
					ModTime: obj.Updated,
					Size:    obj.Size,
					MD5:     obj.MD5,
					AsFunc:  asFunc,
				}
			} else {

				page.Objects[i] = &driver.ListObject{
					Key:    unescapeKey(obj.Prefix),
					IsDir:  true,
					AsFunc: asFunc,
				}
			}
		}

		sort.Slice(page.Objects, func(i, j int) bool {
			return page.Objects[i].Key < page.Objects[j].Key
		})
	}
	return &page, nil
}

func (b *bucket) As(i interface{}) bool {
	p, ok := i.(**storage.Client)
	if !ok {
		return false
	}
	*p = b.client
	return true
}

func (b *bucket) ErrorAs(err error, i interface{}) bool {
	switch v := err.(type) {
	case *googleapi.Error:
		if p, ok := i.(**googleapi.Error); ok {
			*p = v
			return true
		}
	}
	return false
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	key = escapeKey(key)
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	eTag := attrs.Etag
	if !strings.HasPrefix(eTag, "W/\"") && !strings.HasPrefix(eTag, "\"") && !strings.HasSuffix(eTag, "\"") {
		eTag = fmt.Sprintf("%q", eTag)
	}
	return &driver.Attributes{
		CacheControl:       attrs.CacheControl,
		ContentDisposition: attrs.ContentDisposition,
		ContentEncoding:    attrs.ContentEncoding,
		ContentLanguage:    attrs.ContentLanguage,
		ContentType:        attrs.ContentType,
		Metadata:           attrs.Metadata,
		CreateTime:         attrs.Created,
		ModTime:            attrs.Updated,
		Size:               attrs.Size,
		MD5:                attrs.MD5,
		ETag:               eTag,
		AsFunc: func(i interface{}) bool {
			p, ok := i.(*storage.ObjectAttrs)
			if !ok {
				return false
			}
			*p = *attrs
			return true
		},
	}, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	key = escapeKey(key)
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)

	objp := &obj
	makeReader := func() (*storage.Reader, error) {
		return (*objp).NewRangeReader(ctx, offset, length)
	}

	var r *storage.Reader
	var rerr error
	madeReader := false
	if opts.BeforeRead != nil {
		asFunc := func(i interface{}) bool {
			if p, ok := i.(***storage.ObjectHandle); ok && !madeReader {
				*p = objp
				return true
			}
			if p, ok := i.(**storage.Reader); ok {
				if !madeReader {
					r, rerr = makeReader()
					madeReader = true
					if r == nil {
						return false
					}
				}
				*p = r
				return true
			}
			return false
		}
		if err := opts.BeforeRead(asFunc); err != nil {
			return nil, err
		}
	}
	if !madeReader {
		r, rerr = makeReader()
	}
	if rerr != nil {
		return nil, rerr
	}
	return &reader{
		body: r,
		attrs: driver.ReaderAttributes{
			ContentType: r.Attrs.ContentType,
			ModTime:     r.Attrs.LastModified,
			Size:        r.Attrs.Size,
		},
		raw: r,
	}, nil
}

func escapeKey(key string) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		switch {

		case r[i] == 10 || r[i] == 13:
			return true

		case i > 1 && r[i] == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true
		}
		return false
	})
}

func unescapeKey(key string) string {
	return escape.HexUnescape(key)
}

func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	key = escapeKey(key)
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)

	objp := &obj
	makeWriter := func() *storage.Writer {
		w := (*objp).NewWriter(ctx)
		w.CacheControl = opts.CacheControl
		w.ContentDisposition = opts.ContentDisposition
		w.ContentEncoding = opts.ContentEncoding
		w.ContentLanguage = opts.ContentLanguage
		w.ContentType = contentType
		w.ChunkSize = bufferSize(opts.BufferSize)
		w.Metadata = opts.Metadata
		w.MD5 = opts.ContentMD5
		return w
	}

	var w *storage.Writer
	if opts.BeforeWrite != nil {
		asFunc := func(i interface{}) bool {
			if p, ok := i.(***storage.ObjectHandle); ok && w == nil {
				*p = objp
				return true
			}
			if p, ok := i.(**storage.Writer); ok {
				if w == nil {
					w = makeWriter()
				}
				*p = w
				return true
			}
			return false
		}
		if err := opts.BeforeWrite(asFunc); err != nil {
			return nil, err
		}
	}
	if w == nil {
		w = makeWriter()
	}
	return w, nil
}

type CopyObjectHandles struct {
	Dst, Src *storage.ObjectHandle
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	dstKey = escapeKey(dstKey)
	srcKey = escapeKey(srcKey)
	bkt := b.client.Bucket(b.name)

	handles := CopyObjectHandles{
		Dst: bkt.Object(dstKey),
		Src: bkt.Object(srcKey),
	}
	makeCopier := func() *storage.Copier {
		return handles.Dst.CopierFrom(handles.Src)
	}

	var copier *storage.Copier
	if opts.BeforeCopy != nil {
		asFunc := func(i interface{}) bool {
			if p, ok := i.(**CopyObjectHandles); ok && copier == nil {
				*p = &handles
				return true
			}
			if p, ok := i.(**storage.Copier); ok {
				if copier == nil {
					copier = makeCopier()
				}
				*p = copier
				return true
			}
			return false
		}
		if err := opts.BeforeCopy(asFunc); err != nil {
			return err
		}
	}
	if copier == nil {
		copier = makeCopier()
	}
	_, err := copier.Run(ctx)
	return err
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	key = escapeKey(key)
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)
	return obj.Delete(ctx)
}

func (b *bucket) SignedURL(ctx context.Context, key string, dopts *driver.SignedURLOptions) (string, error) {
	numSigners := 0
	if b.opts.PrivateKey != nil {
		numSigners++
	}
	if b.opts.SignBytes != nil {
		numSigners++
	}
	if b.opts.MakeSignBytes != nil {
		numSigners++
	}
	if b.opts.GoogleAccessID == "" || numSigners != 1 {
		return "", gcerr.New(gcerr.Unimplemented, nil, 1, "gcsblob: to use SignedURL, you must call OpenBucket with a valid Options.GoogleAccessID and exactly one of Options.PrivateKey, Options.SignBytes, or Options.MakeSignBytes")
	}

	key = escapeKey(key)
	opts := &storage.SignedURLOptions{
		Expires:        time.Now().Add(dopts.Expiry),
		Method:         dopts.Method,
		ContentType:    dopts.ContentType,
		GoogleAccessID: b.opts.GoogleAccessID,
		PrivateKey:     b.opts.PrivateKey,
		SignBytes:      b.opts.SignBytes,
	}
	if b.opts.MakeSignBytes != nil {
		opts.SignBytes = b.opts.MakeSignBytes(ctx)
	}
	if dopts.BeforeSign != nil {
		asFunc := func(i interface{}) bool {
			v, ok := i.(**storage.SignedURLOptions)
			if ok {
				*v = opts
			}
			return ok
		}
		if err := dopts.BeforeSign(asFunc); err != nil {
			return "", err
		}
	}
	return storage.SignedURL(b.name, key, opts)
}

func bufferSize(size int) int {
	if size == 0 {
		return googleapi.DefaultUploadChunkSize
	} else if size > 0 {
		return size
	}
	return 0
}
