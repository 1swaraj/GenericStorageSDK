package genericstorage

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/swaraj1802/GenericStorageSDK/genericstorage/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
	"github.com/swaraj1802/GenericStorageSDK/internal/oc"
	"github.com/swaraj1802/GenericStorageSDK/internal/openurl"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

type Reader struct {
	b   driver.Bucket
	r   driver.Reader
	key string
	end func(error)

	statsTagMutators []tag.Mutator
	bytesRead        int
	closed           bool
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.bytesRead += n
	return n, wrapError(r.b, err, r.key)
}

func (r *Reader) Close() error {
	r.closed = true
	err := wrapError(r.b, r.r.Close(), r.key)
	r.end(err)

	stats.RecordWithTags(
		context.Background(),
		r.statsTagMutators,
		bytesReadMeasure.M(int64(r.bytesRead)))
	return err
}

func (r *Reader) ContentType() string {
	return r.r.Attributes().ContentType
}

func (r *Reader) ModTime() time.Time {
	return r.r.Attributes().ModTime
}

func (r *Reader) Size() int64 {
	return r.r.Attributes().Size
}

func (r *Reader) As(i interface{}) bool {
	return r.r.As(i)
}

func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	_, nw, err := readFromWriteTo(r, w)
	return nw, err
}

func readFromWriteTo(r io.Reader, w io.Writer) (int64, int64, error) {
	buf := make([]byte, 1024)
	var totalRead, totalWritten int64
	for {
		numRead, rerr := r.Read(buf)
		if numRead > 0 {
			totalRead += int64(numRead)
			numWritten, werr := w.Write(buf[0:numRead])
			totalWritten += int64(numWritten)
			if werr != nil {
				return totalRead, totalWritten, werr
			}
		}
		if rerr == io.EOF {

			return totalRead, totalWritten, nil
		}
		if rerr != nil {
			return totalRead, totalWritten, rerr
		}
	}
}

type Attributes struct {
	CacheControl string

	ContentDisposition string

	ContentEncoding string

	ContentLanguage string

	ContentType string

	Metadata map[string]string

	CreateTime time.Time

	ModTime time.Time

	Size int64

	MD5 []byte

	ETag string

	asFunc func(interface{}) bool
}

func (a *Attributes) As(i interface{}) bool {
	if a.asFunc == nil {
		return false
	}
	return a.asFunc(i)
}

type Writer struct {
	b                driver.Bucket
	w                driver.Writer
	key              string
	end              func(error)
	cancel           func()
	contentMD5       []byte
	md5hash          hash.Hash
	statsTagMutators []tag.Mutator
	bytesWritten     int
	closed           bool

	ctx  context.Context
	opts *driver.WriterOptions
	buf  *bytes.Buffer
}

const sniffLen = 512

func (w *Writer) Write(p []byte) (int, error) {
	if len(w.contentMD5) > 0 {
		if _, err := w.md5hash.Write(p); err != nil {
			return 0, err
		}
	}
	if w.w != nil {
		return w.write(p)
	}

	if w.buf.Len() == 0 && len(p) >= sniffLen {
		return w.open(p)
	}

	n, err := w.buf.Write(p)
	if err != nil {
		return 0, err
	}
	if w.buf.Len() >= sniffLen {

		_, err := w.open(w.buf.Bytes())
		return n, err
	}
	return n, nil
}

func (w *Writer) Close() (err error) {
	w.closed = true
	defer func() {
		w.end(err)

		stats.RecordWithTags(
			context.Background(),
			w.statsTagMutators,
			bytesWrittenMeasure.M(int64(w.bytesWritten)))
	}()
	if len(w.contentMD5) > 0 {

		md5sum := w.md5hash.Sum(nil)
		if !bytes.Equal(md5sum, w.contentMD5) {

			w.cancel()
			if w.w != nil {
				_ = w.w.Close()
			}
			return gcerr.Newf(gcerr.FailedPrecondition, nil, "genericstorage: the WriterOptions.ContentMD5 you specified (%X) did not match what was written (%X)", w.contentMD5, md5sum)
		}
	}

	defer w.cancel()
	if w.w != nil {
		return wrapError(w.b, w.w.Close(), w.key)
	}
	if _, err := w.open(w.buf.Bytes()); err != nil {
		return err
	}
	return wrapError(w.b, w.w.Close(), w.key)
}

func (w *Writer) open(p []byte) (int, error) {
	ct := http.DetectContentType(p)
	var err error
	if w.w, err = w.b.NewTypedWriter(w.ctx, w.key, ct, w.opts); err != nil {
		return 0, wrapError(w.b, err, w.key)
	}

	w.buf = nil
	w.ctx = nil
	w.opts = nil
	return w.write(p)
}

func (w *Writer) write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.bytesWritten += n
	return n, wrapError(w.b, err, w.key)
}

func (w *Writer) ReadFrom(r io.Reader) (int64, error) {
	nr, _, err := readFromWriteTo(r, w)
	return nr, err
}

type ListOptions struct {
	Prefix string

	Delimiter string

	BeforeList func(asFunc func(interface{}) bool) error
}

type ListIterator struct {
	b       *Bucket
	opts    *driver.ListOptions
	page    *driver.ListPage
	nextIdx int
}

func (i *ListIterator) Next(ctx context.Context) (*ListObject, error) {
	if i.page != nil {

		if i.nextIdx < len(i.page.Objects) {

			dobj := i.page.Objects[i.nextIdx]
			i.nextIdx++
			return &ListObject{
				Key:     dobj.Key,
				ModTime: dobj.ModTime,
				Size:    dobj.Size,
				MD5:     dobj.MD5,
				IsDir:   dobj.IsDir,
				asFunc:  dobj.AsFunc,
			}, nil
		}
		if len(i.page.NextPageToken) == 0 {

			return nil, io.EOF
		}

		i.opts.PageToken = i.page.NextPageToken
	}
	i.b.mu.RLock()
	defer i.b.mu.RUnlock()
	if i.b.closed {
		return nil, errClosed
	}

	p, err := i.b.b.ListPaged(ctx, i.opts)
	if err != nil {
		return nil, wrapError(i.b.b, err, "")
	}
	i.page = p
	i.nextIdx = 0
	return i.Next(ctx)
}

type ListObject struct {
	Key string

	ModTime time.Time

	Size int64

	MD5 []byte

	IsDir bool

	asFunc func(interface{}) bool
}

func (o *ListObject) As(i interface{}) bool {
	if o.asFunc == nil {
		return false
	}
	return o.asFunc(i)
}

type Bucket struct {
	b      driver.Bucket
	tracer *oc.Tracer

	mu     sync.RWMutex
	closed bool
}

const pkgName = "github.com/swaraj1802/GenericStorageSDK/genericstorage"

var (
	latencyMeasure      = oc.LatencyMeasure(pkgName)
	bytesReadMeasure    = stats.Int64(pkgName+"/bytes_read", "Total bytes read", stats.UnitBytes)
	bytesWrittenMeasure = stats.Int64(pkgName+"/bytes_written", "Total bytes written", stats.UnitBytes)

	OpenCensusViews = append(
		oc.Views(pkgName, latencyMeasure),
		&view.View{
			Name:        pkgName + "/bytes_read",
			Measure:     bytesReadMeasure,
			Description: "Sum of bytes read from the service.",
			TagKeys:     []tag.Key{oc.ProviderKey},
			Aggregation: view.Sum(),
		},
		&view.View{
			Name:        pkgName + "/bytes_written",
			Measure:     bytesWrittenMeasure,
			Description: "Sum of bytes written to the service.",
			TagKeys:     []tag.Key{oc.ProviderKey},
			Aggregation: view.Sum(),
		})
)

var NewBucket = newBucket

func newBucket(b driver.Bucket) *Bucket {
	return &Bucket{
		b: b,
		tracer: &oc.Tracer{
			Package:        pkgName,
			Provider:       oc.ProviderName(b),
			LatencyMeasure: latencyMeasure,
		},
	}
}

func (b *Bucket) As(i interface{}) bool {
	if i == nil {
		return false
	}
	return b.b.As(i)
}

func (b *Bucket) ErrorAs(err error, i interface{}) bool {
	return gcerr.ErrorAs(err, i, b.b.ErrorAs)
}

func (b *Bucket) ReadAll(ctx context.Context, key string) (_ []byte, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	r, err := b.NewReader(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func (b *Bucket) List(opts *ListOptions) *ListIterator {
	if opts == nil {
		opts = &ListOptions{}
	}
	dopts := &driver.ListOptions{
		Prefix:     opts.Prefix,
		Delimiter:  opts.Delimiter,
		BeforeList: opts.BeforeList,
	}
	return &ListIterator{b: b, opts: dopts}
}

var FirstPageToken = []byte("first page")

func (b *Bucket) ListPage(ctx context.Context, pageToken []byte, pageSize int, opts *ListOptions) (retval []*ListObject, nextPageToken []byte, err error) {
	if opts == nil {
		opts = &ListOptions{}
	}
	if pageSize <= 0 {
		return nil, nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: pageSize must be > 0")
	}

	if len(pageToken) == 0 {
		return nil, nil, io.EOF
	}

	if bytes.Equal(pageToken, FirstPageToken) {
		pageToken = nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, nil, errClosed
	}

	ctx = b.tracer.Start(ctx, "ListPage")
	defer func() { b.tracer.End(ctx, err) }()

	dopts := &driver.ListOptions{
		Prefix:     opts.Prefix,
		Delimiter:  opts.Delimiter,
		BeforeList: opts.BeforeList,
		PageToken:  pageToken,
		PageSize:   pageSize,
	}
	retval = make([]*ListObject, 0, pageSize)
	for len(retval) < pageSize {
		p, err := b.b.ListPaged(ctx, dopts)
		if err != nil {
			return nil, nil, wrapError(b.b, err, "")
		}
		for _, dobj := range p.Objects {
			retval = append(retval, &ListObject{
				Key:     dobj.Key,
				ModTime: dobj.ModTime,
				Size:    dobj.Size,
				MD5:     dobj.MD5,
				IsDir:   dobj.IsDir,
				asFunc:  dobj.AsFunc,
			})
		}

		dopts.PageSize = pageSize - len(retval)
		dopts.PageToken = p.NextPageToken
		if len(dopts.PageToken) == 0 {
			dopts.PageToken = nil
			break
		}
	}
	return retval, dopts.PageToken, nil
}

func (b *Bucket) IsAccessible(ctx context.Context) (bool, error) {
	_, _, err := b.ListPage(ctx, FirstPageToken, 1, nil)
	if err == nil {
		return true, nil
	}
	if gcerrors.Code(err) == gcerrors.NotFound {
		return false, nil
	}
	return false, err
}

func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	_, err := b.Attributes(ctx, key)
	if err == nil {
		return true, nil
	}
	if gcerrors.Code(err) == gcerrors.NotFound {
		return false, nil
	}
	return false, err
}

func (b *Bucket) Attributes(ctx context.Context, key string) (_ *Attributes, err error) {
	if !utf8.ValidString(key) {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: Attributes key must be a valid UTF-8 string: %q", key)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	ctx = b.tracer.Start(ctx, "Attributes")
	defer func() { b.tracer.End(ctx, err) }()

	a, err := b.b.Attributes(ctx, key)
	if err != nil {
		return nil, wrapError(b.b, err, key)
	}
	var md map[string]string
	if len(a.Metadata) > 0 {

		md = make(map[string]string, len(a.Metadata))
		for k, v := range a.Metadata {
			md[strings.ToLower(k)] = v
		}
	}
	return &Attributes{
		CacheControl:       a.CacheControl,
		ContentDisposition: a.ContentDisposition,
		ContentEncoding:    a.ContentEncoding,
		ContentLanguage:    a.ContentLanguage,
		ContentType:        a.ContentType,
		Metadata:           md,
		CreateTime:         a.CreateTime,
		ModTime:            a.ModTime,
		Size:               a.Size,
		MD5:                a.MD5,
		ETag:               a.ETag,
		asFunc:             a.AsFunc,
	}, nil
}

func (b *Bucket) NewReader(ctx context.Context, key string, opts *ReaderOptions) (*Reader, error) {
	return b.newRangeReader(ctx, key, 0, -1, opts)
}

func (b *Bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (_ *Reader, err error) {
	return b.newRangeReader(ctx, key, offset, length, opts)
}

func (b *Bucket) newRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (_ *Reader, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	if offset < 0 {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: NewRangeReader offset must be non-negative (%d)", offset)
	}
	if !utf8.ValidString(key) {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: NewRangeReader key must be a valid UTF-8 string: %q", key)
	}
	if opts == nil {
		opts = &ReaderOptions{}
	}
	dopts := &driver.ReaderOptions{
		BeforeRead: opts.BeforeRead,
	}
	tctx := b.tracer.Start(ctx, "NewRangeReader")
	defer func() {

		if err != nil {
			b.tracer.End(tctx, err)
		}
	}()
	dr, err := b.b.NewRangeReader(ctx, key, offset, length, dopts)
	if err != nil {
		return nil, wrapError(b.b, err, key)
	}
	end := func(err error) { b.tracer.End(tctx, err) }
	r := &Reader{
		b:                b.b,
		r:                dr,
		key:              key,
		end:              end,
		statsTagMutators: []tag.Mutator{tag.Upsert(oc.ProviderKey, b.tracer.Provider)},
	}
	_, file, lineno, ok := runtime.Caller(2)
	runtime.SetFinalizer(r, func(r *Reader) {
		if !r.closed {
			var caller string
			if ok {
				caller = fmt.Sprintf(" (%s:%d)", file, lineno)
			}
			log.Printf("A genericstorage.Reader reading from %q was never closed%s", key, caller)
		}
	})
	return r, nil
}

func (b *Bucket) WriteAll(ctx context.Context, key string, p []byte, opts *WriterOptions) (err error) {
	realOpts := new(WriterOptions)
	if opts != nil {
		*realOpts = *opts
	}
	if len(realOpts.ContentMD5) == 0 {
		sum := md5.Sum(p)
		realOpts.ContentMD5 = sum[:]
	}
	w, err := b.NewWriter(ctx, key, realOpts)
	if err != nil {
		return err
	}
	if _, err := w.Write(p); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (b *Bucket) NewWriter(ctx context.Context, key string, opts *WriterOptions) (_ *Writer, err error) {
	if !utf8.ValidString(key) {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: NewWriter key must be a valid UTF-8 string: %q", key)
	}
	if opts == nil {
		opts = &WriterOptions{}
	}
	dopts := &driver.WriterOptions{
		CacheControl:       opts.CacheControl,
		ContentDisposition: opts.ContentDisposition,
		ContentEncoding:    opts.ContentEncoding,
		ContentLanguage:    opts.ContentLanguage,
		ContentMD5:         opts.ContentMD5,
		BufferSize:         opts.BufferSize,
		BeforeWrite:        opts.BeforeWrite,
	}
	if len(opts.Metadata) > 0 {

		md := make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			if k == "" {
				return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: WriterOptions.Metadata keys may not be empty strings")
			}
			if !utf8.ValidString(k) {
				return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: WriterOptions.Metadata keys must be valid UTF-8 strings: %q", k)
			}
			if !utf8.ValidString(v) {
				return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: WriterOptions.Metadata values must be valid UTF-8 strings: %q", v)
			}
			lowerK := strings.ToLower(k)
			if _, found := md[lowerK]; found {
				return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: WriterOptions.Metadata has a duplicate case-insensitive metadata key: %q", lowerK)
			}
			md[lowerK] = v
		}
		dopts.Metadata = md
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, errClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	tctx := b.tracer.Start(ctx, "NewWriter")
	end := func(err error) { b.tracer.End(tctx, err) }
	defer func() {
		if err != nil {
			end(err)
		}
	}()

	w := &Writer{
		b:                b.b,
		end:              end,
		cancel:           cancel,
		key:              key,
		contentMD5:       opts.ContentMD5,
		md5hash:          md5.New(),
		statsTagMutators: []tag.Mutator{tag.Upsert(oc.ProviderKey, b.tracer.Provider)},
	}
	if opts.ContentType != "" {
		t, p, err := mime.ParseMediaType(opts.ContentType)
		if err != nil {
			cancel()
			return nil, err
		}
		ct := mime.FormatMediaType(t, p)
		dw, err := b.b.NewTypedWriter(ctx, key, ct, dopts)
		if err != nil {
			cancel()
			return nil, wrapError(b.b, err, key)
		}
		w.w = dw
	} else {

		w.ctx = ctx
		w.opts = dopts
		w.buf = bytes.NewBuffer([]byte{})
	}
	_, file, lineno, ok := runtime.Caller(1)
	runtime.SetFinalizer(w, func(w *Writer) {
		if !w.closed {
			var caller string
			if ok {
				caller = fmt.Sprintf(" (%s:%d)", file, lineno)
			}
			log.Printf("A genericstorage.Writer writing to %q was never closed%s", key, caller)
		}
	})
	return w, nil
}

func (b *Bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *CopyOptions) (err error) {
	if !utf8.ValidString(srcKey) {
		return gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: Copy srcKey must be a valid UTF-8 string: %q", srcKey)
	}
	if !utf8.ValidString(dstKey) {
		return gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: Copy dstKey must be a valid UTF-8 string: %q", dstKey)
	}
	if opts == nil {
		opts = &CopyOptions{}
	}
	dopts := &driver.CopyOptions{
		BeforeCopy: opts.BeforeCopy,
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return errClosed
	}
	ctx = b.tracer.Start(ctx, "Copy")
	defer func() { b.tracer.End(ctx, err) }()
	return wrapError(b.b, b.b.Copy(ctx, dstKey, srcKey, dopts), fmt.Sprintf("%s -> %s", srcKey, dstKey))
}

func (b *Bucket) Delete(ctx context.Context, key string) (err error) {
	if !utf8.ValidString(key) {
		return gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: Delete key must be a valid UTF-8 string: %q", key)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return errClosed
	}
	ctx = b.tracer.Start(ctx, "Delete")
	defer func() { b.tracer.End(ctx, err) }()
	return wrapError(b.b, b.b.Delete(ctx, key), key)
}

func (b *Bucket) SignedURL(ctx context.Context, key string, opts *SignedURLOptions) (string, error) {
	if !utf8.ValidString(key) {
		return "", gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: SignedURL key must be a valid UTF-8 string: %q", key)
	}
	dopts := new(driver.SignedURLOptions)
	if opts == nil {
		opts = new(SignedURLOptions)
	}
	switch {
	case opts.Expiry < 0:
		return "", gcerr.Newf(gcerr.InvalidArgument, nil, "genericstorage: SignedURLOptions.Expiry must be >= 0 (%v)", opts.Expiry)
	case opts.Expiry == 0:
		dopts.Expiry = DefaultSignedURLExpiry
	default:
		dopts.Expiry = opts.Expiry
	}
	switch opts.Method {
	case "":
		dopts.Method = http.MethodGet
	case http.MethodGet, http.MethodPut, http.MethodDelete:
		dopts.Method = opts.Method
	default:
		return "", fmt.Errorf("genericstorage: unsupported SignedURLOptions.Method %q", opts.Method)
	}
	if opts.ContentType != "" && opts.Method != http.MethodPut {
		return "", fmt.Errorf("genericstorage: SignedURLOptions.ContentType must be empty for signing a %s URL", opts.Method)
	}
	if opts.EnforceAbsentContentType && opts.Method != http.MethodPut {
		return "", fmt.Errorf("genericstorage: SignedURLOptions.EnforceAbsentContentType must be false for signing a %s URL", opts.Method)
	}
	dopts.ContentType = opts.ContentType
	dopts.EnforceAbsentContentType = opts.EnforceAbsentContentType
	dopts.BeforeSign = opts.BeforeSign
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return "", errClosed
	}
	url, err := b.b.SignedURL(ctx, key, dopts)
	return url, wrapError(b.b, err, key)
}

func (b *Bucket) Close() error {
	b.mu.Lock()
	prev := b.closed
	b.closed = true
	b.mu.Unlock()
	if prev {
		return errClosed
	}
	return wrapError(b.b, b.b.Close(), "")
}

const DefaultSignedURLExpiry = 1 * time.Hour

type SignedURLOptions struct {
	Expiry time.Duration

	Method string

	ContentType string

	EnforceAbsentContentType bool

	BeforeSign func(asFunc func(interface{}) bool) error
}

type ReaderOptions struct {
	BeforeRead func(asFunc func(interface{}) bool) error
}

type WriterOptions struct {
	BufferSize int

	CacheControl string

	ContentDisposition string

	ContentEncoding string

	ContentLanguage string

	ContentType string

	ContentMD5 []byte

	Metadata map[string]string

	BeforeWrite func(asFunc func(interface{}) bool) error
}

type CopyOptions struct {
	BeforeCopy func(asFunc func(interface{}) bool) error
}

type BucketURLOpener interface {
	OpenBucketURL(ctx context.Context, u *url.URL) (*Bucket, error)
}

type URLMux struct {
	schemes openurl.SchemeMap
}

func (mux *URLMux) BucketSchemes() []string { return mux.schemes.Schemes() }

func (mux *URLMux) ValidBucketScheme(scheme string) bool { return mux.schemes.ValidScheme(scheme) }

func (mux *URLMux) RegisterBucket(scheme string, opener BucketURLOpener) {
	mux.schemes.Register("genericstorage", "Bucket", scheme, opener)
}

func (mux *URLMux) OpenBucket(ctx context.Context, urlstr string) (*Bucket, error) {
	opener, u, err := mux.schemes.FromString("Bucket", urlstr)
	if err != nil {
		return nil, err
	}
	return applyPrefixParam(ctx, opener.(BucketURLOpener), u)
}

func (mux *URLMux) OpenBucketURL(ctx context.Context, u *url.URL) (*Bucket, error) {
	opener, err := mux.schemes.FromURL("Bucket", u)
	if err != nil {
		return nil, err
	}
	return applyPrefixParam(ctx, opener.(BucketURLOpener), u)
}

func applyPrefixParam(ctx context.Context, opener BucketURLOpener, u *url.URL) (*Bucket, error) {
	prefix := u.Query().Get("prefix")
	if prefix != "" {

		urlCopy := *u
		q := urlCopy.Query()
		q.Del("prefix")
		urlCopy.RawQuery = q.Encode()
		u = &urlCopy
	}
	bucket, err := opener.OpenBucketURL(ctx, u)
	if err != nil {
		return nil, err
	}
	if prefix != "" {
		bucket = PrefixedBucket(bucket, prefix)
	}
	return bucket, nil
}

var defaultURLMux = new(URLMux)

func DefaultURLMux() *URLMux {
	return defaultURLMux
}

func OpenBucket(ctx context.Context, urlstr string) (*Bucket, error) {
	return defaultURLMux.OpenBucket(ctx, urlstr)
}

func wrapError(b driver.Bucket, err error, key string) error {
	if err == nil {
		return nil
	}
	if gcerr.DoNotWrap(err) {
		return err
	}
	msg := "genericstorage"
	if key != "" {
		msg += fmt.Sprintf(" (key %q)", key)
	}
	code := gcerrors.Code(err)
	if code == gcerrors.Unknown {
		code = b.ErrorCode(err)
	}
	return gcerr.New(code, err, 2, msg)
}

var errClosed = gcerr.Newf(gcerr.FailedPrecondition, nil, "genericstorage: Bucket has been closed")

func PrefixedBucket(bucket *Bucket, prefix string) *Bucket {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	bucket.closed = true
	return NewBucket(driver.NewPrefixedBucket(bucket.b, prefix))
}
