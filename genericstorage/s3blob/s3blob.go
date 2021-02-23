package s3blob

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	gcaws "github.com/swaraj1802/GenericStorageSDK/aws"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/escape"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/wire"
)

const defaultPageSize = 1000

func init() {
	genericstorage.DefaultURLMux().RegisterBucket(Scheme, new(lazySessionOpener))
}

var Set = wire.NewSet(
	wire.Struct(new(URLOpener), "ConfigProvider"),
)

type lazySessionOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *lazySessionOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*genericstorage.Bucket, error) {
	o.init.Do(func() {
		sess, err := gcaws.NewDefaultSession()
		if err != nil {
			o.err = err
			return
		}
		o.opener = &URLOpener{
			ConfigProvider: sess,
		}
	})
	if o.err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, o.err)
	}
	return o.opener.OpenBucketURL(ctx, u)
}

const Scheme = "s3"

type URLOpener struct {
	ConfigProvider client.ConfigProvider

	Options Options
}

func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*genericstorage.Bucket, error) {
	configProvider := &gcaws.ConfigOverrider{
		Base: o.ConfigProvider,
	}
	overrideCfg, err := gcaws.ConfigFromURLParams(u.Query())
	if err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, err)
	}
	configProvider.Configs = append(configProvider.Configs, overrideCfg)
	return OpenBucket(ctx, configProvider, u.Host, &o.Options)
}

type Options struct {
	UseLegacyList bool
}

func openBucket(ctx context.Context, sess client.ConfigProvider, bucketName string, opts *Options) (*bucket, error) {
	if sess == nil {
		return nil, errors.New("s3blob.OpenBucket: sess is required")
	}
	if bucketName == "" {
		return nil, errors.New("s3blob.OpenBucket: bucketName is required")
	}
	if opts == nil {
		opts = &Options{}
	}
	return &bucket{
		name:          bucketName,
		client:        s3.New(sess),
		useLegacyList: opts.UseLegacyList,
	}, nil
}

func OpenBucket(ctx context.Context, sess client.ConfigProvider, bucketName string, opts *Options) (*genericstorage.Bucket, error) {
	drv, err := openBucket(ctx, sess, bucketName, opts)
	if err != nil {
		return nil, err
	}
	return genericstorage.NewBucket(drv), nil
}

type reader struct {
	body  io.ReadCloser
	attrs driver.ReaderAttributes
	raw   *s3.GetObjectOutput
}

func (r *reader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *reader) Close() error {
	return r.body.Close()
}

func (r *reader) As(i interface{}) bool {
	p, ok := i.(*s3.GetObjectOutput)
	if !ok {
		return false
	}
	*p = *r.raw
	return true
}

func (r *reader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

type writer struct {
	w *io.PipeWriter

	ctx      context.Context
	uploader *s3manager.Uploader
	req      *s3manager.UploadInput
	donec    chan struct{}

	err error
}

func (w *writer) Write(p []byte) (int, error) {

	if len(p) == 0 {
		return 0, nil
	}
	if w.w == nil {

		pr, pw := io.Pipe()
		w.w = pw
		if err := w.open(pr); err != nil {
			return 0, err
		}
	}
	select {
	case <-w.donec:
		return 0, w.err
	default:
	}
	return w.w.Write(p)
}

func (w *writer) open(pr *io.PipeReader) error {

	go func() {
		defer close(w.donec)

		if pr == nil {

			w.req.Body = http.NoBody
		} else {
			w.req.Body = pr
		}
		_, err := w.uploader.UploadWithContext(w.ctx, w.req)
		if err != nil {
			w.err = err
			if pr != nil {
				pr.CloseWithError(err)
			}
			return
		}
	}()
	return nil
}

func (w *writer) Close() error {
	if w.w == nil {

		w.open(nil)
	} else if err := w.w.Close(); err != nil {
		return err
	}
	<-w.donec
	return w.err
}

type bucket struct {
	name          string
	client        *s3.S3
	useLegacyList bool
}

func (b *bucket) Close() error {
	return nil
}

func (b *bucket) ErrorCode(err error) gcerrors.ErrorCode {
	e, ok := err.(awserr.Error)
	if !ok {
		return gcerrors.Unknown
	}
	switch {
	case e.Code() == "NoSuchBucket" || e.Code() == "NoSuchKey" || e.Code() == "NotFound" || e.Code() == s3.ErrCodeObjectNotInActiveTierError:
		return gcerrors.NotFound
	default:
		return gcerrors.Unknown
	}
}

func (b *bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	in := &s3.ListObjectsV2Input{
		Bucket:  aws.String(b.name),
		MaxKeys: aws.Int64(int64(pageSize)),
	}
	if len(opts.PageToken) > 0 {
		in.ContinuationToken = aws.String(string(opts.PageToken))
	}
	if opts.Prefix != "" {
		in.Prefix = aws.String(escapeKey(opts.Prefix))
	}
	if opts.Delimiter != "" {
		in.Delimiter = aws.String(escapeKey(opts.Delimiter))
	}
	resp, err := b.listObjects(ctx, in, opts)
	if err != nil {
		return nil, err
	}
	page := driver.ListPage{}
	if resp.NextContinuationToken != nil {
		page.NextPageToken = []byte(*resp.NextContinuationToken)
	}
	if n := len(resp.Contents) + len(resp.CommonPrefixes); n > 0 {
		page.Objects = make([]*driver.ListObject, n)
		for i, obj := range resp.Contents {
			obj := obj
			page.Objects[i] = &driver.ListObject{
				Key:     unescapeKey(aws.StringValue(obj.Key)),
				ModTime: *obj.LastModified,
				Size:    *obj.Size,
				MD5:     eTagToMD5(obj.ETag),
				AsFunc: func(i interface{}) bool {
					p, ok := i.(*s3.Object)
					if !ok {
						return false
					}
					*p = *obj
					return true
				},
			}
		}
		for i, prefix := range resp.CommonPrefixes {
			prefix := prefix
			page.Objects[i+len(resp.Contents)] = &driver.ListObject{
				Key:   unescapeKey(aws.StringValue(prefix.Prefix)),
				IsDir: true,
				AsFunc: func(i interface{}) bool {
					p, ok := i.(*s3.CommonPrefix)
					if !ok {
						return false
					}
					*p = *prefix
					return true
				},
			}
		}
		if len(resp.Contents) > 0 && len(resp.CommonPrefixes) > 0 {

			sort.Slice(page.Objects, func(i, j int) bool {
				return page.Objects[i].Key < page.Objects[j].Key
			})
		}
	}
	return &page, nil
}

func (b *bucket) listObjects(ctx context.Context, in *s3.ListObjectsV2Input, opts *driver.ListOptions) (*s3.ListObjectsV2Output, error) {
	if !b.useLegacyList {
		if opts.BeforeList != nil {
			asFunc := func(i interface{}) bool {
				p, ok := i.(**s3.ListObjectsV2Input)
				if !ok {
					return false
				}
				*p = in
				return true
			}
			if err := opts.BeforeList(asFunc); err != nil {
				return nil, err
			}
		}
		return b.client.ListObjectsV2WithContext(ctx, in)
	}

	legacyIn := &s3.ListObjectsInput{
		Bucket:       in.Bucket,
		Delimiter:    in.Delimiter,
		EncodingType: in.EncodingType,
		Marker:       in.ContinuationToken,
		MaxKeys:      in.MaxKeys,
		Prefix:       in.Prefix,
		RequestPayer: in.RequestPayer,
	}
	if opts.BeforeList != nil {
		asFunc := func(i interface{}) bool {
			p, ok := i.(**s3.ListObjectsInput)
			if !ok {
				return false
			}
			*p = legacyIn
			return true
		}
		if err := opts.BeforeList(asFunc); err != nil {
			return nil, err
		}
	}
	legacyResp, err := b.client.ListObjectsWithContext(ctx, legacyIn)
	if err != nil {
		return nil, err
	}

	var nextContinuationToken *string
	if legacyResp.NextMarker != nil {
		nextContinuationToken = legacyResp.NextMarker
	} else if aws.BoolValue(legacyResp.IsTruncated) {
		nextContinuationToken = aws.String(aws.StringValue(legacyResp.Contents[len(legacyResp.Contents)-1].Key))
	}
	return &s3.ListObjectsV2Output{
		CommonPrefixes:        legacyResp.CommonPrefixes,
		Contents:              legacyResp.Contents,
		NextContinuationToken: nextContinuationToken,
	}, nil
}

func (b *bucket) As(i interface{}) bool {
	p, ok := i.(**s3.S3)
	if !ok {
		return false
	}
	*p = b.client
	return true
}

func (b *bucket) ErrorAs(err error, i interface{}) bool {
	switch v := err.(type) {
	case awserr.Error:
		if p, ok := i.(*awserr.Error); ok {
			*p = v
			return true
		}
	}
	return false
}

func (b *bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	key = escapeKey(key)
	in := &s3.HeadObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	}
	resp, err := b.client.HeadObjectWithContext(ctx, in)
	if err != nil {
		return nil, err
	}

	md := make(map[string]string, len(resp.Metadata))
	for k, v := range resp.Metadata {

		md[escape.HexUnescape(escape.URLUnescape(k))] = escape.URLUnescape(aws.StringValue(v))
	}
	return &driver.Attributes{
		CacheControl:       aws.StringValue(resp.CacheControl),
		ContentDisposition: aws.StringValue(resp.ContentDisposition),
		ContentEncoding:    aws.StringValue(resp.ContentEncoding),
		ContentLanguage:    aws.StringValue(resp.ContentLanguage),
		ContentType:        aws.StringValue(resp.ContentType),
		Metadata:           md,

		ModTime: aws.TimeValue(resp.LastModified),
		Size:    aws.Int64Value(resp.ContentLength),
		MD5:     eTagToMD5(resp.ETag),
		ETag:    aws.StringValue(resp.ETag),
		AsFunc: func(i interface{}) bool {
			p, ok := i.(*s3.HeadObjectOutput)
			if !ok {
				return false
			}
			*p = *resp
			return true
		},
	}, nil
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	key = escapeKey(key)
	in := &s3.GetObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	}
	if offset > 0 && length < 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", offset))
	} else if length == 0 {

		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset))
	} else if length >= 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}
	if opts.BeforeRead != nil {
		asFunc := func(i interface{}) bool {
			if p, ok := i.(**s3.GetObjectInput); ok {
				*p = in
				return true
			}
			return false
		}
		if err := opts.BeforeRead(asFunc); err != nil {
			return nil, err
		}
	}
	resp, err := b.client.GetObjectWithContext(ctx, in)
	if err != nil {
		return nil, err
	}
	body := resp.Body
	if length == 0 {
		body = http.NoBody
	}
	return &reader{
		body: body,
		attrs: driver.ReaderAttributes{
			ContentType: aws.StringValue(resp.ContentType),
			ModTime:     aws.TimeValue(resp.LastModified),
			Size:        getSize(resp),
		},
		raw: resp,
	}, nil
}

func eTagToMD5(etag *string) []byte {
	if etag == nil {

		return nil
	}

	quoted := *etag
	if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
		return nil
	}
	unquoted := quoted[1 : len(quoted)-1]

	md5, err := hex.DecodeString(unquoted)
	if err != nil {
		return nil
	}
	return md5
}

func getSize(resp *s3.GetObjectOutput) int64 {

	size := aws.Int64Value(resp.ContentLength)
	if cr := aws.StringValue(resp.ContentRange); cr != "" {

		parts := strings.Split(cr, "/")
		if len(parts) == 2 {
			if i, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				size = i
			}
		}
	}
	return size
}

func escapeKey(key string) string {
	return escape.HexEscape(key, func(r []rune, i int) bool {
		c := r[i]
		switch {

		case c < 32:
			return true

		case i > 1 && c == '/' && r[i-1] == '.' && r[i-2] == '.':
			return true

		case i > 0 && c == '/' && r[i-1] == '/':
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
	uploader := s3manager.NewUploaderWithClient(b.client, func(u *s3manager.Uploader) {
		if opts.BufferSize != 0 {
			u.PartSize = int64(opts.BufferSize)
		}
	})
	md := make(map[string]*string, len(opts.Metadata))
	for k, v := range opts.Metadata {

		k = escape.HexEscape(url.PathEscape(k), func(runes []rune, i int) bool {
			c := runes[i]
			return c == '@' || c == ':' || c == '='
		})
		md[k] = aws.String(url.PathEscape(v))
	}
	req := &s3manager.UploadInput{
		Bucket:      aws.String(b.name),
		ContentType: aws.String(contentType),
		Key:         aws.String(key),
		Metadata:    md,
	}
	if opts.CacheControl != "" {
		req.CacheControl = aws.String(opts.CacheControl)
	}
	if opts.ContentDisposition != "" {
		req.ContentDisposition = aws.String(opts.ContentDisposition)
	}
	if opts.ContentEncoding != "" {
		req.ContentEncoding = aws.String(opts.ContentEncoding)
	}
	if opts.ContentLanguage != "" {
		req.ContentLanguage = aws.String(opts.ContentLanguage)
	}
	if len(opts.ContentMD5) > 0 {
		req.ContentMD5 = aws.String(base64.StdEncoding.EncodeToString(opts.ContentMD5))
	}
	if opts.BeforeWrite != nil {
		asFunc := func(i interface{}) bool {
			pu, ok := i.(**s3manager.Uploader)
			if ok {
				*pu = uploader
				return true
			}
			pui, ok := i.(**s3manager.UploadInput)
			if ok {
				*pui = req
				return true
			}
			return false
		}
		if err := opts.BeforeWrite(asFunc); err != nil {
			return nil, err
		}
	}
	return &writer{
		ctx:      ctx,
		uploader: uploader,
		req:      req,
		donec:    make(chan struct{}),
	}, nil
}

func (b *bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	dstKey = escapeKey(dstKey)
	srcKey = escapeKey(srcKey)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(b.name),
		CopySource: aws.String(b.name + "/" + srcKey),
		Key:        aws.String(dstKey),
	}
	if opts.BeforeCopy != nil {
		asFunc := func(i interface{}) bool {
			switch v := i.(type) {
			case **s3.CopyObjectInput:
				*v = input
				return true
			}
			return false
		}
		if err := opts.BeforeCopy(asFunc); err != nil {
			return err
		}
	}
	_, err := b.client.CopyObjectWithContext(ctx, input)
	return err
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	if _, err := b.Attributes(ctx, key); err != nil {
		return err
	}
	key = escapeKey(key)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	}
	_, err := b.client.DeleteObjectWithContext(ctx, input)
	return err
}

func (b *bucket) SignedURL(_ context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	key = escapeKey(key)
	var req *request.Request
	switch opts.Method {
	case http.MethodGet:
		in := &s3.GetObjectInput{
			Bucket: aws.String(b.name),
			Key:    aws.String(key),
		}
		if opts.BeforeSign != nil {
			asFunc := func(i interface{}) bool {
				v, ok := i.(**s3.GetObjectInput)
				if ok {
					*v = in
				}
				return ok
			}
			if err := opts.BeforeSign(asFunc); err != nil {
				return "", err
			}
		}
		req, _ = b.client.GetObjectRequest(in)
	case http.MethodPut:
		in := &s3.PutObjectInput{
			Bucket:      aws.String(b.name),
			Key:         aws.String(key),
			ContentType: aws.String(opts.ContentType),
		}
		if opts.BeforeSign != nil {
			asFunc := func(i interface{}) bool {
				v, ok := i.(**s3.PutObjectInput)
				if ok {
					*v = in
				}
				return ok
			}
			if err := opts.BeforeSign(asFunc); err != nil {
				return "", err
			}
		}
		req, _ = b.client.PutObjectRequest(in)
	case http.MethodDelete:
		in := &s3.DeleteObjectInput{
			Bucket: aws.String(b.name),
			Key:    aws.String(key),
		}
		if opts.BeforeSign != nil {
			asFunc := func(i interface{}) bool {
				v, ok := i.(**s3.DeleteObjectInput)
				if ok {
					*v = in
				}
				return ok
			}
			if err := opts.BeforeSign(asFunc); err != nil {
				return "", err
			}
		}
		req, _ = b.client.DeleteObjectRequest(in)
	default:
		return "", fmt.Errorf("unsupported Method %q", opts.Method)
	}
	return req.Presign(opts.Expiry)
}
