package driver

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
)

type ReaderOptions struct {
	BeforeRead func(asFunc func(interface{}) bool) error
}

type Reader interface {
	io.ReadCloser

	Attributes() *ReaderAttributes

	As(interface{}) bool
}

type Writer interface {
	io.WriteCloser
}

type WriterOptions struct {
	BufferSize int

	CacheControl string

	ContentDisposition string

	ContentEncoding string

	ContentLanguage string

	ContentMD5 []byte

	Metadata map[string]string

	BeforeWrite func(asFunc func(interface{}) bool) error
}

type CopyOptions struct {
	BeforeCopy func(asFunc func(interface{}) bool) error
}

type ReaderAttributes struct {
	ContentType string

	ModTime time.Time

	Size int64
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

	AsFunc func(interface{}) bool
}

type ListOptions struct {
	Prefix string

	Delimiter string

	PageSize int

	PageToken []byte

	BeforeList func(asFunc func(interface{}) bool) error
}

type ListObject struct {
	Key string

	ModTime time.Time

	Size int64

	MD5 []byte

	IsDir bool

	AsFunc func(interface{}) bool
}

type ListPage struct {
	Objects []*ListObject

	NextPageToken []byte
}

type Bucket interface {
	ErrorCode(error) gcerrors.ErrorCode

	As(i interface{}) bool

	ErrorAs(error, interface{}) bool

	Attributes(ctx context.Context, key string) (*Attributes, error)

	ListPaged(ctx context.Context, opts *ListOptions) (*ListPage, error)

	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (Reader, error)

	NewTypedWriter(ctx context.Context, key, contentType string, opts *WriterOptions) (Writer, error)

	Copy(ctx context.Context, dstKey, srcKey string, opts *CopyOptions) error

	Delete(ctx context.Context, key string) error

	SignedURL(ctx context.Context, key string, opts *SignedURLOptions) (string, error)

	Close() error
}

type SignedURLOptions struct {
	Expiry time.Duration

	Method string

	ContentType string

	EnforceAbsentContentType bool

	BeforeSign func(asFunc func(interface{}) bool) error
}

type prefixedBucket struct {
	base   Bucket
	prefix string
}

func NewPrefixedBucket(b Bucket, prefix string) Bucket {
	return &prefixedBucket{base: b, prefix: prefix}
}

func (b *prefixedBucket) ErrorCode(err error) gcerrors.ErrorCode { return b.base.ErrorCode(err) }
func (b *prefixedBucket) As(i interface{}) bool                  { return b.base.As(i) }
func (b *prefixedBucket) ErrorAs(err error, i interface{}) bool  { return b.base.ErrorAs(err, i) }
func (b *prefixedBucket) Attributes(ctx context.Context, key string) (*Attributes, error) {
	return b.base.Attributes(ctx, b.prefix+key)
}
func (b *prefixedBucket) ListPaged(ctx context.Context, opts *ListOptions) (*ListPage, error) {
	var myopts ListOptions
	if opts != nil {
		myopts = *opts
	}
	myopts.Prefix = b.prefix + myopts.Prefix
	page, err := b.base.ListPaged(ctx, &myopts)
	if err != nil {
		return nil, err
	}
	for _, p := range page.Objects {
		p.Key = strings.TrimPrefix(p.Key, b.prefix)
	}
	return page, nil
}
func (b *prefixedBucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *ReaderOptions) (Reader, error) {
	return b.base.NewRangeReader(ctx, b.prefix+key, offset, length, opts)
}
func (b *prefixedBucket) NewTypedWriter(ctx context.Context, key, contentType string, opts *WriterOptions) (Writer, error) {
	if key == "" {
		return nil, errors.New("invalid key (empty string)")
	}
	return b.base.NewTypedWriter(ctx, b.prefix+key, contentType, opts)
}
func (b *prefixedBucket) Copy(ctx context.Context, dstKey, srcKey string, opts *CopyOptions) error {
	return b.base.Copy(ctx, b.prefix+dstKey, b.prefix+srcKey, opts)
}
func (b *prefixedBucket) Delete(ctx context.Context, key string) error {
	return b.base.Delete(ctx, b.prefix+key)
}
func (b *prefixedBucket) SignedURL(ctx context.Context, key string, opts *SignedURLOptions) (string, error) {
	return b.base.SignedURL(ctx, b.prefix+key, opts)
}
func (b *prefixedBucket) Close() error { return b.base.Close() }
