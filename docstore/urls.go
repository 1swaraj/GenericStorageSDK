package docstore

import (
	"context"
	"net/url"

	"github.com/swaraj1802/GenericStorageSDK/internal/openurl"
)

type CollectionURLOpener interface {
	OpenCollectionURL(ctx context.Context, u *url.URL) (*Collection, error)
}

type URLMux struct {
	schemes openurl.SchemeMap
}

func (mux *URLMux) CollectionSchemes() []string { return mux.schemes.Schemes() }

func (mux *URLMux) ValidCollectionScheme(scheme string) bool { return mux.schemes.ValidScheme(scheme) }

func (mux *URLMux) RegisterCollection(scheme string, opener CollectionURLOpener) {
	mux.schemes.Register("docstore", "Collection", scheme, opener)
}

func (mux *URLMux) OpenCollection(ctx context.Context, urlstr string) (*Collection, error) {
	opener, u, err := mux.schemes.FromString("Collection", urlstr)
	if err != nil {
		return nil, err
	}
	return opener.(CollectionURLOpener).OpenCollectionURL(ctx, u)
}

func (mux *URLMux) OpenCollectionURL(ctx context.Context, u *url.URL) (*Collection, error) {
	opener, err := mux.schemes.FromURL("Collection", u)
	if err != nil {
		return nil, err
	}
	return opener.(CollectionURLOpener).OpenCollectionURL(ctx, u)
}

var defaultURLMux = new(URLMux)

func DefaultURLMux() *URLMux {
	return defaultURLMux
}

func OpenCollection(ctx context.Context, urlstr string) (*Collection, error) {
	return defaultURLMux.OpenCollection(ctx, urlstr)
}
