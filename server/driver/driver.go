package driver

import (
	"context"
	"net/http"
)

type Server interface {
	ListenAndServe(addr string, h http.Handler) error

	Shutdown(ctx context.Context) error
}

type TLSServer interface {
	ListenAndServeTLS(addr, certFile, keyFile string, h http.Handler) error
}
