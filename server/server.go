package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/server/driver"
	"github.com/swaraj1802/GenericStorageSDK/server/health"
	"github.com/swaraj1802/GenericStorageSDK/server/requestlog"
	"github.com/google/wire"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
)

var Set = wire.NewSet(
	New,
	wire.Struct(new(Options), "RequestLogger", "HealthChecks", "TraceExporter", "DefaultSamplingPolicy", "Driver"),
	wire.Value(&DefaultDriver{}),
	wire.Bind(new(driver.Server), new(*DefaultDriver)),
)

type Server struct {
	reqlog         requestlog.Logger
	handler        http.Handler
	wrappedHandler http.Handler
	healthHandler  health.Handler
	te             trace.Exporter
	sampler        trace.Sampler
	once           sync.Once
	driver         driver.Server
}

type Options struct {
	RequestLogger requestlog.Logger

	HealthChecks []health.Checker

	TraceExporter trace.Exporter

	DefaultSamplingPolicy trace.Sampler

	Driver driver.Server
}

func New(h http.Handler, opts *Options) *Server {
	srv := &Server{handler: h}
	if opts != nil {
		srv.reqlog = opts.RequestLogger
		srv.te = opts.TraceExporter
		for _, c := range opts.HealthChecks {
			srv.healthHandler.Add(c)
		}
		srv.sampler = opts.DefaultSamplingPolicy
		srv.driver = opts.Driver
	}
	return srv
}

func (srv *Server) init() {
	srv.once.Do(func() {
		if srv.te != nil {
			trace.RegisterExporter(srv.te)
		}
		if srv.sampler != nil {
			trace.ApplyConfig(trace.Config{DefaultSampler: srv.sampler})
		}
		if srv.driver == nil {
			srv.driver = NewDefaultDriver()
		}
		if srv.handler == nil {
			srv.handler = http.DefaultServeMux
		}

		const healthPrefix = "/healthz/"

		mux := http.NewServeMux()
		mux.HandleFunc(healthPrefix+"liveness", health.HandleLive)
		mux.Handle(healthPrefix+"readiness", &srv.healthHandler)
		h := srv.handler
		if srv.reqlog != nil {
			h = requestlog.NewHandler(srv.reqlog, h)
		}
		h = &ochttp.Handler{
			Handler:          h,
			IsPublicEndpoint: true,
		}
		mux.Handle("/", h)
		srv.wrappedHandler = mux
	})
}

func (srv *Server) ListenAndServe(addr string) error {
	srv.init()
	return srv.driver.ListenAndServe(addr, srv.wrappedHandler)
}

func (srv *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {

	tlsDriver, ok := srv.driver.(driver.TLSServer)
	if !ok {
		return fmt.Errorf("driver %T does not support ListenAndServeTLS", srv.driver)
	}
	srv.init()
	return tlsDriver.ListenAndServeTLS(addr, certFile, keyFile, srv.wrappedHandler)
}

func (srv *Server) Shutdown(ctx context.Context) error {
	if srv.driver == nil {
		return nil
	}
	return srv.driver.Shutdown(ctx)
}

type DefaultDriver struct {
	Server http.Server
}

func NewDefaultDriver() *DefaultDriver {
	return &DefaultDriver{
		Server: http.Server{
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}
}

func (dd *DefaultDriver) ListenAndServe(addr string, h http.Handler) error {
	dd.Server.Addr = addr
	dd.Server.Handler = h
	return dd.Server.ListenAndServe()
}

func (dd *DefaultDriver) ListenAndServeTLS(addr, certFile, keyFile string, h http.Handler) error {
	dd.Server.Addr = addr
	dd.Server.Handler = h
	return dd.Server.ListenAndServeTLS(certFile, keyFile)
}

func (dd *DefaultDriver) Shutdown(ctx context.Context) error {
	return dd.Server.Shutdown(ctx)
}
