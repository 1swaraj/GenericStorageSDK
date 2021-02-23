package xrayserver

import (
	"fmt"
	"os"

	exporter "contrib.go.opencensus.io/exporter/aws"
	"github.com/swaraj1802/GenericStorageSDK/server"
	"github.com/swaraj1802/GenericStorageSDK/server/requestlog"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/xray"
	"github.com/aws/aws-sdk-go/service/xray/xrayiface"
	"github.com/google/wire"
	"go.opencensus.io/trace"
)

var Set = wire.NewSet(
	server.Set,
	ServiceSet,
	NewExporter,
	wire.Bind(new(trace.Exporter), new(*exporter.Exporter)),
	NewRequestLogger,
	wire.Bind(new(requestlog.Logger), new(*requestlog.NCSALogger)),
)

var ServiceSet = wire.NewSet(
	NewXRayClient,
	wire.Bind(new(xrayiface.XRayAPI), new(*xray.XRay)),
)

func NewExporter(api xrayiface.XRayAPI) (*exporter.Exporter, func(), error) {
	e, err := exporter.NewExporter(exporter.WithAPI(api))
	if err != nil {
		return nil, nil, err
	}
	return e, func() { e.Close() }, nil
}

func NewXRayClient(p client.ConfigProvider) *xray.XRay {
	return xray.New(p)
}

func NewRequestLogger() *requestlog.NCSALogger {
	return requestlog.NewNCSALogger(os.Stdout, func(e error) { fmt.Fprintln(os.Stderr, e) })
}
