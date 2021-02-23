package sdserver

import (
	"fmt"
	"os"

	"github.com/swaraj1802/GenericStorageSDK/gcp"
	"github.com/swaraj1802/GenericStorageSDK/internal/useragent"
	"github.com/swaraj1802/GenericStorageSDK/server"
	"github.com/swaraj1802/GenericStorageSDK/server/requestlog"
	"github.com/google/wire"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"contrib.go.opencensus.io/exporter/stackdriver/monitoredresource"
	"go.opencensus.io/trace"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

var Set = wire.NewSet(
	server.Set,
	NewExporter,
	monitoredresource.Autodetect,
	wire.Bind(new(trace.Exporter), new(*stackdriver.Exporter)),
	NewRequestLogger,
	wire.Bind(new(requestlog.Logger), new(*requestlog.StackdriverLogger)),
)

func NewExporter(id gcp.ProjectID, ts gcp.TokenSource, mr monitoredresource.Interface) (*stackdriver.Exporter, func(), error) {
	opts := []option.ClientOption{
		option.WithTokenSource(oauth2.TokenSource(ts)),
		useragent.ClientOption("server"),
	}
	exp, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID:               string(id),
		MonitoringClientOptions: opts,
		TraceClientOptions:      opts,
		MonitoredResource:       mr,
	})
	if err != nil {
		return nil, nil, err
	}

	return exp, func() { exp.Flush() }, err
}

func NewRequestLogger() *requestlog.StackdriverLogger {

	return requestlog.NewStackdriverLogger(os.Stdout, func(e error) { fmt.Println(e) })
}
