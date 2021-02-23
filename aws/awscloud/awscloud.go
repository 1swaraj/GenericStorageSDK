package awscloud

import (
	"github.com/google/wire"
	"github.com/swaraj1802/GenericStorageSDK/aws"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage/s3blob"
	"github.com/swaraj1802/GenericStorageSDK/server/xrayserver"
	"net/http"
)

var AWS = wire.NewSet(
	Services,
	aws.DefaultSession,
	wire.Value(http.DefaultClient),
)

var Services = wire.NewSet(
	s3blob.Set,
	xrayserver.Set,
)
