














package gcpcloud

import (
	"github.com/google/wire"
	"github.com/swaraj1802/GenericStorageSDK/genericstorage/gcsblob"
	"github.com/swaraj1802/GenericStorageSDK/gcp"
	"github.com/swaraj1802/GenericStorageSDK/server/sdserver"
)



var GCP = wire.NewSet(Services, gcp.DefaultIdentity)




var Services = wire.NewSet(
	gcp.DefaultTransport,
	gcp.NewHTTPClient,
	gcsblob.Set,
	sdserver.Set,
)
