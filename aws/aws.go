package aws

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/wire"
)

var DefaultSession = wire.NewSet(
	SessionConfig,
	ConfigCredentials,
	NewDefaultSession,
	wire.Bind(new(client.ConfigProvider), new(*session.Session)),
)

func NewDefaultSession() (*session.Session, error) {
	return session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
}

func SessionConfig(sess *session.Session) *aws.Config {
	return sess.Config
}

func ConfigCredentials(cfg *aws.Config) *credentials.Credentials {
	return cfg.Credentials
}

type ConfigOverrider struct {
	Base    client.ConfigProvider
	Configs []*aws.Config
}

func (co ConfigOverrider) ClientConfig(serviceName string, cfgs ...*aws.Config) client.Config {
	cfgs = append(co.Configs[:len(co.Configs):len(co.Configs)], cfgs...)
	return co.Base.ClientConfig(serviceName, cfgs...)
}

func ConfigFromURLParams(q url.Values) (*aws.Config, error) {
	var cfg aws.Config
	for param, values := range q {
		value := values[0]
		switch param {
		case "region":
			cfg.Region = aws.String(value)
		case "endpoint":
			cfg.Endpoint = aws.String(value)
		case "disableSSL":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("invalid value for query parameter %q: %v", param, err)
			}
			cfg.DisableSSL = aws.Bool(b)
		case "s3ForcePathStyle":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("invalid value for query parameter %q: %v", param, err)
			}
			cfg.S3ForcePathStyle = aws.Bool(b)
		default:
			return nil, fmt.Errorf("unknown query parameter %q", param)
		}
	}
	return &cfg, nil
}
