package gcsblob

import (
	"context"
	"sync"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	gax "github.com/googleapis/gax-go/v2"
	credentialspb "google.golang.org/genproto/googleapis/iam/credentials/v1"
)

type credentialsClient struct {
	init sync.Once
	err  error

	client interface {
		SignBlob(context.Context, *credentialspb.SignBlobRequest, ...gax.CallOption) (*credentialspb.SignBlobResponse, error)
	}
}

func (c *credentialsClient) CreateMakeSignBytesWith(lifetimeCtx context.Context, googleAccessID string) func(context.Context) SignBytesFunc {
	return func(requestCtx context.Context) SignBytesFunc {
		c.init.Do(func() {
			if c.client != nil {

				return
			}
			c.client, c.err = credentials.NewIamCredentialsClient(lifetimeCtx)
		})

		return func(p []byte) ([]byte, error) {
			if c.err != nil {
				return nil, c.err
			}

			resp, err := c.client.SignBlob(
				requestCtx,
				&credentialspb.SignBlobRequest{
					Name:    googleAccessID,
					Payload: p,
				})
			if err != nil {
				return nil, err
			}
			return resp.GetSignedBlob(), nil
		}
	}
}
