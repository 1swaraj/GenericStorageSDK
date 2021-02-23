package useragent

import (
	"fmt"
	"net/http"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	prefix  = "go-cloud"
	version = "0.1.0"
)

func ClientOption(api string) option.ClientOption {
	return option.WithUserAgent(userAgentString(api))
}

func GRPCDialOption(api string) grpc.DialOption {
	return grpc.WithUserAgent(userAgentString(api))
}

func AzureUserAgentPrefix(api string) string {
	return userAgentString(api)
}

func userAgentString(api string) string {
	return fmt.Sprintf("%s/%s/%s", prefix, api, version)
}

type userAgentTransport struct {
	base http.RoundTripper
	api  string
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {

	newReq := *req
	newReq.Header = make(http.Header)
	for k, vv := range req.Header {
		newReq.Header[k] = vv
	}

	newReq.Header.Set("User-Agent", req.UserAgent()+" "+userAgentString(t.api))
	return t.base.RoundTrip(&newReq)
}

func HTTPClient(client *http.Client, api string) *http.Client {
	c := *client
	c.Transport = &userAgentTransport{base: c.Transport, api: api}
	return &c
}
