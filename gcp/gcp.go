package gcp

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/wire"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var DefaultIdentity = wire.NewSet(
	CredentialsTokenSource,
	DefaultCredentials,
	DefaultProjectID)

type ProjectID string

type TokenSource oauth2.TokenSource

type HTTPClient struct {
	http.Client
}

func NewAnonymousHTTPClient(transport http.RoundTripper) *HTTPClient {
	return &HTTPClient{
		Client: http.Client{
			Transport: transport,
		},
	}
}

func NewHTTPClient(transport http.RoundTripper, ts TokenSource) (*HTTPClient, error) {
	if ts == nil {
		return nil, errors.New("gcp: no credentials available")
	}
	return &HTTPClient{
		Client: http.Client{
			Transport: &oauth2.Transport{
				Base:   transport,
				Source: ts,
			},
		},
	}, nil
}

func DefaultTransport() http.RoundTripper {
	return http.DefaultTransport
}

func DefaultCredentials(ctx context.Context) (*google.Credentials, error) {
	adc, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, err
	}
	return adc, nil
}

func CredentialsTokenSource(creds *google.Credentials) TokenSource {
	if creds == nil {
		return nil
	}
	return TokenSource(creds.TokenSource)
}

func DefaultProjectID(creds *google.Credentials) (ProjectID, error) {
	if creds == nil {
		return "", errors.New("gcp: no project found in credentials")
	}
	return ProjectID(creds.ProjectID), nil
}
