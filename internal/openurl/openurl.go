package openurl

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

type SchemeMap struct {
	api string
	m   map[string]interface{}
}

func (m *SchemeMap) Register(api, typ, scheme string, value interface{}) {
	if m.m == nil {
		m.m = map[string]interface{}{}
	}
	if api != strings.ToLower(api) {
		panic(fmt.Errorf("api should be lowercase: %q", api))
	}
	if m.api == "" {
		m.api = api
	} else if m.api != api {
		panic(fmt.Errorf("previously registered using api %q (now %q)", m.api, api))
	}
	if _, exists := m.m[scheme]; exists {
		panic(fmt.Errorf("scheme %q already registered for %s.%s", scheme, api, typ))
	}
	m.m[scheme] = value
}

func (m *SchemeMap) FromString(typ, urlstr string) (interface{}, *url.URL, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s.%s: %v", m.api, typ, err)
	}
	val, err := m.FromURL(typ, u)
	if err != nil {
		return nil, nil, err
	}
	return val, u, nil
}

func (m *SchemeMap) FromURL(typ string, u *url.URL) (interface{}, error) {
	scheme := u.Scheme
	if scheme == "" {
		return nil, fmt.Errorf("open %s.%s: no scheme in URL %q", m.api, typ, u)
	}
	for _, prefix := range []string{
		fmt.Sprintf("%s+%s+", m.api, strings.ToLower(typ)),
		fmt.Sprintf("%s+", m.api),
	} {
		scheme = strings.TrimPrefix(scheme, prefix)
	}
	v, ok := m.m[scheme]
	if !ok {
		return nil, fmt.Errorf("open %s.%s: no driver registered for %q for URL %q; available schemes: %v", m.api, typ, scheme, u, strings.Join(m.Schemes(), ", "))
	}
	return v, nil
}

func (m *SchemeMap) Schemes() []string {
	var schemes []string
	for s := range m.m {
		schemes = append(schemes, s)
	}
	sort.Strings(schemes)
	return schemes
}

func (m *SchemeMap) ValidScheme(scheme string) bool {
	for s := range m.m {
		if scheme == s {
			return true
		}
	}
	return false
}
