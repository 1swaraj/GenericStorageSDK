package memdocstore

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/swaraj1802/GenericStorageSDK/docstore"
)

func init() {
	docstore.DefaultURLMux().RegisterCollection(Scheme, &URLOpener{})
}

const Scheme = "mem"

type URLOpener struct {
	mu          sync.Mutex
	collections map[string]urlColl
}

type urlColl struct {
	keyName string
	coll    *docstore.Collection
}

func (o *URLOpener) OpenCollectionURL(ctx context.Context, u *url.URL) (*docstore.Collection, error) {
	q := u.Query()
	collName := u.Host
	if collName == "" {
		return nil, fmt.Errorf("open collection %v: empty collection name", u)
	}
	keyName := u.Path
	if strings.HasPrefix(keyName, "/") {
		keyName = keyName[1:]
	}
	if keyName == "" || strings.ContainsRune(keyName, '/') {
		return nil, fmt.Errorf("open collection %v: invalid key name %q (must be non-empty and have no slashes)", u, keyName)
	}

	options := &Options{
		RevisionField: q.Get("revision_field"),
		Filename:      q.Get("filename"),
		onClose: func() {
			o.mu.Lock()
			delete(o.collections, collName)
			o.mu.Unlock()
		},
	}
	q.Del("revision_field")
	q.Del("filename")
	for param := range q {
		return nil, fmt.Errorf("open collection %v: invalid query parameter %q", u, param)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.collections == nil {
		o.collections = map[string]urlColl{}
	}
	ucoll, ok := o.collections[collName]
	if !ok {
		coll, err := OpenCollection(keyName, options)
		if err != nil {
			return nil, err
		}
		o.collections[collName] = urlColl{keyName, coll}
		return coll, nil
	}
	if ucoll.keyName != keyName {
		return nil, fmt.Errorf("open collection %v: key name %q does not equal existing key name %q",
			u, keyName, ucoll.keyName)
	}
	return ucoll.coll, nil
}
