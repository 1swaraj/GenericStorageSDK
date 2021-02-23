package memdocstore

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/swaraj1802/GenericStorageSDK/docstore"
	"github.com/swaraj1802/GenericStorageSDK/docstore/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
)

type Options struct {
	RevisionField string

	MaxOutstandingActions int

	Filename string

	onClose func()
}

func OpenCollection(keyField string, opts *Options) (*docstore.Collection, error) {
	c, err := newCollection(keyField, nil, opts)
	if err != nil {
		return nil, err
	}
	return docstore.NewCollection(c), nil
}

func OpenCollectionWithKeyFunc(keyFunc func(docstore.Document) interface{}, opts *Options) (*docstore.Collection, error) {
	c, err := newCollection("", keyFunc, opts)
	if err != nil {
		return nil, err
	}
	return docstore.NewCollection(c), nil
}

func newCollection(keyField string, keyFunc func(docstore.Document) interface{}, opts *Options) (driver.Collection, error) {
	if keyField == "" && keyFunc == nil {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "must provide either keyField or keyFunc")
	}
	if opts == nil {
		opts = &Options{}
	}
	if opts.RevisionField == "" {
		opts.RevisionField = docstore.DefaultRevisionField
	}
	docs, err := loadDocs(opts.Filename)
	if err != nil {
		return nil, err
	}
	return &collection{
		keyField:    keyField,
		keyFunc:     keyFunc,
		docs:        docs,
		opts:        opts,
		curRevision: 0,
	}, nil
}

type storedDoc map[string]interface{}

type collection struct {
	keyField    string
	keyFunc     func(docstore.Document) interface{}
	opts        *Options
	mu          sync.Mutex
	docs        map[interface{}]storedDoc
	curRevision int64
}

func (c *collection) Key(doc driver.Document) (interface{}, error) {
	if c.keyField != "" {
		key, _ := doc.GetField(c.keyField)
		return key, nil
	}
	key := c.keyFunc(doc.Origin)
	if key == nil || driver.IsEmptyValue(reflect.ValueOf(key)) {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "missing document key")
	}
	return key, nil
}

func (c *collection) RevisionField() string {
	return c.opts.RevisionField
}

func (c *collection) ErrorCode(err error) gcerrors.ErrorCode {
	return gcerrors.Code(err)
}

func (c *collection) RunActions(ctx context.Context, actions []*driver.Action, opts *driver.RunActionsOptions) driver.ActionListError {
	errs := make([]error, len(actions))

	run := func(as []*driver.Action) {
		t := driver.NewThrottle(c.opts.MaxOutstandingActions)
		for _, a := range as {
			a := a
			t.Acquire()
			go func() {
				defer t.Release()
				errs[a.Index] = c.runAction(ctx, a)
			}()
		}
		t.Wait()
	}

	if opts.BeforeDo != nil {
		if err := opts.BeforeDo(func(interface{}) bool { return false }); err != nil {
			for i := range errs {
				errs[i] = err
			}
			return driver.NewActionListError(errs)
		}
	}

	beforeGets, gets, writes, afterGets := driver.GroupActions(actions)
	run(beforeGets)
	run(gets)
	run(writes)
	run(afterGets)
	return driver.NewActionListError(errs)
}

func (c *collection) runAction(ctx context.Context, a *driver.Action) error {

	if ctx.Err() != nil {
		return ctx.Err()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		current storedDoc
		exists  bool
	)
	if a.Key != nil {
		current, exists = c.docs[a.Key]
	}

	if !exists && (a.Kind == driver.Replace || a.Kind == driver.Update || a.Kind == driver.Get) {
		return gcerr.Newf(gcerr.NotFound, nil, "document with key %v does not exist", a.Key)
	}
	switch a.Kind {
	case driver.Create:

		if exists {
			return gcerr.Newf(gcerr.AlreadyExists, nil, "Create: document with key %v exists", a.Key)
		}

		if a.Key == nil {
			a.Key = driver.UniqueString()

			if err := a.Doc.SetField(c.keyField, a.Key); err != nil {
				return gcerr.Newf(gcerr.InvalidArgument, nil, "cannot set key field %q", c.keyField)
			}
		}
		fallthrough

	case driver.Replace, driver.Put:
		if err := c.checkRevision(a.Doc, current); err != nil {
			return err
		}
		doc, err := encodeDoc(a.Doc)
		if err != nil {
			return err
		}
		if a.Doc.HasField(c.opts.RevisionField) {
			c.changeRevision(doc)
			if err := a.Doc.SetField(c.opts.RevisionField, doc[c.opts.RevisionField]); err != nil {
				return err
			}
		}
		c.docs[a.Key] = doc

	case driver.Delete:
		if err := c.checkRevision(a.Doc, current); err != nil {
			return err
		}
		delete(c.docs, a.Key)

	case driver.Update:
		if err := c.checkRevision(a.Doc, current); err != nil {
			return err
		}
		if err := c.update(current, a.Mods); err != nil {
			return err
		}
		if a.Doc.HasField(c.opts.RevisionField) {
			c.changeRevision(current)
			if err := a.Doc.SetField(c.opts.RevisionField, current[c.opts.RevisionField]); err != nil {
				return err
			}
		}

	case driver.Get:

		if err := decodeDoc(current, a.Doc, a.FieldPaths); err != nil {
			return err
		}
	default:
		return gcerr.Newf(gcerr.Internal, nil, "unknown kind %v", a.Kind)
	}
	return nil
}

func (c *collection) update(doc storedDoc, mods []driver.Mod) error {

	sort.Slice(mods, func(i, j int) bool { return mods[i].FieldPath[0] < mods[j].FieldPath[0] })

	type guaranteedMod struct {
		parentMap    map[string]interface{}
		key          string
		encodedValue interface{}
	}

	gmods := make([]guaranteedMod, len(mods))
	var err error
	for i, mod := range mods {
		gmod := &gmods[i]

		if gmod.parentMap, err = getParentMap(doc, mod.FieldPath, false); err != nil {
			return err
		}
		gmod.key = mod.FieldPath[len(mod.FieldPath)-1]
		if inc, ok := mod.Value.(driver.IncOp); ok {
			amt, err := encodeValue(inc.Amount)
			if err != nil {
				return err
			}
			if gmod.encodedValue, err = add(gmod.parentMap[gmod.key], amt); err != nil {
				return err
			}
		} else if mod.Value != nil {

			if gmod.encodedValue, err = encodeValue(mod.Value); err != nil {
				return err
			}
		}
	}

	for _, m := range gmods {
		if m.encodedValue == nil {
			delete(m.parentMap, m.key)
		} else {
			m.parentMap[m.key] = m.encodedValue
		}
	}
	return nil
}

func add(x, y interface{}) (interface{}, error) {
	if x == nil {
		return y, nil
	}
	switch x := x.(type) {
	case int64:
		switch y := y.(type) {
		case int64:
			return x + y, nil
		case float64:
			return float64(x) + y, nil
		default:

			return nil, gcerr.Newf(gcerr.Internal, nil, "bad increment aount type %T", y)
		}
	case float64:
		switch y := y.(type) {
		case int64:
			return x + float64(y), nil
		case float64:
			return x + y, nil
		default:

			return nil, gcerr.Newf(gcerr.Internal, nil, "bad increment aount type %T", y)
		}
	default:
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "value %v being incremented not int64 or float64", x)
	}
}

func (c *collection) changeRevision(doc storedDoc) {
	c.curRevision++
	doc[c.opts.RevisionField] = c.curRevision
}

func (c *collection) checkRevision(arg driver.Document, current storedDoc) error {
	if current == nil {
		return nil
	}
	curRev, ok := current[c.opts.RevisionField]
	if !ok {
		return nil
	}
	curRev = curRev.(int64)
	r, err := arg.GetField(c.opts.RevisionField)
	if err != nil || r == nil {
		return nil
	}
	wantRev, ok := r.(int64)
	if !ok {
		return gcerr.Newf(gcerr.InvalidArgument, nil, "revision field %s is not an int64", c.opts.RevisionField)
	}
	if wantRev != curRev {
		return gcerr.Newf(gcerr.FailedPrecondition, nil, "mismatched revisions: want %d, current %d", wantRev, curRev)
	}
	return nil
}

func getAtFieldPath(m map[string]interface{}, fp []string) (interface{}, error) {
	m2, err := getParentMap(m, fp, false)
	if err != nil {
		return nil, err
	}
	v, ok := m2[fp[len(fp)-1]]
	if ok {
		return v, nil
	}
	return nil, gcerr.Newf(gcerr.NotFound, nil, "field %s not found", fp)
}

func setAtFieldPath(m map[string]interface{}, fp []string, val interface{}) error {
	m2, err := getParentMap(m, fp, true)
	if err != nil {
		return err
	}
	m2[fp[len(fp)-1]] = val
	return nil
}

func deleteAtFieldPath(m map[string]interface{}, fp []string) {
	m2, _ := getParentMap(m, fp, false)
	if m2 != nil {
		delete(m2, fp[len(fp)-1])
	}
}

func getParentMap(m map[string]interface{}, fp []string, create bool) (map[string]interface{}, error) {
	var ok bool
	for _, k := range fp[:len(fp)-1] {
		if m[k] == nil {
			if !create {
				return nil, nil
			}
			m[k] = map[string]interface{}{}
		}
		m, ok = m[k].(map[string]interface{})
		if !ok {
			return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "invalid field path %q at %q", strings.Join(fp, "."), k)
		}
	}
	return m, nil
}

func (c *collection) RevisionToBytes(rev interface{}) ([]byte, error) {
	r, ok := rev.(int64)
	if !ok {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "revision %v of type %[1]T is not an int64", rev)
	}
	return strconv.AppendInt(nil, r, 10), nil
}

func (c *collection) BytesToRevision(b []byte) (interface{}, error) {
	return strconv.ParseInt(string(b), 10, 64)
}

func (c *collection) As(i interface{}) bool { return false }

func (c *collection) ErrorAs(err error, i interface{}) bool { return false }

func (c *collection) Close() error {
	if c.opts.onClose != nil {
		c.opts.onClose()
	}
	return saveDocs(c.opts.Filename, c.docs)
}

type mapOfDocs = map[interface{}]storedDoc

func loadDocs(filename string) (mapOfDocs, error) {
	if filename == "" {
		return mapOfDocs{}, nil
	}
	f, err := os.Open(filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		return mapOfDocs{}, nil
	}
	defer f.Close()
	var m mapOfDocs
	if err := gob.NewDecoder(f).Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode from %q: %v", filename, err)
	}
	return m, nil
}

func saveDocs(filename string, m mapOfDocs) error {
	if filename == "" {
		return nil
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(m); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to encode to %q: %v", filename, err)
	}
	return f.Close()
}
