package docstore

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/swaraj1802/GenericStorageSDK/docstore/driver"
	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
	"github.com/swaraj1802/GenericStorageSDK/internal/oc"
)

type Document = interface{}

type Collection struct {
	driver driver.Collection
	tracer *oc.Tracer
	mu     sync.Mutex
	closed bool
}

const pkgName = "github.com/swaraj1802/GenericStorageSDK/docstore"

var (
	latencyMeasure = oc.LatencyMeasure(pkgName)

	OpenCensusViews = oc.Views(pkgName, latencyMeasure)
)

var NewCollection = newCollection

func newCollection(d driver.Collection) *Collection {
	c := &Collection{
		driver: d,
		tracer: &oc.Tracer{
			Package:        pkgName,
			Provider:       oc.ProviderName(d),
			LatencyMeasure: latencyMeasure,
		},
	}
	_, file, lineno, ok := runtime.Caller(1)
	runtime.SetFinalizer(c, func(c *Collection) {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if !closed {
			var caller string
			if ok {
				caller = fmt.Sprintf(" (%s:%d)", file, lineno)
			}
			log.Printf("A docstore.Collection was never closed%s", caller)
		}
	})
	return c
}

const DefaultRevisionField = "DocstoreRevision"

func (c *Collection) revisionField() string {
	if r := c.driver.RevisionField(); r != "" {
		return r
	}
	return DefaultRevisionField
}

type FieldPath string

func (c *Collection) Actions() *ActionList {
	return &ActionList{coll: c}
}

type ActionList struct {
	coll     *Collection
	actions  []*Action
	beforeDo func(asFunc func(interface{}) bool) error
}

type Action struct {
	kind       driver.ActionKind
	doc        Document
	fieldpaths []FieldPath
	mods       Mods
}

func (l *ActionList) add(a *Action) *ActionList {
	l.actions = append(l.actions, a)
	return l
}

func (l *ActionList) Create(doc Document) *ActionList {
	return l.add(&Action{kind: driver.Create, doc: doc})
}

func (l *ActionList) Replace(doc Document) *ActionList {
	return l.add(&Action{kind: driver.Replace, doc: doc})
}

func (l *ActionList) Put(doc Document) *ActionList {
	return l.add(&Action{kind: driver.Put, doc: doc})
}

func (l *ActionList) Delete(doc Document) *ActionList {

	return l.add(&Action{kind: driver.Delete, doc: doc})
}

func (l *ActionList) Get(doc Document, fps ...FieldPath) *ActionList {
	return l.add(&Action{
		kind:       driver.Get,
		doc:        doc,
		fieldpaths: fps,
	})
}

func (l *ActionList) Update(doc Document, mods Mods) *ActionList {
	return l.add(&Action{
		kind: driver.Update,
		doc:  doc,
		mods: mods,
	})
}

type Mods map[FieldPath]interface{}

func Increment(amount interface{}) interface{} {
	return driver.IncOp{amount}
}

type ActionListError []struct {
	Index int
	Err   error
}

func (e ActionListError) Error() string {
	var s []string
	for _, x := range e {
		s = append(s, fmt.Sprintf("at %d: %v", x.Index, x.Err))
	}
	return strings.Join(s, "; ")
}

func (e ActionListError) Unwrap() error {
	if len(e) == 1 {
		return e[0].Err
	}

	return nil
}

func (l *ActionList) BeforeDo(f func(asFunc func(interface{}) bool) error) *ActionList {
	l.beforeDo = f
	return l
}

func (l *ActionList) Do(ctx context.Context) error {
	return l.do(ctx, true)
}

func (l *ActionList) do(ctx context.Context, oc bool) (err error) {
	if err := l.coll.checkClosed(); err != nil {
		return ActionListError{{-1, errClosed}}
	}

	if oc {
		ctx = l.coll.tracer.Start(ctx, "ActionList.Do")
		defer func() { l.coll.tracer.End(ctx, err) }()
	}

	das, err := l.toDriverActions()
	if err != nil {
		return err
	}
	dopts := &driver.RunActionsOptions{BeforeDo: l.beforeDo}
	alerr := ActionListError(l.coll.driver.RunActions(ctx, das, dopts))
	if len(alerr) == 0 {
		return nil
	}
	for i := range alerr {
		alerr[i].Err = wrapError(l.coll.driver, alerr[i].Err)
	}
	return alerr
}

func (l *ActionList) toDriverActions() ([]*driver.Action, error) {
	var das []*driver.Action
	var alerr ActionListError

	type keyAndKind struct {
		key   interface{}
		isGet bool
	}
	seen := map[keyAndKind]bool{}
	for i, a := range l.actions {
		d, err := l.coll.toDriverAction(a)

		if err == nil && d.Key != nil {
			kk := keyAndKind{d.Key, d.Kind == driver.Get}
			if seen[kk] {
				err = gcerr.Newf(gcerr.InvalidArgument, nil, "duplicate key in action list: %v", d.Key)
			} else {
				seen[kk] = true
			}
		}
		if err != nil {
			alerr = append(alerr, struct {
				Index int
				Err   error
			}{i, wrapError(l.coll.driver, err)})
		} else {
			d.Index = i
			das = append(das, d)
		}
	}
	if len(alerr) > 0 {
		return nil, alerr
	}
	return das, nil
}

func (c *Collection) toDriverAction(a *Action) (*driver.Action, error) {
	ddoc, err := driver.NewDocument(a.doc)
	if err != nil {
		return nil, err
	}
	key, err := c.driver.Key(ddoc)
	if err != nil {
		if gcerrors.Code(err) != gcerr.InvalidArgument {
			err = gcerr.Newf(gcerr.InvalidArgument, err, "bad document key")
		}
		return nil, err
	}
	if key == nil || driver.IsEmptyValue(reflect.ValueOf(key)) {
		if a.kind != driver.Create {
			return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "missing document key")
		}

		key = nil
	}
	if reflect.ValueOf(key).Kind() == reflect.Ptr {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "keys cannot be pointers")
	}
	rev, _ := ddoc.GetField(c.revisionField())
	if a.kind == driver.Create && rev != nil {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "cannot create a document with a revision field")
	}
	kind := a.kind
	if kind == driver.Put && rev != nil {

		kind = driver.Replace
	}
	d := &driver.Action{Kind: kind, Doc: ddoc, Key: key}
	if a.fieldpaths != nil {
		d.FieldPaths, err = parseFieldPaths(a.fieldpaths)
		if err != nil {
			return nil, err
		}
	}
	if a.kind == driver.Update {
		d.Mods, err = toDriverMods(a.mods)
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

func parseFieldPaths(fps []FieldPath) ([][]string, error) {
	res := make([][]string, len(fps))
	for i, s := range fps {
		fp, err := parseFieldPath(s)
		if err != nil {
			return nil, err
		}
		res[i] = fp
	}
	return res, nil
}

func toDriverMods(mods Mods) ([]driver.Mod, error) {

	if len(mods) == 0 {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "no mods passed to Update")
	}

	var keys []string
	for k := range mods {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)

	var dmods []driver.Mod
	for _, k := range keys {
		k := FieldPath(k)
		v := mods[k]
		fp, err := parseFieldPath(k)
		if err != nil {
			return nil, err
		}
		for _, d := range dmods {
			if fpHasPrefix(fp, d.FieldPath) {
				return nil, gcerr.Newf(gcerr.InvalidArgument, nil,
					"field path %q is a prefix of %q", strings.Join(d.FieldPath, "."), k)
			}
		}
		if inc, ok := v.(driver.IncOp); ok && !isIncNumber(inc.Amount) {
			return nil, gcerr.Newf(gcerr.InvalidArgument, nil,
				"Increment amount %v of type %[1]T must be an integer or floating-point number", inc.Amount)
		}
		dmods = append(dmods, driver.Mod{FieldPath: fp, Value: v})
	}
	return dmods, nil
}

func fpHasPrefix(fp, prefix []string) bool {
	if len(fp) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if fp[i] != p {
			return false
		}
	}
	return true
}

func isIncNumber(x interface{}) bool {
	switch reflect.TypeOf(x).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func (l *ActionList) String() string {
	var as []string
	for _, a := range l.actions {
		as = append(as, a.String())
	}
	return "[" + strings.Join(as, ", ") + "]"
}

func (a *Action) String() string {
	buf := &strings.Builder{}
	fmt.Fprintf(buf, "%s(%v", a.kind, a.doc)
	for _, fp := range a.fieldpaths {
		fmt.Fprintf(buf, ", %s", fp)
	}
	for _, m := range a.mods {
		fmt.Fprintf(buf, ", %v", m)
	}
	fmt.Fprint(buf, ")")
	return buf.String()
}

func (c *Collection) Create(ctx context.Context, doc Document) error {
	if err := c.Actions().Create(doc).Do(ctx); err != nil {
		return err.(ActionListError).Unwrap()
	}
	return nil
}

func (c *Collection) Replace(ctx context.Context, doc Document) error {
	if err := c.Actions().Replace(doc).Do(ctx); err != nil {
		return err.(ActionListError).Unwrap()
	}
	return nil
}

func (c *Collection) Put(ctx context.Context, doc Document) error {
	if err := c.Actions().Put(doc).Do(ctx); err != nil {
		return err.(ActionListError).Unwrap()
	}
	return nil
}

func (c *Collection) Delete(ctx context.Context, doc Document) error {
	if err := c.Actions().Delete(doc).Do(ctx); err != nil {
		return err.(ActionListError).Unwrap()
	}
	return nil
}

func (c *Collection) Get(ctx context.Context, doc Document, fps ...FieldPath) error {
	if err := c.Actions().Get(doc, fps...).Do(ctx); err != nil {
		return err.(ActionListError).Unwrap()
	}
	return nil
}

func (c *Collection) Update(ctx context.Context, doc Document, mods Mods) error {
	if err := c.Actions().Update(doc, mods).Do(ctx); err != nil {
		return err.(ActionListError).Unwrap()
	}
	return nil
}

func parseFieldPath(fp FieldPath) ([]string, error) {
	if len(fp) == 0 {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "empty field path")
	}
	if !utf8.ValidString(string(fp)) {
		return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "invalid UTF-8 field path %q", fp)
	}
	parts := strings.Split(string(fp), ".")
	for _, p := range parts {
		if p == "" {
			return nil, gcerr.Newf(gcerr.InvalidArgument, nil, "empty component in field path %q", fp)
		}
	}
	return parts, nil
}

func (c *Collection) RevisionToString(rev interface{}) (string, error) {
	if rev == nil {
		return "", gcerr.Newf(gcerr.InvalidArgument, nil, "RevisionToString: nil revision")
	}
	bytes, err := c.driver.RevisionToBytes(rev)
	if err != nil {
		return "", wrapError(c.driver, err)
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func (c *Collection) StringToRevision(s string) (interface{}, error) {
	if s == "" {
		return "", gcerr.Newf(gcerr.InvalidArgument, nil, "StringToRevision: empty string")
	}
	bytes, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	rev, err := c.driver.BytesToRevision(bytes)
	if err != nil {
		return "", wrapError(c.driver, err)
	}
	return rev, nil
}

func (c *Collection) As(i interface{}) bool {
	if i == nil {
		return false
	}
	return c.driver.As(i)
}

var errClosed = gcerr.Newf(gcerr.FailedPrecondition, nil, "docstore: Collection has been closed")

func (c *Collection) Close() error {
	c.mu.Lock()
	prev := c.closed
	c.closed = true
	c.mu.Unlock()
	if prev {
		return errClosed
	}
	return wrapError(c.driver, c.driver.Close())
}

func (c *Collection) checkClosed() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errClosed
	}
	return nil
}

func wrapError(c driver.Collection, err error) error {
	if err == nil {
		return nil
	}
	if gcerr.DoNotWrap(err) {
		return err
	}
	if _, ok := err.(*gcerr.Error); ok {
		return err
	}
	return gcerr.New(c.ErrorCode(err), err, 2, "docstore")
}

func (c *Collection) ErrorAs(err error, i interface{}) bool {
	return gcerr.ErrorAs(err, i, c.driver.ErrorAs)
}
