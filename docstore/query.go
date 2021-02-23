package docstore

import (
	"context"
	"io"
	"reflect"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/docstore/driver"
	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
)

type Query struct {
	coll *Collection
	dq   *driver.Query
	err  error
}

func (c *Collection) Query() *Query {
	return &Query{coll: c, dq: &driver.Query{}}
}

func (q *Query) Where(fp FieldPath, op string, value interface{}) *Query {
	if q.err != nil {
		return q
	}
	pfp, err := parseFieldPath(fp)
	if err != nil {
		q.err = err
		return q
	}
	if !validOp[op] {
		return q.invalidf("invalid filter operator: %q. Use one of: =, >, <, >=, <=", op)
	}
	if !validFilterValue(value) {
		return q.invalidf("invalid filter value: %v", value)
	}
	q.dq.Filters = append(q.dq.Filters, driver.Filter{
		FieldPath: pfp,
		Op:        op,
		Value:     value,
	})
	return q
}

var validOp = map[string]bool{
	"=":  true,
	">":  true,
	"<":  true,
	">=": true,
	"<=": true,
}

func validFilterValue(v interface{}) bool {
	if v == nil {
		return false
	}
	if _, ok := v.(time.Time); ok {
		return true
	}
	switch reflect.TypeOf(v).Kind() {
	case reflect.String:
		return true
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

func (q *Query) Limit(n int) *Query {
	if q.err != nil {
		return q
	}
	if n <= 0 {
		return q.invalidf("limit value of %d must be greater than zero", n)
	}
	if q.dq.Limit > 0 {
		return q.invalidf("query can have at most one limit clause")
	}
	q.dq.Limit = n
	return q
}

const (
	Ascending  = "asc"
	Descending = "desc"
)

func (q *Query) OrderBy(field, direction string) *Query {
	if q.err != nil {
		return q
	}
	if field == "" {
		return q.invalidf("OrderBy: empty field")
	}
	if direction != Ascending && direction != Descending {
		return q.invalidf("OrderBy: direction must be one of %q or %q", Ascending, Descending)
	}
	if q.dq.OrderByField != "" {
		return q.invalidf("a query can have at most one OrderBy")
	}
	q.dq.OrderByField = field
	q.dq.OrderAscending = (direction == Ascending)
	return q
}

func (q *Query) BeforeQuery(f func(asFunc func(interface{}) bool) error) *Query {
	q.dq.BeforeQuery = f
	return q
}

func (q *Query) Get(ctx context.Context, fps ...FieldPath) *DocumentIterator {
	return q.get(ctx, true, fps...)
}

func (q *Query) get(ctx context.Context, oc bool, fps ...FieldPath) *DocumentIterator {
	dcoll := q.coll.driver
	if err := q.initGet(fps); err != nil {
		return &DocumentIterator{err: wrapError(dcoll, err)}
	}

	var err error
	if oc {
		ctx = q.coll.tracer.Start(ctx, "Query.Get")
		defer func() { q.coll.tracer.End(ctx, err) }()
	}
	it, err := dcoll.RunGetQuery(ctx, q.dq)
	return &DocumentIterator{iter: it, coll: q.coll, err: wrapError(dcoll, err)}
}

func (q *Query) initGet(fps []FieldPath) error {
	if q.err != nil {
		return q.err
	}
	if err := q.coll.checkClosed(); err != nil {
		return errClosed
	}
	pfps, err := parseFieldPaths(fps)
	if err != nil {
		return err
	}
	q.dq.FieldPaths = pfps
	if q.dq.OrderByField != "" && len(q.dq.Filters) > 0 {
		found := false
		for _, f := range q.dq.Filters {
			if len(f.FieldPath) == 1 && f.FieldPath[0] == q.dq.OrderByField {
				found = true
				break
			}
		}
		if !found {
			return gcerr.Newf(gcerr.InvalidArgument, nil, "OrderBy field %s must appear in a Where clause",
				q.dq.OrderByField)
		}
	}
	return nil
}

func (q *Query) invalidf(format string, args ...interface{}) *Query {
	q.err = gcerr.Newf(gcerr.InvalidArgument, nil, format, args...)
	return q
}

type DocumentIterator struct {
	iter driver.DocumentIterator
	coll *Collection
	err  error
}

func (it *DocumentIterator) Next(ctx context.Context, dst Document) error {
	if it.err != nil {
		return it.err
	}
	if err := it.coll.checkClosed(); err != nil {
		it.err = err
		return it.err
	}
	ddoc, err := driver.NewDocument(dst)
	if err != nil {
		it.err = wrapError(it.coll.driver, err)
		return it.err
	}
	it.err = wrapError(it.coll.driver, it.iter.Next(ctx, ddoc))
	return it.err
}

func (it *DocumentIterator) Stop() {
	if it.err != nil {
		return
	}
	it.err = io.EOF
	it.iter.Stop()
}

func (it *DocumentIterator) As(i interface{}) bool {
	if i == nil || it.iter == nil {
		return false
	}
	return it.iter.As(i)
}

func (q *Query) Plan(fps ...FieldPath) (string, error) {
	if err := q.initGet(fps); err != nil {
		return "", err
	}
	return q.coll.driver.QueryPlan(q.dq)
}
