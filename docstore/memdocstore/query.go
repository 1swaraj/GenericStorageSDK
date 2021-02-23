package memdocstore

import (
	"context"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/docstore/driver"
)

func (c *collection) RunGetQuery(_ context.Context, q *driver.Query) (driver.DocumentIterator, error) {
	if q.BeforeQuery != nil {
		if err := q.BeforeQuery(func(interface{}) bool { return false }); err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var resultDocs []storedDoc
	for _, doc := range c.docs {
		if q.Limit > 0 && len(resultDocs) == q.Limit {
			break
		}
		if filtersMatch(q.Filters, doc) {
			resultDocs = append(resultDocs, doc)
		}
	}
	if q.OrderByField != "" {
		sortDocs(resultDocs, q.OrderByField, q.OrderAscending)
	}

	var fps [][]string
	if len(q.FieldPaths) > 0 && c.keyField != "" {
		fps = append([][]string{{c.keyField}}, q.FieldPaths...)
	} else {
		fps = q.FieldPaths
	}

	return &docIterator{
		docs:       resultDocs,
		fieldPaths: fps,
		revField:   c.opts.RevisionField,
	}, nil
}

func filtersMatch(fs []driver.Filter, doc storedDoc) bool {
	for _, f := range fs {
		if !filterMatches(f, doc) {
			return false
		}
	}
	return true
}

func filterMatches(f driver.Filter, doc storedDoc) bool {
	docval, err := getAtFieldPath(doc, f.FieldPath)

	if err != nil {
		return false
	}
	c, ok := compare(docval, f.Value)
	if !ok {
		return false
	}
	return applyComparison(f.Op, c)
}

func applyComparison(op string, c int) bool {
	switch op {
	case driver.EqualOp:
		return c == 0
	case ">":
		return c > 0
	case "<":
		return c < 0
	case ">=":
		return c >= 0
	case "<=":
		return c <= 0
	default:
		panic("bad op")
	}
}

func compare(x1, x2 interface{}) (int, bool) {
	v1 := reflect.ValueOf(x1)
	v2 := reflect.ValueOf(x2)
	if v1.Kind() == reflect.String && v2.Kind() == reflect.String {
		return strings.Compare(v1.String(), v2.String()), true
	}
	if cmp, err := driver.CompareNumbers(v1, v2); err == nil {
		return cmp, true
	}
	if t1, ok := x1.(time.Time); ok {
		if t2, ok := x2.(time.Time); ok {
			return driver.CompareTimes(t1, t2), true
		}
	}
	return 0, false
}

func sortDocs(docs []storedDoc, field string, asc bool) {
	sort.Slice(docs, func(i, j int) bool {
		c, ok := compare(docs[i][field], docs[j][field])
		if !ok {
			return false
		}
		if asc {
			return c < 0
		} else {
			return c > 0
		}
	})
}

type docIterator struct {
	docs       []storedDoc
	fieldPaths [][]string
	revField   string
	err        error
}

func (it *docIterator) Next(ctx context.Context, doc driver.Document) error {
	if it.err != nil {
		return it.err
	}
	if len(it.docs) == 0 {
		it.err = io.EOF
		return it.err
	}
	if err := decodeDoc(it.docs[0], doc, it.fieldPaths); err != nil {
		it.err = err
		return it.err
	}
	it.docs = it.docs[1:]
	return nil
}

func (it *docIterator) Stop() { it.err = io.EOF }

func (it *docIterator) As(i interface{}) bool { return false }

func (c *collection) QueryPlan(q *driver.Query) (string, error) {
	return "", nil
}
