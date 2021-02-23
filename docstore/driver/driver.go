package driver

import (
	"context"

	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
)

type Collection interface {
	Key(Document) (interface{}, error)

	RevisionField() string

	RunActions(ctx context.Context, actions []*Action, opts *RunActionsOptions) ActionListError

	RunGetQuery(context.Context, *Query) (DocumentIterator, error)

	QueryPlan(*Query) (string, error)

	RevisionToBytes(interface{}) ([]byte, error)

	BytesToRevision([]byte) (interface{}, error)

	As(i interface{}) bool

	ErrorAs(err error, i interface{}) bool

	ErrorCode(error) gcerrors.ErrorCode

	Close() error
}

type DeleteQueryer interface {
	RunDeleteQuery(context.Context, *Query) error
}

type UpdateQueryer interface {
	RunUpdateQuery(context.Context, *Query, []Mod) error
}

type ActionKind int

const (
	Create ActionKind = iota
	Replace
	Put
	Get
	Delete
	Update
)

type Action struct {
	Kind       ActionKind
	Doc        Document
	Key        interface{}
	FieldPaths [][]string
	Mods       []Mod
	Index      int
}

type Mod struct {
	FieldPath []string
	Value     interface{}
}

type IncOp struct {
	Amount interface{}
}

type ActionListError []struct {
	Index int
	Err   error
}

func NewActionListError(errs []error) ActionListError {
	var alerr ActionListError
	for i, err := range errs {
		if err != nil {
			alerr = append(alerr, struct {
				Index int
				Err   error
			}{i, err})
		}
	}
	return alerr
}

type RunActionsOptions struct {
	BeforeDo func(asFunc func(interface{}) bool) error
}

type Query struct {
	FieldPaths [][]string

	Filters []Filter

	Limit int

	OrderByField string

	OrderAscending bool

	BeforeQuery func(asFunc func(interface{}) bool) error
}

type Filter struct {
	FieldPath []string
	Op        string
	Value     interface{}
}

type DocumentIterator interface {
	Next(context.Context, Document) error

	Stop()

	As(i interface{}) bool
}

const EqualOp = "="
