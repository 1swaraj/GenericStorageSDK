package gcerr

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/swaraj1802/GenericStorageSDK/internal/retry"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrorCode int

const (
	OK ErrorCode = 0

	Unknown ErrorCode = 1

	NotFound ErrorCode = 2

	AlreadyExists ErrorCode = 3

	InvalidArgument ErrorCode = 4

	Internal ErrorCode = 5

	Unimplemented ErrorCode = 6

	FailedPrecondition ErrorCode = 7

	PermissionDenied ErrorCode = 8

	ResourceExhausted ErrorCode = 9

	Canceled ErrorCode = 10

	DeadlineExceeded ErrorCode = 11
)

type Error struct {
	Code  ErrorCode
	msg   string
	frame xerrors.Frame
	err   error
}

func (e *Error) Error() string {
	return fmt.Sprint(e)
}

func (e *Error) Format(s fmt.State, c rune) {
	xerrors.FormatError(e, s, c)
}

func (e *Error) FormatError(p xerrors.Printer) (next error) {
	if e.msg == "" {
		p.Printf("code=%v", e.Code)
	} else {
		p.Printf("%s (code=%v)", e.msg, e.Code)
	}
	e.frame.Format(p)
	return e.err
}

func (e *Error) Unwrap() error {
	return e.err
}

func New(c ErrorCode, err error, callDepth int, msg string) *Error {
	return &Error{
		Code:  c,
		msg:   msg,
		frame: xerrors.Caller(callDepth),
		err:   err,
	}
}

func Newf(c ErrorCode, err error, format string, args ...interface{}) *Error {
	return New(c, err, 2, fmt.Sprintf(format, args...))
}

func DoNotWrap(err error) bool {
	if xerrors.Is(err, io.EOF) {
		return true
	}
	if xerrors.Is(err, context.Canceled) {
		return true
	}
	if xerrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var r *retry.ContextError
	if xerrors.As(err, &r) {
		return true
	}
	return false
}

func GRPCCode(err error) ErrorCode {
	switch status.Code(err) {
	case codes.NotFound:
		return NotFound
	case codes.AlreadyExists:
		return AlreadyExists
	case codes.InvalidArgument:
		return InvalidArgument
	case codes.Internal:
		return Internal
	case codes.Unimplemented:
		return Unimplemented
	case codes.FailedPrecondition:
		return FailedPrecondition
	case codes.PermissionDenied:
		return PermissionDenied
	case codes.ResourceExhausted:
		return ResourceExhausted
	case codes.Canceled:
		return Canceled
	case codes.DeadlineExceeded:
		return DeadlineExceeded
	default:
		return Unknown
	}
}

func ErrorAs(err error, target interface{}, errorAs func(error, interface{}) bool) bool {
	if err == nil {
		return false
	}
	if target == nil {
		panic("ErrorAs target cannot be nil")
	}
	val := reflect.ValueOf(target)
	if val.Type().Kind() != reflect.Ptr || val.IsNil() {
		panic("ErrorAs target must be a non-nil pointer")
	}
	if e, ok := err.(*Error); ok {
		err = e.Unwrap()
	}
	return errorAs(err, target)
}
