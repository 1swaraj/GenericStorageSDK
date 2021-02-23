package gcerrors

import (
	"context"

	"github.com/swaraj1802/GenericStorageSDK/internal/gcerr"
	"golang.org/x/xerrors"
)

type ErrorCode = gcerr.ErrorCode

const (
	OK ErrorCode = gcerr.OK

	Unknown ErrorCode = gcerr.Unknown

	NotFound ErrorCode = gcerr.NotFound

	AlreadyExists ErrorCode = gcerr.AlreadyExists

	InvalidArgument ErrorCode = gcerr.InvalidArgument

	Internal ErrorCode = gcerr.Internal

	Unimplemented ErrorCode = gcerr.Unimplemented

	FailedPrecondition ErrorCode = gcerr.FailedPrecondition

	PermissionDenied ErrorCode = gcerr.PermissionDenied

	ResourceExhausted ErrorCode = gcerr.ResourceExhausted

	Canceled ErrorCode = gcerr.Canceled

	DeadlineExceeded ErrorCode = gcerr.DeadlineExceeded
)

func Code(err error) ErrorCode {
	if err == nil {
		return OK
	}
	var e *gcerr.Error
	if xerrors.As(err, &e) {
		return e.Code
	}
	if xerrors.Is(err, context.Canceled) {
		return Canceled
	}
	if xerrors.Is(err, context.DeadlineExceeded) {
		return DeadlineExceeded
	}
	return Unknown
}
