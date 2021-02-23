package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/googleapis/gax-go/v2"
)

func Call(ctx context.Context, bo gax.Backoff, isRetryable func(error) bool, f func() error) error {
	return call(ctx, bo, isRetryable, f, gax.Sleep)
}

func call(ctx context.Context, bo gax.Backoff, isRetryable func(error) bool, f func() error,
	sleep func(context.Context, time.Duration) error) error {

	if err := ctx.Err(); err != nil {
		return &ContextError{CtxErr: err}
	}
	for {
		err := f()
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return err
		}
		if cerr := sleep(ctx, bo.Pause()); cerr != nil {
			return &ContextError{CtxErr: cerr, FuncErr: err}
		}
	}
}

type ContextError struct {
	CtxErr  error
	FuncErr error
}

func (e *ContextError) Error() string {
	return fmt.Sprintf("%v; last error: %v", e.CtxErr, e.FuncErr)
}

func (e *ContextError) Is(target error) bool {
	return e.CtxErr == target || e.FuncErr == target
}
