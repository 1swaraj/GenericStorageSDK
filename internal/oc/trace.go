package oc

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/swaraj1802/GenericStorageSDK/gcerrors"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

type Tracer struct {
	Package        string
	Provider       string
	LatencyMeasure *stats.Float64Measure
}

func ProviderName(driver interface{}) string {

	if driver == nil {
		return ""
	}
	t := reflect.TypeOf(driver)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath()
}

type startTimeKey struct{}

func (t *Tracer) Start(ctx context.Context, methodName string) context.Context {
	fullName := t.Package + "." + methodName
	ctx, _ = trace.StartSpan(ctx, fullName)
	ctx, err := tag.New(ctx,
		tag.Upsert(MethodKey, fullName),
		tag.Upsert(ProviderKey, t.Provider))
	if err != nil {

		panic(fmt.Sprintf("fullName=%q, provider=%q: %v", fullName, t.Provider, err))
	}
	return context.WithValue(ctx, startTimeKey{}, time.Now())
}

func (t *Tracer) End(ctx context.Context, err error) {
	startTime := ctx.Value(startTimeKey{}).(time.Time)
	elapsed := time.Since(startTime)
	code := gcerrors.Code(err)
	span := trace.FromContext(ctx)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(code), Message: err.Error()})
	}
	span.End()
	stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(StatusKey, fmt.Sprint(code))},
		t.LatencyMeasure.M(float64(elapsed.Nanoseconds())/1e6))
}
