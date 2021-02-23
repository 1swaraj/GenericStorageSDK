package oc

import (
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

func LatencyMeasure(pkg string) *stats.Float64Measure {
	return stats.Float64(
		pkg+"/latency",
		"Latency of method call",
		stats.UnitMilliseconds)
}

var (
	MethodKey   = tag.MustNewKey("gocdk_method")
	StatusKey   = tag.MustNewKey("gocdk_status")
	ProviderKey = tag.MustNewKey("gocdk_provider")
)

func Views(pkg string, latencyMeasure *stats.Float64Measure) []*view.View {
	return []*view.View{
		{
			Name:        pkg + "/completed_calls",
			Measure:     latencyMeasure,
			Description: "Count of method calls by provider, method and status.",
			TagKeys:     []tag.Key{ProviderKey, MethodKey, StatusKey},
			Aggregation: view.Count(),
		},
		{
			Name:        pkg + "/latency",
			Measure:     latencyMeasure,
			Description: "Distribution of method latency, by provider and method.",
			TagKeys:     []tag.Key{ProviderKey, MethodKey},
			Aggregation: ocgrpc.DefaultMillisecondsDistribution,
		},
	}
}
