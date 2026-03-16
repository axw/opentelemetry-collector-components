// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package partitioningprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/partitioningprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"golang.org/x/sync/errgroup"
)

type partitioningProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	keyNames        []string
	logsPartitioner logsPartitioner
	nextLogs        consumer.Logs
	nextMetrics     consumer.Metrics
	nextTraces      consumer.Traces
	nextProfiles    xconsumer.Profiles
}

func (p *partitioningProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *partitioningProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	partitions, err := p.logsPartitioner.partitionLogs(ctx, ld)
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, part := range partitions {
		g.Go(func() error {
			return p.nextLogs.ConsumeLogs(contextWithMetadata(ctx, p.keyNames, part.values), part.logs)
		})
	}
	return g.Wait()
}

// TODO: implement metrics partitioning
func (p *partitioningProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return p.nextMetrics.ConsumeMetrics(ctx, md)
}

// TODO: implement traces partitioning
func (p *partitioningProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return p.nextTraces.ConsumeTraces(ctx, td)
}

// TODO: implement profiles partitioning
func (p *partitioningProcessor) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	return p.nextProfiles.ConsumeProfiles(ctx, pd)
}

func contextWithMetadata(ctx context.Context, keyNames, values []string) context.Context {
	info := client.FromContext(ctx)
	md := make(map[string][]string, len(keyNames))
	for k := range info.Metadata.Keys() {
		md[k] = info.Metadata.Get(k)
	}
	for i, k := range keyNames {
		md[k] = []string{values[i]}
	}
	info.Metadata = client.NewMetadata(md)
	return client.NewContext(ctx, info)
}
