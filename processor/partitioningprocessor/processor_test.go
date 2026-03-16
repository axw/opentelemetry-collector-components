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

package partitioningprocessor

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestConsumeLogs_SinglePartition(t *testing.T) {
	sink := new(consumertest.LogsSink)
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)

	logs := plog.NewLogs()
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("tenant.id", "t1")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("tenant.id", "t1")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log2")

	require.NoError(t, p.ConsumeLogs(context.Background(), logs))
	assert.Len(t, sink.AllLogs(), 1)
	assert.Equal(t, 2, sink.AllLogs()[0].ResourceLogs().Len())
}

func TestConsumeLogs_MultiplePartitions(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)

	logs := plog.NewLogs()
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("tenant.id", "t1")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("tenant.id", "t2")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log2")

	require.NoError(t, p.ConsumeLogs(context.Background(), logs))

	assert.Len(t, sink.allLogs, 2)

	// Collect all tenant_id metadata values
	tenants := make(map[string]bool)
	for _, md := range sink.allMetadata {
		vals := md.Get("tenant_id")
		require.Len(t, vals, 1)
		tenants[vals[0]] = true
	}
	assert.True(t, tenants["t1"])
	assert.True(t, tenants["t2"])
}

func TestConsumeLogs_NilAttributeValue(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["missing"]`},
	)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("other", "value")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	require.NoError(t, p.ConsumeLogs(context.Background(), logs))
	require.Len(t, sink.allLogs, 1)

	// Missing attribute should produce empty string in metadata
	vals := sink.allMetadata[0].Get("tenant_id")
	require.Len(t, vals, 1)
	assert.Equal(t, "", vals[0])
}

func TestConsumeLogs_MultipleKeys(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		PartitionKeyConfig{Name: "service", Value: `resource.attributes["service.name"]`},
	)

	logs := plog.NewLogs()
	// Same tenant, different service
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("tenant.id", "t1")
	rl1.Resource().Attributes().PutStr("service.name", "svc-a")
	rl1.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("tenant.id", "t1")
	rl2.Resource().Attributes().PutStr("service.name", "svc-b")
	rl2.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log2")

	require.NoError(t, p.ConsumeLogs(context.Background(), logs))
	assert.Len(t, sink.allLogs, 2)
}

func TestConsumeLogs_LogLevelPartitioning(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
	)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test")
	sl := rl.ScopeLogs().AppendEmpty()

	lr1 := sl.LogRecords().AppendEmpty()
	lr1.Body().SetStr("error log")
	lr1.SetSeverityText("ERROR")

	lr2 := sl.LogRecords().AppendEmpty()
	lr2.Body().SetStr("info log")
	lr2.SetSeverityText("INFO")

	lr3 := sl.LogRecords().AppendEmpty()
	lr3.Body().SetStr("another error")
	lr3.SetSeverityText("ERROR")

	require.NoError(t, p.ConsumeLogs(context.Background(), logs))
	assert.Len(t, sink.allLogs, 2)

	severities := make(map[string]int)
	for i, md := range sink.allMetadata {
		vals := md.Get("severity")
		require.Len(t, vals, 1)
		severities[vals[0]] = sink.allLogs[i].LogRecordCount()
	}
	assert.Equal(t, 2, severities["ERROR"])
	assert.Equal(t, 1, severities["INFO"])
}

func TestConsumeLogs_PreservesExistingMetadata(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("tenant.id", "t1")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	// Set up existing client metadata on the incoming context.
	info := client.Info{Metadata: client.NewMetadata(map[string][]string{
		"existing_key": {"existing_value"},
	})}
	ctx := client.NewContext(context.Background(), info)

	require.NoError(t, p.ConsumeLogs(ctx, logs))
	require.Len(t, sink.allMetadata, 1)

	md := sink.allMetadata[0]
	// Partition key should be present.
	assert.Equal(t, []string{"t1"}, md.Get("tenant_id"))
	// Existing metadata should be preserved.
	assert.Equal(t, []string{"existing_value"}, md.Get("existing_key"))
}

func TestConsumeLogs_ScopePartitioning(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "scope_name", Value: `scope.name`},
	)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test")

	sl1 := rl.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("scope-a")
	sl1.LogRecords().AppendEmpty().Body().SetStr("log1")
	sl1.LogRecords().AppendEmpty().Body().SetStr("log2")

	sl2 := rl.ScopeLogs().AppendEmpty()
	sl2.Scope().SetName("scope-b")
	sl2.LogRecords().AppendEmpty().Body().SetStr("log3")

	require.NoError(t, p.ConsumeLogs(context.Background(), logs))
	assert.Len(t, sink.allLogs, 2)

	scopes := make(map[string]int)
	for i, md := range sink.allMetadata {
		vals := md.Get("scope_name")
		require.Len(t, vals, 1)
		scopes[vals[0]] = sink.allLogs[i].LogRecordCount()
	}
	assert.Equal(t, 2, scopes["scope-a"])
	assert.Equal(t, 1, scopes["scope-b"])
}

func TestCapabilities(t *testing.T) {
	p := &partitioningProcessor{}
	assert.False(t, p.Capabilities().MutatesData)
}

func TestConsumeLogs_NonStringValueErrors(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "count", Value: `resource.attributes["count"]`},
	)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutInt("count", 42)
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log1")

	err := p.ConsumeLogs(context.Background(), logs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected value expression to evaluate to a string")
	assert.Empty(t, sink.allLogs)
}

func TestConsumeLogs_KeyOrderInvariance(t *testing.T) {
	build := func() plog.Logs {
		logs := plog.NewLogs()
		for _, tenant := range []string{"t1", "t2", "t1"} {
			for _, svc := range []string{"svc-a", "svc-b"} {
				rl := logs.ResourceLogs().AppendEmpty()
				rl.Resource().Attributes().PutStr("tenant.id", tenant)
				rl.Resource().Attributes().PutStr("service.name", svc)
				rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("x")
			}
		}
		return logs
	}

	tenantFirst := &metadataCaptureSink{}
	pA := createTestProcessor(t, tenantFirst,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		PartitionKeyConfig{Name: "service", Value: `resource.attributes["service.name"]`},
	)
	require.NoError(t, pA.ConsumeLogs(context.Background(), build()))

	serviceFirst := &metadataCaptureSink{}
	pB := createTestProcessor(t, serviceFirst,
		PartitionKeyConfig{Name: "service", Value: `resource.attributes["service.name"]`},
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)
	require.NoError(t, pB.ConsumeLogs(context.Background(), build()))

	summarize := func(s *metadataCaptureSink) map[string]int {
		out := make(map[string]int)
		for i, md := range s.allMetadata {
			tenant := md.Get("tenant_id")[0]
			svc := md.Get("service")[0]
			out[tenant+"|"+svc] = s.allLogs[i].LogRecordCount()
		}
		return out
	}
	assert.Equal(t, summarize(tenantFirst), summarize(serviceFirst))
	assert.Len(t, tenantFirst.allLogs, 4) // 2 tenants x 2 services
}

func TestConsumeLogs_LogRecordPartitionerDedup(t *testing.T) {
	// One source RL + SL, records split across two partitions — each
	// partition's output should have exactly one RL and one SL.
	t.Run("split across partitions", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)

		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "test")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("s")
		for _, sev := range []string{"ERROR", "INFO", "ERROR"} {
			lr := sl.LogRecords().AppendEmpty()
			lr.SetSeverityText(sev)
		}

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 2)
		for _, ld := range sink.allLogs {
			assert.Equal(t, 1, ld.ResourceLogs().Len())
			assert.Equal(t, 1, ld.ResourceLogs().At(0).ScopeLogs().Len())
		}
	})

	// One source RL + SL, all records → same partition, output has exactly
	// one RL and one SL (no duplication).
	t.Run("all same partition", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)

		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "test")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("s")
		for i := 0; i < 3; i++ {
			sl.LogRecords().AppendEmpty().SetSeverityText("ERROR")
		}

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 1)
		ld := sink.allLogs[0]
		require.Equal(t, 1, ld.ResourceLogs().Len())
		destRL := ld.ResourceLogs().At(0)
		require.Equal(t, 1, destRL.ScopeLogs().Len())
		assert.Equal(t, 3, destRL.ScopeLogs().At(0).LogRecords().Len())
	})

	// Two source RLs, records landing in same partition — preserve source
	// identity, so the partition has two dest RLs (not merged).
	t.Run("two source RLs same partition", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)

		logs := plog.NewLogs()
		for _, svc := range []string{"svc-a", "svc-b"} {
			rl := logs.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", svc)
			rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetSeverityText("ERROR")
		}

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 1)
		assert.Equal(t, 2, sink.allLogs[0].ResourceLogs().Len())
	})
}

func TestConsumeLogs_ScopePartitionerDedup(t *testing.T) {
	// Two scopes under same RL, both landing in same partition — one dest
	// RL with two dest SLs.
	t.Run("two scopes same RL same partition", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
		)

		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("tenant.id", "t1")
		rl.ScopeLogs().AppendEmpty().Scope().SetName("scope-a")
		rl.ScopeLogs().AppendEmpty().Scope().SetName("scope-b")

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 1)
		ld := sink.allLogs[0]
		require.Equal(t, 1, ld.ResourceLogs().Len())
		assert.Equal(t, 2, ld.ResourceLogs().At(0).ScopeLogs().Len())
	})

	// Two scopes under different RLs, both landing in same partition — two
	// dest RLs each with one dest SL.
	t.Run("two scopes different RLs same partition", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "scope_name", Value: `scope.name`},
		)

		logs := plog.NewLogs()
		for _, svc := range []string{"svc-a", "svc-b"} {
			rl := logs.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("service.name", svc)
			rl.ScopeLogs().AppendEmpty().Scope().SetName("shared-scope")
		}

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 1)
		ld := sink.allLogs[0]
		require.Equal(t, 2, ld.ResourceLogs().Len())
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			assert.Equal(t, 1, ld.ResourceLogs().At(i).ScopeLogs().Len())
		}
	})
}

func TestConsumeLogs_PreservesSchemaURL(t *testing.T) {
	t.Run("scope partitioner", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "scope_name", Value: `scope.name`},
		)

		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl("https://example.com/resource-schema")
		rl.Resource().Attributes().PutStr("service.name", "test")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.SetSchemaUrl("https://example.com/scope-schema")
		sl.Scope().SetName("s")
		sl.LogRecords().AppendEmpty()

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 1)
		destRL := sink.allLogs[0].ResourceLogs().At(0)
		assert.Equal(t, "https://example.com/resource-schema", destRL.SchemaUrl())
		assert.Equal(t, "https://example.com/scope-schema", destRL.ScopeLogs().At(0).SchemaUrl())
	})

	t.Run("log record partitioner", func(t *testing.T) {
		sink := &metadataCaptureSink{}
		p := createTestProcessor(t, sink,
			PartitionKeyConfig{Name: "severity", Value: `log.severity_text`},
		)

		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.SetSchemaUrl("https://example.com/resource-schema")
		sl := rl.ScopeLogs().AppendEmpty()
		sl.SetSchemaUrl("https://example.com/scope-schema")
		sl.Scope().SetName("s")
		sl.LogRecords().AppendEmpty().SetSeverityText("ERROR")

		require.NoError(t, p.ConsumeLogs(context.Background(), logs))
		require.Len(t, sink.allLogs, 1)
		destRL := sink.allLogs[0].ResourceLogs().At(0)
		assert.Equal(t, "https://example.com/resource-schema", destRL.SchemaUrl())
		assert.Equal(t, "https://example.com/scope-schema", destRL.ScopeLogs().At(0).SchemaUrl())
	})
}

func TestConsumeLogs_EmptyInput(t *testing.T) {
	sink := &metadataCaptureSink{}
	p := createTestProcessor(t, sink,
		PartitionKeyConfig{Name: "tenant_id", Value: `resource.attributes["tenant.id"]`},
	)
	require.NoError(t, p.ConsumeLogs(context.Background(), plog.NewLogs()))
	assert.Empty(t, sink.allLogs)
}

func createTestProcessor(t *testing.T, next consumer.Logs, keys ...PartitionKeyConfig) *partitioningProcessor {
	t.Helper()
	cfg := &Config{Keys: keys}
	settings := processortest.NewNopSettings(typ)
	partitioner, keyNames, err := newLogsPartitioner(cfg, settings.TelemetrySettings)
	require.NoError(t, err)
	return &partitioningProcessor{
		keyNames:        keyNames,
		logsPartitioner: partitioner,
		nextLogs:        next,
	}
}

type metadataCaptureSink struct {
	mu          sync.Mutex
	allLogs     []plog.Logs
	allMetadata []client.Metadata
}

func (s *metadataCaptureSink) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := client.FromContext(ctx)
	s.allLogs = append(s.allLogs, ld)
	s.allMetadata = append(s.allMetadata, info.Metadata)
	return nil
}

func (s *metadataCaptureSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}
