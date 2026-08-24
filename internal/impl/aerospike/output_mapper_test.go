// Copyright 2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newTestWriter builds a writer from YAML, which exercises the config spec and
// the parsing together rather than hand-assembling a config struct that could
// drift from what a real pipeline produces.
func newTestWriter(t *testing.T, yaml string) *aerospikeWriter {
	t.Helper()
	conf, err := outputSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	parsed, err := parseOutputConfig(conf)
	require.NoError(t, err)
	return &aerospikeWriter{conf: parsed, conn: NewConnection(parsed.client, nil)}
}

// mapOne maps a single message through the batch mapper, which is how the
// writer resolves every interpolated field.
func mapOne(t *testing.T, w *aerospikeWriter, m *service.Message) (*pendingOp, error) {
	t.Helper()
	return newBatchMapper(w.conf, service.MessageBatch{m}).mapMessage(0)
}

const baseConfig = `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! json("id") }'
bins: 'root = this.without("id")'
`

func TestParseTTL(t *testing.T) {
	tests := []struct {
		in      string
		want    uint32
		wantErr bool
	}{
		{"0s", as.TTLServerDefault, false},
		{"", as.TTLServerDefault, false},
		{"never", as.TTLDontExpire, false},
		{"keep", as.TTLDontUpdate, false},
		{"24h", 86400, false},
		{"90s", 90, false},
		// Rounding a sub-second TTL to zero would silently mean "namespace
		// default", which is the opposite of what was asked for.
		{"500ms", 0, true},
		{"-5s", 0, true},
		{"nonsense", 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseTTL(tc.in)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseOpKind(t *testing.T) {
	for in, want := range map[string]opKind{
		"write":       opWrite,
		"update":      opWrite,
		"replace":     opReplace,
		"create_only": opCreateOnly,
		"update_only": opUpdateOnly,
		"delete":      opDelete,
		"DELETE":      opDelete,
	} {
		got, err := parseOpKind(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	_, err := parseOpKind("upsert")
	assert.Error(t, err)
}

func TestMapMessage(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	msg := service.NewMessage([]byte(`{"id":"u1","name":"Ada","score":10}`))
	op, err := mapOne(t, w, msg)
	require.NoError(t, err)

	assert.Equal(t, opWrite, op.kind)
	assert.Equal(t, "test", op.key.Namespace())
	assert.Equal(t, "users", op.key.SetName())
	assert.Equal(t, "u1", op.key.Value().GetObject())
	assert.Equal(t, []string{"name", "score"}, op.binOrder)
	assert.Equal(t, "Ada", op.bins["name"])
	assert.Equal(t, int64(10), op.bins["score"])
}

func TestMapMessageRejectsLongBinName(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	msg := service.NewMessage([]byte(`{"id":"u1","this_field_name_is_far_too_long":1}`))
	_, err := mapOne(t, w, msg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 15")
}

func TestMapMessageTombstoneBecomesDelete(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! meta("k") }'
bins: 'root = this'
`)

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("k", "u1")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opDelete, op.kind)
	assert.Empty(t, op.binOrder)
}

func TestMapMessageStructuredObjectIsNotTombstone(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{"id": "u1", "v": 1})

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opWrite, op.kind)
}

func TestMapMessageDynamicOperation(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! json("id") }'
operation: '${! if json("op") == "d" { "delete" } else { "write" } }'
tombstone_as_delete: false
bins: 'root = this.without("id", "op")'
`)

	del, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","op":"d"}`)))
	require.NoError(t, err)
	assert.Equal(t, opDelete, del.kind)

	upd, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","op":"c","v":1}`)))
	require.NoError(t, err)
	assert.Equal(t, opWrite, upd.kind)
}

func TestMapMessageIntKeyType(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
key_type: int
bins: 'root = this.without("id")'
`)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"7","v":1}`)))
	require.NoError(t, err)
	assert.Equal(t, int64(7), op.key.Value().GetObject())

	_, err = mapOne(t, w, service.NewMessage([]byte(`{"id":"abc","v":1}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not an integer")
}

func TestMapMessageFencing(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
bins: 'root = this.without("id")'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)

	msg := service.NewMessage([]byte(`{"id":"u1","v":1}`))
	msg.MetaSet("kafka_offset", "42")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.True(t, op.hasFence)
	assert.Equal(t, int64(42), op.fence)
	assert.Equal(t, int64(42), op.bins["_off"])
}

func TestMapMessageFencedDeleteWritesTombstone(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! meta("k") }'
bins: 'root = this'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("k", "u1")
	msg.MetaSet("kafka_offset", "9")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.Equal(t, opDelete, op.kind)
	assert.True(t, op.hasFence)
	assert.Equal(t, int64(9), op.fence)
	assert.Equal(t, int64(9), op.bins["_off"])
	assert.Equal(t, true, op.bins["_deleted"])
}

func TestConfigRejectsFenceBinMatchingTombstoneBin(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
key: 'k'
fencing:
  enabled: true
  bin: _deleted
  tombstone_bin: _deleted
`, nil)
	require.NoError(t, err)

	_, err = parseOutputConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be the same")
}

func TestMapMessageRejectsEmptyKey(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	_, err := mapOne(t, w, service.NewMessage([]byte(`{"name":"Ada"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a usable record key")
}

func TestMapMessageRejectsEmptyBins(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
tombstone_as_delete: false
bins: 'root = {}'
`)

	_, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "produced no bins")
}

func TestConfigRejectsBadStaticOperation(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
key: 'k'
operation: upsert
`, nil)
	require.NoError(t, err)

	_, err = parseOutputConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown operation")
}

func TestMapMessageGeneration(t *testing.T) {
	w := newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
key: '${! json("id") }'
bins: 'root = this.without("id")'
generation: '${! meta("aerospike_generation") }'
`)

	msg := service.NewMessage([]byte(`{"id":"u1","v":1}`))
	msg.MetaSet("aerospike_generation", "7")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)
	assert.True(t, op.hasGeneration)
	assert.Equal(t, uint32(7), op.generation)

	rec := w.buildRecord(op)
	write, ok := rec.(*as.BatchWrite)
	require.True(t, ok)
	assert.Equal(t, as.EXPECT_GEN_EQUAL, write.Policy.GenerationPolicy)
	assert.Equal(t, uint32(7), write.Policy.Generation)
}

func TestMapMessageOmitsGenerationWhenEmpty(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","v":1}`)))
	require.NoError(t, err)
	assert.False(t, op.hasGeneration)

	rec := w.buildRecord(op)
	write, ok := rec.(*as.BatchWrite)
	require.True(t, ok)
	assert.Equal(t, as.NONE, write.Policy.GenerationPolicy)
}

func TestConfigRejectsLongFenceBin(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
hosts: [ "localhost:3000" ]
namespace: test
key: 'k'
fencing:
  enabled: true
  bin: this_bin_name_is_too_long
`, nil)
	require.NoError(t, err)

	_, err = parseOutputConfig(conf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 15")
}

func TestParseConfigSkillDefaults(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	assert.Equal(t, uint32(as.TTLDontUpdate), w.conf.staticTTL)
	assert.Equal(t, 0, w.conf.batchPolicy.MaxRetries)
	assert.Equal(t, as.COMMIT_ALL, w.conf.writePolicy.CommitLevel)
	assert.Equal(t, as.COMMIT_ALL, w.conf.deletePolicy.CommitLevel)
	assert.Equal(t, 0, w.conf.maxRecordBytes)
}

func TestParseCommitLevelMaster(t *testing.T) {
	w := newTestWriter(t, baseConfig+"commit_level: master\n")
	assert.Equal(t, as.COMMIT_MASTER, w.conf.writePolicy.CommitLevel)
	assert.Equal(t, as.COMMIT_MASTER, w.conf.deletePolicy.CommitLevel)
}

func TestMapMessageRejectsOversizeRecord(t *testing.T) {
	w := newTestWriter(t, baseConfig+"max_record_bytes: 32\n")
	_, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","blob":"abcdefghijklmnopqrstuvwxyz0123456789"}`)))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_record_bytes")
}
