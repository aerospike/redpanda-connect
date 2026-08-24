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
	"slices"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func testKey(t *testing.T, k string) *as.Key {
	t.Helper()
	key, err := as.NewKey("test", "users", k)
	require.NoError(t, err)
	return key
}

func opWith(t *testing.T, idx int, k string, kind opKind, bins map[string]any) *pendingOp {
	t.Helper()
	op := newPendingOp(idx, testKey(t, k), kind, 0)
	// Insert in sorted order so binOrder is deterministic, matching what the
	// real mapper produces.
	names := make([]string, 0, len(bins))
	for n := range bins {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		op.setBin(n, bins[n])
	}
	return op
}

// TestFold covers the merge rules that make coalescing safe. Each case states
// what applying the operations in sequence would leave on the server, and
// asserts the folded single write produces the same thing.
func TestFold(t *testing.T) {
	tests := []struct {
		name     string
		ops      []*pendingOp
		wantKind opKind
		wantBins map[string]any
	}{
		{
			name: "two writes merge their bins",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "later write wins on a shared bin",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"a": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 2},
		},
		{
			// replace defines the full record, so anything accumulated before
			// it is irrelevant.
			name: "replace discards earlier bins",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opReplace, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		{
			// A write after a replace merges into it, and the result must stay
			// a replace so bins already on the server are still cleared.
			name: "write after replace stays a replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplace, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "delete discards earlier bins",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opDelete, nil),
			},
			wantKind: opDelete,
			wantBins: map[string]any{},
		},
		{
			// The critical case. Skipping the delete and emitting a merging
			// write would leave pre-delete bins on the server, so the fold has
			// to promote to replace.
			name: "write after delete is promoted to replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opDelete, nil),
				opWith(t, 2, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		{
			name: "delete then write then delete ends deleted",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opDelete, nil),
				opWith(t, 1, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 2, "u1", opDelete, nil),
			},
			wantKind: opDelete,
			wantBins: map[string]any{},
		},
		// create_only / update_only must not steal the existence action of a
		// preceding write. Folding [write{a}, update_only{b}] into update_only{b}
		// is a poison batch: sequential application creates then updates, but
		// a lone update_only fails KEY_NOT_FOUND and nacks both messages forever.
		{
			name: "update_only after write stays a write",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "update_only after replace stays a replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplace, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "two update_onlys merge like two writes",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opUpdateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opUpdateOnly,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		// Folding [create_only{a}, write{b}] into create_only nacks the write
		// against an existing record. Promote to write so the write still lands.
		{
			name: "write after create_only is promoted to write",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opCreateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		{
			name: "write after update_only is promoted to write",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opUpdateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		// create_only after a write cannot succeed; keep the write so it is
		// not nacked as KEY_EXISTS, and drop the create_only bins.
		{
			name: "create_only after write is dropped",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opWrite, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opCreateOnly, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1},
		},
		{
			name: "create_only after replace is dropped",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opReplace, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opCreateOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"a": 1},
		},
		{
			name: "create_only after update_only is promoted to write",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opUpdateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opCreateOnly, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
		// Sequential delete then update_only leaves the record gone. Folding
		// to update_only would nack the delete as KEY_NOT_FOUND.
		{
			name: "update_only after delete stays a delete",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opDelete, nil),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opDelete,
			wantBins: map[string]any{},
		},
		// Sequential delete then create_only is a full-record rewrite.
		{
			name: "create_only after delete is promoted to replace",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opDelete, nil),
				opWith(t, 1, "u1", opCreateOnly, map[string]any{"b": 2}),
			},
			wantKind: opReplace,
			wantBins: map[string]any{"b": 2},
		},
		// One command cannot keep both existence checks, so promote to write
		// rather than poison either message.
		{
			name: "update_only after create_only is promoted to write",
			ops: []*pendingOp{
				opWith(t, 0, "u1", opCreateOnly, map[string]any{"a": 1}),
				opWith(t, 1, "u1", opUpdateOnly, map[string]any{"b": 2}),
			},
			wantKind: opWrite,
			wantBins: map[string]any{"a": 1, "b": 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acc := tc.ops[0]
			for _, next := range tc.ops[1:] {
				acc.fold(next)
			}

			assert.Equal(t, tc.wantKind, acc.kind)
			assert.Equal(t, tc.wantBins, acc.bins)

			// Every folded message must be represented so a failure can be
			// reported against all of them.
			assert.Len(t, acc.indexes, len(tc.ops))
		})
	}
}

func TestFoldTracksAllSourceIndexes(t *testing.T) {
	acc := opWith(t, 3, "u1", opWrite, map[string]any{"a": 1})
	acc.fold(opWith(t, 7, "u1", opWrite, map[string]any{"b": 2}))
	acc.fold(opWith(t, 9, "u1", opWrite, map[string]any{"c": 3}))

	assert.Equal(t, []int{3, 7, 9}, acc.indexes)
}

func TestFoldKeepsHigherFence(t *testing.T) {
	newer := opWith(t, 0, "u1", opWrite, map[string]any{"a": 10})
	newer.hasFence, newer.fence = true, 10
	stale := opWith(t, 1, "u1", opWrite, map[string]any{"a": 3})
	stale.hasFence, stale.fence = true, 3

	newer.fold(stale)
	assert.Equal(t, opWrite, newer.kind)
	assert.Equal(t, int64(10), newer.fence)
	assert.Equal(t, map[string]any{"a": 10}, newer.bins)
	assert.Equal(t, []int{0, 1}, newer.indexes)

	del := opWith(t, 2, "u1", opDelete, map[string]any{"_deleted": true})
	del.hasFence, del.fence = true, 11
	newer.fold(del)
	assert.Equal(t, opDelete, newer.kind)
	assert.Equal(t, int64(11), newer.fence)
	assert.Equal(t, true, newer.bins["_deleted"])
}

func TestFoldMergesWhenHigherFenceArrivesLater(t *testing.T) {
	// Sequential write{a}@3 then write{b}@10 leaves {a,b} under fence 10.
	// Replacing the payload with only {b} would drop the first write.
	older := opWith(t, 0, "u1", opWrite, map[string]any{"a": 1})
	older.hasFence, older.fence = true, 3
	newer := opWith(t, 1, "u1", opWrite, map[string]any{"b": 2})
	newer.hasFence, newer.fence = true, 10

	older.fold(newer)
	assert.Equal(t, opWrite, older.kind)
	assert.Equal(t, int64(10), older.fence)
	assert.Equal(t, map[string]any{"a": 1, "b": 2}, older.bins)
	assert.Equal(t, []int{0, 1}, older.indexes)
}

func TestFoldWriteAfterLowerFenceDeleteIsReplace(t *testing.T) {
	// Sequential delete@3 then write{a}@10 is a replace. Taking the write
	// wholesale would leave pre-delete bins on the server.
	del := opWith(t, 0, "u1", opDelete, nil)
	del.hasFence, del.fence = true, 3
	write := opWith(t, 1, "u1", opWrite, map[string]any{"a": 1})
	write.hasFence, write.fence = true, 10

	del.fold(write)
	assert.Equal(t, opReplace, del.kind)
	assert.Equal(t, int64(10), del.fence)
	assert.Equal(t, map[string]any{"a": 1}, del.bins)
}

func TestPlanBatchCoalescesRepeatedKeys(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u2","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","b":2}`)),
		service.NewMessage([]byte(`{"id":"u1","a":9}`)),
	}

	ops, failures := w.planBatch(batch)
	require.Empty(t, failures)

	// Four messages, two distinct keys, so two writes rather than four
	// commands contending on one record.
	require.Len(t, ops, 2)

	assert.Equal(t, []int{0, 2, 3}, ops[0].indexes)
	assert.Equal(t, int64(9), ops[0].bins["a"])
	assert.Equal(t, int64(2), ops[0].bins["b"])

	assert.Equal(t, []int{1}, ops[1].indexes)
}

func TestPlanBatchWriteThenUpdateOnlyStaysWrite(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\noperation: '${! json(\"op\") }'\n")

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","op":"write","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","op":"update_only","b":2}`)),
	}

	ops, failures := w.planBatch(batch)
	require.Empty(t, failures)
	require.Len(t, ops, 1)
	assert.Equal(t, opWrite, ops[0].kind)
	assert.Equal(t, []int{0, 1}, ops[0].indexes)
	assert.Equal(t, int64(1), ops[0].bins["a"])
	assert.Equal(t, int64(2), ops[0].bins["b"])
}

func TestPlanBatchWithoutCoalescing(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\ncoalesce_batch_keys: false\n")

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"id":"u1","b":2}`)),
	}

	ops, failures := w.planBatch(batch)
	require.Empty(t, failures)
	assert.Len(t, ops, 2)
}

func TestPlanBatchIsolatesMappingFailures(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","a":1}`)),
		service.NewMessage([]byte(`{"no_id":true}`)), // key resolves empty
		service.NewMessage([]byte(`{"id":"u3","a":3}`)),
	}

	ops, failures := w.planBatch(batch)

	// One bad message must not sink the two good ones.
	assert.Len(t, ops, 2)
	require.Len(t, failures, 1)
	assert.Contains(t, failures[1].Error(), "not a usable record key")
}

func TestBuildRecordAppliesOperationAndTTL(t *testing.T) {
	w := newTestWriter(t, baseConfig+"\nttl: 24h\noperation: replace\n")

	op, err := mapOne(t, w, service.NewMessage([]byte(`{"id":"u1","a":1}`)))
	require.NoError(t, err)

	rec := w.buildRecord(op)
	bw, ok := rec.(*as.BatchWrite)
	require.True(t, ok)

	assert.Equal(t, as.REPLACE, bw.Policy.RecordExistsAction)
	assert.Equal(t, uint32(86400), bw.Policy.Expiration)
	assert.Nil(t, bw.Policy.FilterExpression)
	assert.Len(t, bw.Ops, 1)
}

func TestBuildRecordAppliesFenceExpression(t *testing.T) {
	w := newTestWriter(t, baseConfig+`
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)

	msg := service.NewMessage([]byte(`{"id":"u1","a":1}`))
	msg.MetaSet("kafka_offset", "5")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)

	bw, ok := w.buildRecord(op).(*as.BatchWrite)
	require.True(t, ok)
	assert.NotNil(t, bw.Policy.FilterExpression)
	// Payload, fence bin, and a nil tombstone bin so a prior fenced delete is cleared.
	assert.Len(t, bw.Ops, 3)
}

func TestBuildRecordDelete(t *testing.T) {
	w := newTestWriter(t, baseConfig)

	op := newPendingOp(0, testKey(t, "u1"), opDelete, 0)
	rec := w.buildRecord(op)

	_, ok := rec.(*as.BatchDelete)
	assert.True(t, ok)
}

func TestBuildRecordFencedDeleteIsReplace(t *testing.T) {
	w := newTestWriter(t, baseConfig+`
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)

	msg := service.NewMessage([]byte(``))
	msg.MetaSet("kafka_offset", "4")
	// Key interpolation uses json("id") in baseConfig; set a body that still
	// counts as a tombstone after using an explicit key field instead.
	w = newTestWriter(t, `
hosts: [ "localhost:3000" ]
namespace: test
set: users
key: '${! meta("k") }'
bins: 'root = this'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("kafka_offset") }'
`)
	msg = service.NewMessage([]byte(``))
	msg.MetaSet("k", "u1")
	msg.MetaSet("kafka_offset", "4")

	op, err := mapOne(t, w, msg)
	require.NoError(t, err)

	bw, ok := w.buildRecord(op).(*as.BatchWrite)
	require.True(t, ok)
	assert.Equal(t, as.REPLACE, bw.Policy.RecordExistsAction)
	assert.NotNil(t, bw.Policy.FilterExpression)
	assert.Equal(t, uint32(as.TTLDontExpire), bw.Policy.Expiration)
}

func TestClassifyRecord(t *testing.T) {
	tests := []struct {
		name     string
		code     types.ResultCode
		kind     opKind
		wantErr  bool
		contains string
	}{
		{name: "ok", code: types.OK, kind: opWrite},
		// The fence rejected a stale message. That is the fence working, not a
		// delivery failure — nacking here would retry forever.
		{name: "filtered out is a success", code: types.FILTERED_OUT, kind: opWrite},
		// Deleting an absent record is the desired end state, which keeps
		// deletes idempotent under redelivery.
		{name: "missing key on delete is a success", code: types.KEY_NOT_FOUND_ERROR, kind: opDelete},
		{name: "missing key on update_only fails", code: types.KEY_NOT_FOUND_ERROR, kind: opUpdateOnly,
			wantErr: true, contains: "does not exist and the operation is update_only"},
		{name: "key busy explains hot keys", code: types.KEY_BUSY, kind: opWrite,
			wantErr: true, contains: "transaction-pending-limit"},
		{name: "forbidden explains nsup-period", code: types.FAIL_FORBIDDEN, kind: opWrite,
			wantErr: true, contains: "nsup-period"},
		{name: "record too big explains remodelling", code: types.RECORD_TOO_BIG, kind: opWrite,
			wantErr: true, contains: "max-record-size"},
		{name: "invalid namespace explains config", code: types.INVALID_NAMESPACE, kind: opWrite,
			wantErr: true, contains: "cannot be created at runtime"},
		{name: "no response is retryable", code: types.NO_RESPONSE, kind: opWrite,
			wantErr: true, contains: "retryable"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: tc.code}
			err := classifyRecord(rec, tc.kind)

			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.contains)
		})
	}
}

func TestClassifyRecordFlagsInDoubt(t *testing.T) {
	rec := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: types.TIMEOUT, InDoubt: true}
	err := classifyRecord(rec, opWrite)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "in doubt")
}

func TestFilteredOutCountIncludesFoldedIndexes(t *testing.T) {
	op := opWith(t, 0, "u1", opWrite, map[string]any{"a": 1})
	op.fold(opWith(t, 1, "u1", opWrite, map[string]any{"b": 2}))
	op.fold(opWith(t, 2, "u1", opWrite, map[string]any{"c": 3}))

	filtered := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: types.FILTERED_OUT}
	assert.Equal(t, 3, filteredOutCount(filtered, op))

	ok := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: types.OK}
	assert.Equal(t, 0, filteredOutCount(ok, op))
}

func TestHandleRecordFilteredOutIsSuccess(t *testing.T) {
	w := &aerospikeWriter{}
	op := opWith(t, 0, "u1", opWrite, map[string]any{"a": 1})
	rec := &as.BatchRecord{Key: testKey(t, "u1"), ResultCode: types.FILTERED_OUT}

	assert.NoError(t, w.handleRecord(rec, op))
}
