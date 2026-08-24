// Copyright 2026 Redpanda Data, Inc.
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
	"context"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestWriteBatchNotConnected(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestWriteBatchConnectionError(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(*as.BatchPolicy, []as.BatchRecordIfc) error {
		return &as.AerospikeError{ResultCode: types.NETWORK_ERROR}
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.ErrorIs(t, err, service.ErrNotConnected)
}

func TestWriteBatchPartialFailure(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		require.Len(t, recs, 1)
		recs[0].BatchRec().ResultCode = types.KEY_BUSY
		return nil
	}

	batch := service.MessageBatch{service.NewMessage([]byte(`{"id":"u1","v":1}`))}
	indexer := batch.Index()

	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	var berr *service.BatchError
	require.ErrorAs(t, err, &berr)
	require.Equal(t, 1, berr.IndexedErrors())
	var msgErr error
	berr.WalkMessagesIndexedBy(indexer, func(_ int, _ *service.Message, e error) bool {
		msgErr = e
		return false
	})
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "hot key")
}

func TestWriteBatchSuccess(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	w.operate = func(_ *as.BatchPolicy, recs []as.BatchRecordIfc) error {
		for _, rec := range recs {
			rec.BatchRec().ResultCode = types.OK
		}
		return nil
	}

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"u1","v":1}`)),
	})
	require.NoError(t, err)
}

func TestConnectionTestCancelled(t *testing.T) {
	w := newTestWriter(t, baseConfig)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := w.ConnectionTest(ctx)
	require.Len(t, results, 1)
	require.Error(t, results[0].Err)
	assert.ErrorIs(t, results[0].Err, context.Canceled)
}
