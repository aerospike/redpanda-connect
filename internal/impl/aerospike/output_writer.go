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
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterBatchOutput("aerospike", outputSpec(), newAerospikeOutput)
}

func newAerospikeOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	batchPolicy, err := conf.FieldBatchPolicy(fieldBatching)
	if err != nil {
		return nil, batchPolicy, 0, err
	}
	// An unbatched database sink round-trips per message. If the user has not
	// expressed a preference, batch by default rather than shipping a pipeline
	// that is an order of magnitude slower than it should be. The framework
	// renders an unset policy as `count: 0` in the config reference, so say out
	// loud what actually got applied.
	if batchPolicy.IsNoop() {
		batchPolicy.Count = defaultBatchCount
		batchPolicy.Period = defaultBatchPeriod
		mgr.Logger().Infof(
			"No batching policy set, defaulting to %d messages or %s; set batching.count to 1 to write one message per command",
			defaultBatchCount, defaultBatchPeriod)
	}

	maxInFlight, err := conf.FieldMaxInFlight()
	if err != nil {
		return nil, batchPolicy, 0, err
	}

	parsed, err := parseOutputConfig(conf)
	if err != nil {
		return nil, batchPolicy, 0, err
	}

	return &aerospikeWriter{
		conf:        parsed,
		conn:        NewConnection(parsed.client, mgr.Logger()),
		log:         mgr.Logger(),
		filteredOut: mgr.Metrics().NewCounter("aerospike_filtered_out"),
	}, batchPolicy, maxInFlight, nil
}

type aerospikeWriter struct {
	conf        *aerospikeConfig
	conn        *Connection
	log         *service.Logger
	filteredOut *service.MetricCounter

	// operate, when set, replaces the live client's BatchOperate. Tests use
	// this to exercise WriteBatch without a cluster.
	operate func(*as.BatchPolicy, []as.BatchRecordIfc) error
}

func (w *aerospikeWriter) Connect(ctx context.Context) error {
	return w.conn.Connect(ctx)
}

func (w *aerospikeWriter) Close(ctx context.Context) error {
	return w.conn.Close(ctx)
}

func (w *aerospikeWriter) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	// A connection test should not warm a pool it is about to throw away.
	probe := *w.conf.client
	probe.WarmUp = false
	tmp := NewConnection(&probe, w.log)
	if err := tmp.Connect(ctx); err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}
	_ = tmp.Close(ctx)
	return service.ConnectionTestSucceeded().AsList()
}

func (w *aerospikeWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	operate := w.operate
	if operate == nil {
		client := w.conn.Client()
		if client == nil {
			if w.log != nil {
				w.log.Error("Not connected to Aerospike")
			}
			return service.ErrNotConnected
		}
		operate = func(p *as.BatchPolicy, recs []as.BatchRecordIfc) error {
			if err := client.BatchOperate(p, recs); err != nil {
				return err
			}
			return nil
		}
	}

	ops, failures := w.planBatch(batch)

	if len(ops) > 0 {
		records := make([]as.BatchRecordIfc, len(ops))
		for i, op := range ops {
			records[i] = w.buildRecord(op)
		}

		policy, err := BatchPolicyForContext(ctx, w.conf.batchPolicy)
		if err != nil {
			return err
		}

		batchErr := operate(policy, records)
		if batchErr != nil && IsConnectionError(batchErr) {
			if w.log != nil {
				w.log.Errorf("Aerospike batch command failed: %v", batchErr)
			}
			return service.ErrNotConnected
		}

		// Whether or not the call reported an error, every record carries its
		// own result code — a batch "failure" usually means some subset of keys
		// was rejected while the rest committed.
		for i, op := range ops {
			rec := records[i].BatchRec()
			if err := w.handleRecord(rec, op); err != nil {
				for _, idx := range op.indexes {
					failures[idx] = err
				}
			}
		}

		if batchErr != nil && len(failures) == 0 {
			// The command failed before any per-key code was set. Fail the
			// whole batch rather than silently acknowledging it.
			return fmt.Errorf("aerospike batch command failed: %w", batchErr)
		}
	}

	if len(failures) == 0 {
		return nil
	}

	err := service.NewBatchError(batch, fmt.Errorf(
		"%v of %v messages did not write to aerospike", len(failures), len(batch)))
	for idx, msgErr := range failures {
		err = err.Failed(idx, msgErr)
	}
	return err
}

// planBatch maps every message to a pending write and, unless disabled,
// coalesces repeats of the same record key.
//
// Coalescing matters more than it looks: a topic partition routinely delivers
// several messages for one key inside a single batch, and Aerospike limits
// concurrent commands per record. Sending them as separate batch entries
// contends on that one record — visible as KEY_BUSY, or as latency inflation
// with no error at all when it stays under the limit. Folding them into one
// write also removes any question of ordering within the batch.
func (w *aerospikeWriter) planBatch(batch service.MessageBatch) ([]*pendingOp, map[int]error) {
	failures := map[int]error{}
	ops := make([]*pendingOp, 0, len(batch))
	mapper := newBatchMapper(w.conf, batch)

	for i := range batch {
		op, err := mapper.mapMessage(i)
		if err != nil {
			failures[i] = err
			continue
		}
		ops = append(ops, op)
	}

	if !w.conf.coalesce {
		return ops, failures
	}
	return coalesceOps(ops), failures
}

func (w *aerospikeWriter) buildRecord(op *pendingOp) as.BatchRecordIfc {
	if op.kind == opDelete && !op.hasFence {
		if !op.hasGeneration {
			return as.NewBatchDelete(w.conf.deletePolicy, op.key)
		}
		policy := *w.conf.deletePolicy
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = op.generation
		return as.NewBatchDelete(&policy, op.key)
	}

	// Copy the shared policy so per-message expiration, fencing, and generation
	// checks do not race across concurrent batches.
	policy := *w.conf.writePolicy
	policy.RecordExistsAction = op.kind.recordExistsAction()
	policy.Expiration = op.ttl
	if op.kind == opDelete {
		// A fenced delete is a replace that leaves the fence on the key.
		policy.RecordExistsAction = as.REPLACE
		policy.Expiration = w.conf.tombstoneTTL
	}
	if op.hasFence {
		policy.FilterExpression = fenceExpression(w.conf.fenceBin, op.fence)
	}
	if op.hasGeneration {
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = op.generation
	}

	ops := make([]*as.Operation, 0, len(op.binOrder))
	for _, name := range op.binOrder {
		ops = append(ops, as.PutOp(as.NewBin(name, op.bins[name])))
	}

	return as.NewBatchWrite(&policy, op.key, ops...)
}

func filteredOutCount(rec *as.BatchRecord, op *pendingOp) int {
	if rec.ResultCode != types.FILTERED_OUT {
		return 0
	}
	return len(op.indexes)
}

func (w *aerospikeWriter) handleRecord(rec *as.BatchRecord, op *pendingOp) error {
	if n := filteredOutCount(rec, op); n > 0 {
		if w.filteredOut != nil {
			w.filteredOut.Incr(int64(n))
		}
		if w.log != nil {
			w.log.Debugf("aerospike fenced out %d message(s) for key %v", n, rec.Key)
		}
	}
	return classifyRecord(rec, op.kind)
}

// fenceExpression admits a write only when the record is new, or when the
// stored fence is strictly older than this message's. Evaluated on the server,
// so a redelivered or out-of-order message is discarded without a round trip
// and without a transaction.
func fenceExpression(bin string, fence int64) *as.Expression {
	return as.ExpOr(
		as.ExpNot(as.ExpBinExists(bin)),
		as.ExpLess(as.ExpIntBin(bin), as.ExpIntVal(fence)),
	)
}
