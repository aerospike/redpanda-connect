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
	"fmt"
	"slices"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type opKind int

const (
	opWrite opKind = iota
	opReplace
	opCreateOnly
	opUpdateOnly
	opDelete
)

func (o opKind) String() string {
	switch o {
	case opWrite:
		return "write"
	case opReplace:
		return "replace"
	case opCreateOnly:
		return "create_only"
	case opUpdateOnly:
		return "update_only"
	case opDelete:
		return "delete"
	}
	return "unknown"
}

func (o opKind) recordExistsAction() as.RecordExistsAction {
	switch o {
	case opReplace:
		return as.REPLACE
	case opCreateOnly:
		return as.CREATE_ONLY
	case opUpdateOnly:
		return as.UPDATE_ONLY
	default:
		return as.UPDATE
	}
}

func parseOpKind(s string) (opKind, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "write", "update", "":
		return opWrite, nil
	case "replace":
		return opReplace, nil
	case "create_only", "create":
		return opCreateOnly, nil
	case "update_only":
		return opUpdateOnly, nil
	case "delete":
		return opDelete, nil
	}
	return 0, fmt.Errorf("unknown operation %q: expected one of write, replace, create_only, update_only, delete", s)
}

// pendingOp is one record write, possibly folded from several source messages.
type pendingOp struct {
	key *as.Key
	// indexes lists every source batch index folded into this op, so a failure
	// can be reported against all of them.
	indexes []int

	kind opKind

	// bins is the accumulated bin set. binOrder preserves insertion order so
	// generated operations are deterministic, which keeps tests meaningful.
	bins     map[string]any
	binOrder []string

	ttl uint32

	generation    uint32
	hasGeneration bool

	fence    int64
	hasFence bool
}

func newPendingOp(index int, key *as.Key, kind opKind, ttl uint32) *pendingOp {
	return &pendingOp{
		key:     key,
		indexes: []int{index},
		kind:    kind,
		bins:    map[string]any{},
		ttl:     ttl,
	}
}

func (p *pendingOp) setBin(name string, value any) {
	if _, exists := p.bins[name]; !exists {
		p.binOrder = append(p.binOrder, name)
	}
	p.bins[name] = value
}

func (p *pendingOp) resetBins() {
	p.bins = map[string]any{}
	p.binOrder = nil
}

func (p *pendingOp) copyBinsFrom(next *pendingOp) {
	for _, name := range next.binOrder {
		p.setBin(name, next.bins[name])
	}
}

func (p *pendingOp) take(next *pendingOp) {
	p.kind = next.kind
	p.resetBins()
	p.copyBinsFrom(next)
}

// fold merges a later operation for the same key into this one, so that a batch
// containing several messages for one record issues a single write.
//
// The merge rules preserve the outcome of applying the operations in sequence
// when a single Aerospike command can do that. Existence-conditional ops
// (create_only / update_only) cannot always keep their existence check without
// nacking a preceding write that would have succeeded, so the check is dropped
// in favour of the sequential final record:
//
//   - delete and replace each discard whatever was accumulated, because they
//     define the record's full state.
//   - write merges its bins over the accumulation, and inherits the accumulated
//     kind — so [replace{a}, write{b}] stays a replace carrying {a,b}.
//   - write after delete is promoted to replace, because the intervening delete
//     means bins already on the server must not survive.
//   - update_only after write/replace merges bins and keeps the write/replace,
//     so [write{a}, update_only{b}] is not folded into a lone update_only that
//     would KEY_NOT_FOUND and nack both messages.
//   - write after create_only/update_only is promoted to write, so the write
//     is not nacked by the preceding existence check.
//   - create_only after write/replace is dropped: it cannot succeed against
//     the record those ops leave, and folding into create_only would nack the
//     write as KEY_EXISTS.
//
// When both sides are fenced, a lower-fence payload is ignored — the server
// would FILTERED_OUT it. A higher-fence payload is merged with the rules above
// and carries that fence, so [write{a}@3, write{b}@10] stays write{a,b}@10
// rather than replacing the payload with only {b}.
func (p *pendingOp) fold(next *pendingOp) {
	p.indexes = append(p.indexes, next.indexes...)

	if p.hasFence && next.hasFence && next.fence < p.fence {
		return
	}

	p.ttl = next.ttl
	p.generation, p.hasGeneration = next.generation, next.hasGeneration
	if next.hasFence {
		p.fence, p.hasFence = next.fence, true
	}

	switch next.kind {
	case opDelete, opReplace:
		p.take(next)
	case opCreateOnly:
		switch p.kind {
		case opWrite, opReplace:
			// create_only cannot succeed against the record those ops leave.
			// Absorb it so the preceding write is not nacked as KEY_EXISTS.
		case opUpdateOnly:
			// One command cannot keep both existence checks. Promote to write
			// so neither message is poisoned by the other's constraint.
			p.kind = opWrite
			p.copyBinsFrom(next)
		case opDelete:
			// Sequential delete then create is a full-record rewrite.
			p.kind = opReplace
			p.resetBins()
			p.copyBinsFrom(next)
		default:
			p.take(next)
		}
	case opUpdateOnly:
		switch p.kind {
		case opDelete:
			// Sequential update_only fails after delete. Keep the delete so a
			// successful delete is not nacked as KEY_NOT_FOUND.
		case opCreateOnly:
			// One command cannot keep both existence checks. Promote to write
			// so neither message is poisoned by the other's constraint.
			p.kind = opWrite
			p.copyBinsFrom(next)
		default:
			p.copyBinsFrom(next)
		}
	case opWrite:
		if p.kind == opDelete {
			p.kind = opReplace
			p.resetBins()
		} else if p.kind == opCreateOnly || p.kind == opUpdateOnly {
			p.kind = opWrite
		}
		p.copyBinsFrom(next)
	}
}

// batchMapper maps one batch of messages into pending writes.
//
// Every interpolation and mapping is bound to the batch rather than to
// individual messages, so batch-aware Bloblang — `batch_index()`, windowed
// queries — behaves as it does elsewhere in Redpanda Connect.
type batchMapper struct {
	conf  *aerospikeConfig
	batch service.MessageBatch

	keys       *KeyResolver
	bins       *service.MessageBatchBloblangExecutor
	op         *service.MessageBatchInterpolationExecutor
	ttl        *service.MessageBatchInterpolationExecutor
	generation *service.MessageBatchInterpolationExecutor
	fence      *service.MessageBatchInterpolationExecutor
}

func newBatchMapper(conf *aerospikeConfig, batch service.MessageBatch) *batchMapper {
	m := &batchMapper{
		conf:  conf,
		batch: batch,
		keys:  conf.keys.Resolver(batch),
		bins:  batch.BloblangExecutor(conf.bins),
	}
	if !conf.operationIsStatic {
		m.op = batch.InterpolationExecutor(conf.operation)
	}
	if !conf.ttlIsStatic {
		m.ttl = batch.InterpolationExecutor(conf.ttl)
	}
	if conf.fenceEnabled {
		m.fence = batch.InterpolationExecutor(conf.fenceValue)
	}
	if lit, ok := conf.generation.Static(); !ok || strings.TrimSpace(lit) != "" {
		m.generation = batch.InterpolationExecutor(conf.generation)
	}
	return m
}

// mapMessage converts the message at the given batch index into a pending write.
func (m *batchMapper) mapMessage(index int) (*pendingOp, error) {
	key, err := m.keys.Key(index)
	if err != nil {
		return nil, err
	}

	kind, err := m.resolveOperation(index)
	if err != nil {
		return nil, err
	}

	ttl, err := m.resolveTTL(index)
	if err != nil {
		return nil, err
	}

	op := newPendingOp(index, key, kind, ttl)
	if gen, ok, err := m.resolveGeneration(index); err != nil {
		return nil, err
	} else if ok {
		op.generation, op.hasGeneration = gen, true
	}
	if m.conf.fenceEnabled {
		fence, err := m.resolveFence(index)
		if err != nil {
			return nil, err
		}
		op.fence, op.hasFence = fence, true
		op.setBin(m.conf.fenceBin, fence)
		if kind == opDelete {
			op.setBin(m.conf.tombstoneBin, true)
			return op, nil
		}
	} else if kind == opDelete {
		return op, nil
	}

	if err := m.applyBins(op, index); err != nil {
		return nil, err
	}

	if m.conf.fenceEnabled && kind != opReplace && kind != opCreateOnly {
		// UPDATE merges; a prior fenced delete left a tombstone bin that must
		// be removed or lookup will still treat the record as missing.
		op.setBin(m.conf.tombstoneBin, nil)
	}

	if len(op.binOrder) == 0 {
		return nil, fmt.Errorf("mapping in field '%v' produced no bins; a record must have at least one bin", fieldBins)
	}

	return op, nil
}

func (m *batchMapper) resolveOperation(index int) (opKind, error) {
	if m.conf.tombstoneAsDelete && isTombstone(m.batch[index]) {
		return opDelete, nil
	}
	if m.conf.operationIsStatic {
		return m.conf.staticOperation, nil
	}
	opStr, err := m.op.TryString(index)
	if err != nil {
		return 0, fmt.Errorf("interpolating '%v': %w", fieldOperation, err)
	}
	kind, err := parseOpKind(opStr)
	if err != nil {
		return 0, fmt.Errorf("field '%v': %w", fieldOperation, err)
	}
	return kind, nil
}

func (m *batchMapper) resolveTTL(index int) (uint32, error) {
	if m.conf.ttlIsStatic {
		return m.conf.staticTTL, nil
	}
	ttlStr, err := m.ttl.TryString(index)
	if err != nil {
		return 0, fmt.Errorf("interpolating '%v': %w", fieldTTL, err)
	}
	ttl, err := parseTTL(ttlStr)
	if err != nil {
		return 0, fmt.Errorf("field '%v': %w", fieldTTL, err)
	}
	return ttl, nil
}

func (m *batchMapper) resolveGeneration(index int) (uint32, bool, error) {
	if m.generation == nil {
		return 0, false, nil
	}
	raw, err := m.generation.TryString(index)
	if err != nil {
		return 0, false, fmt.Errorf("interpolating '%v': %w", fieldGeneration, err)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return 0, false, nil
	}
	v, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("field '%v' resolved to %q, which is not an unsigned integer: %w", fieldGeneration, raw, err)
	}
	return uint32(v), true, nil
}

func (m *batchMapper) resolveFence(index int) (int64, error) {
	raw, err := m.fence.TryString(index)
	if err != nil {
		return 0, fmt.Errorf("interpolating '%v.%v': %w", fieldFencing, fieldFencingValue, err)
	}
	fence, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field '%v.%v' resolved to %q, which is not an integer: %w", fieldFencing, fieldFencingValue, raw, err)
	}
	return fence, nil
}

// applyBins runs the bins mapping and converts the resulting object into bins.
func (m *batchMapper) applyBins(op *pendingOp, index int) error {
	mapped, err := m.bins.Query(index)
	if err != nil {
		return fmt.Errorf("executing mapping in field '%v': %w", fieldBins, err)
	}
	if mapped == nil {
		// The mapping deleted the root. Dropping the message here would look
		// like a successful write, so make the user filter upstream instead.
		return fmt.Errorf("mapping in field '%v' deleted the root; filter unwanted messages with a processor before this output", fieldBins)
	}

	res, err := mapped.AsStructured()
	if err != nil {
		return fmt.Errorf("mapping in field '%v' did not produce structured data: %w", fieldBins, err)
	}

	obj, ok := res.(map[string]any)
	if !ok {
		return fmt.Errorf("mapping in field '%v' must produce an object, got %T", fieldBins, res)
	}

	// Sort for deterministic bin ordering; Go map iteration is randomised and
	// unstable ordering makes failures hard to reproduce.
	names := make([]string, 0, len(obj))
	for name := range obj {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		if err := ValidateBinName(name); err != nil {
			return fmt.Errorf("%w; rename it in the '%v' mapping", err, fieldBins)
		}
		value, err := ToAerospike(obj[name], m.conf.coerceInts)
		if err != nil {
			return fmt.Errorf("bin %q: %w", name, err)
		}
		op.setBin(name, value)
	}

	if m.conf.maxRecordBytes > 0 {
		size := 0
		for _, name := range op.binOrder {
			size += len(name) + EstimateSize(op.bins[name])
		}
		if size > m.conf.maxRecordBytes {
			return fmt.Errorf("mapped record is about %d bytes, which exceeds max_record_bytes (%d); Aerospike rewrites the whole record on every update, so cap collections in the '%v' mapping or split the data across keys", size, m.conf.maxRecordBytes, fieldBins)
		}
	}
	return nil
}

// isTombstone reports whether a message carries no payload, which is the Kafka
// convention for "this key is deleted".
func isTombstone(msg *service.Message) bool {
	if v, err := msg.AsStructured(); err == nil {
		if v == nil {
			return true
		}
		if s, ok := v.(string); ok {
			trimmed := strings.TrimSpace(s)
			return trimmed == "" || trimmed == "null"
		}
		return false
	}
	b, err := msg.AsBytes()
	if err != nil {
		return false
	}
	trimmed := strings.TrimSpace(string(b))
	return trimmed == "" || trimmed == "null"
}
