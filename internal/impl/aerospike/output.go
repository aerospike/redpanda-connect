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
	"fmt"
	"math"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldOperation            = "operation"
	fieldTombstoneAsDelete    = "tombstone_as_delete"
	fieldBins                 = "bins"
	fieldCoerceIntegralFloats = "coerce_integral_floats"
	fieldTTL                  = "ttl"
	fieldGeneration           = "generation"
	fieldSendKey              = "send_key"
	fieldDurableDelete        = "durable_delete"
	fieldCoalesceBatchKeys    = "coalesce_batch_keys"
	fieldCommitLevel          = "commit_level"
	fieldMaxRecordBytes       = "max_record_bytes"
	fieldFencing              = "fencing"
	fieldFencingEnabled       = "enabled"
	fieldFencingBin           = "bin"
	fieldFencingValue         = "value"
	fieldFencingTombstoneBin  = "tombstone_bin"
	fieldFencingTombstoneTTL  = "tombstone_ttl"
	fieldBatching             = "batching"

	// defaultBatchCount and defaultBatchPeriod are applied when no batching
	// policy is configured. An unbatched database sink round-trips per message,
	// which is the most common cause of a slow pipeline.
	defaultBatchCount  = 256
	defaultBatchPeriod = "1s"
)

func outputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Version("4.107.0").
		Categories("Services").
		Summary("Writes messages to an Aerospike cluster as records, batched and keyed by an interpolated primary key.").
		Description(`
Each message is mapped to a single Aerospike record. The record key is built from the
` + "`namespace`" + `, ` + "`set`" + ` and ` + "`key`" + ` fields, and the bins are produced by the
` + "`bins`" + ` Bloblang mapping.

### Shaping data

This output deliberately has no transform language of its own — use Bloblang, either in a
` + "`mapping`" + ` processor upstream or in the ` + "`bins`" + ` field. Aerospike bin names are
capped at 15 characters, so any source field with a longer name must be renamed in the
mapping rather than passed through.

### Record model

This output writes whole bins (` + "`PutOp`" + `). It will not append to a list, increment a
counter, or patch a map in place — those need a modeled operate in application code.
Treat ` + "`bins`" + ` as a schema contract the server will not enforce: keep bin names and types
stable, and cap nested lists and maps in Bloblang (` + "`this.events.slice(0, 100)`" + `). Aerospike
rewrites the entire record on every update, so the bulk of records should stay in
single-digit KiB. Sets are a name, not a shard key — do not interpolate an unbounded
value such as a Kafka topic. Primary-key access is the only read this component offers;
do not treat lookup as a join.

### Batching and hot keys

Writes are dispatched with a single batch command per output batch. Aerospike enforces
concurrency limits per record, so multiple messages for the same key inside one batch are
coalesced into one write by default (see ` + "`coalesce_batch_keys`" + `). Disable it only if
you need strict one-message-one-write semantics (including per-message
` + "`create_only`" + `/` + "`update_only`" + ` existence failures) and have confirmed keys do
not repeat.

Coalescing applies **within a single batch only**. With the default
` + "`max_in_flight`" + ` of 64, batches are dispatched concurrently, so two batches in flight
at the same time can still carry the same record key — which reorders those two writes with
respect to each other and can produce ` + "`KEY_BUSY`" + `. If a record key must never be
written concurrently, or writes to one key must land in stream order, set
` + "`max_in_flight: 1`" + `. That serialises the output, so prefer larger batches to recover
throughput.

### Delivery guarantees

Redpanda Connect delivers at least once, so a message may be written more than once after a
retry. Full-record writes are naturally idempotent. If your mapping is not — or if you want
redelivery of stale data to be rejected outright — enable ` + "`fencing`" + `, which stores a
monotonic value alongside the record and guards each write with a filter expression
evaluated on the server.

### What this output does not do

Writes are whole-bin puts. Atomic increments, list append, and map operations are not
available — shape the record in Bloblang and write the full bin values. There is no input;
Aerospike has no client-side change feed.
` + service.OutputPerformanceDocs(true, true))

	spec = spec.Fields(ClientFields()...)
	spec = spec.Fields(KeyFields(`${! json("user_id") }`, `${! meta("kafka_key") }`)...)

	spec = spec.Fields(
		service.NewInterpolatedStringField(fieldOperation).
			Description(`The write operation to perform. One of:

- `+"`write`"+` — create or update, merging these bins with any existing bins.
- `+"`replace`"+` — create or replace, deleting existing bins not named here.
- `+"`create_only`"+` — fail if the record already exists.
- `+"`update_only`"+` — fail if the record does not exist.
- `+"`delete`"+` — delete the record.

Interpolation allows routing per message, for example from a CDC operation field. For a compact topic or CDC stream that sends the full record each time, use `+"`replace`"+` so leftover bins cannot accumulate.`).
			Default("write").
			Example("replace").
			Example(`${! if json("op") == "d" { "delete" } else { "write" } }`),

		service.NewBoolField(fieldTombstoneAsDelete).
			Description("Treat a message with an empty or `null` payload as a delete, matching the Kafka tombstone convention. Applied before `operation` is evaluated. Disable this for HTTP, file, or other sources where an empty body is not a delete.").
			Default(true),

		service.NewBloblangField(fieldBins).
			Description("A Bloblang mapping producing an object whose keys are bin names and whose values are bin values. Bin names must be 1-15 characters. A `null` value deletes that bin from the record. Nested objects and arrays become Aerospike maps and lists. Bound collection size here — this output cannot append or trim on the server, and an unbounded list makes every subsequent write rewrite a larger record.").
			Default("root = this").
			Example(`root = this.without("user_id")`).
			Example(`root.name = this.full_name
root.email = this.email_address
root.updated = this.updated_at_epoch_seconds
root.events = this.events.slice(0, 50)`),

		service.NewBoolField(fieldCoerceIntegralFloats).
			Description("Store JSON numbers with no fractional part as Aerospike integers rather than doubles. JSON has a single number type, so without this an identifier or counter arrives as a double, which breaks integer comparisons, `add` operations and integer secondary indexes.\n\nThe trade-off is that the stored type then depends on the value: a `price` bin holding `10.0` is written as an integer while `10.5` is written as a double. Aerospike types bins per record rather than per set, so this raises no error, but an integer secondary index over such a bin only indexes the records that happened to be integral. Disable this for bins that are genuinely floating point, or coerce the type explicitly in the `bins` mapping.").
			Default(true).
			Advanced(),

		service.NewInterpolatedStringField(fieldTTL).
			Description(`Record time-to-live. Accepts a duration such as `+"`24h`"+`, or:

- `+"`0s`"+` — use the namespace `+"`default-ttl`"+`. Every write with this value re-bases void-time to that default.
- `+"`never`"+` — never expire.
- `+"`keep`"+` — leave the existing expiration untouched on update. On create, the namespace default applies.

The default is `+"`keep`"+` so a stream of updates does not reset or shorten void-time. Shortening TTL on an existing record can contribute to resurrection after a cold start. A positive TTL requires `+"`nsup-period`"+` greater than 0 on the target namespace, otherwise the server rejects the write and nothing ever expires.`).
			Default("keep").
			Example("24h").
			Example("never").
			Example("keep"),

		service.NewInterpolatedStringField(fieldGeneration).
			Description("Expected record generation for a compare-and-set write. When set, the write fails if the stored generation does not match. Empty disables the check. Pair with `aerospike_lookup` `emit_metadata`, which sets `aerospike_generation` on the looked-up message.\n\nA compare-and-set message is never coalesced, even with `coalesce_batch_keys` enabled: the check refers to the specific record that was read, and one command can carry only one expected generation. If several messages in a batch target the same key and any of them carries a generation, each is sent as its own command and the server arbitrates. On `GENERATION_ERROR` the correct recovery is to re-read the record and redo the change, not to retry the same write.").
			Default("").
			Example(`${! meta("aerospike_generation") }`).
			Advanced(),

		service.NewBoolField(fieldSendKey).
			Description("Store the primary key alongside the record, so reads can return it. Aerospike stores only a digest by default, meaning the original key is not recoverable from the record.").
			Default(false),

		service.NewBoolField(fieldDurableDelete).
			Description("Write a tombstone on delete so deleted records cannot reappear after a cold restart. Enterprise Edition only.").
			Default(false),

		service.NewBoolField(fieldCoalesceBatchKeys).
			Description("Collapse multiple messages for the same record key within a batch into a single write. Strongly recommended: sending them separately contends on one record, which surfaces as `KEY_BUSY` or as latency inflation with no error at all. Mixing `create_only` or `update_only` with `write`/`replace`/`delete` for the same key drops the existence check so a successful write is not nacked as `KEY_EXISTS`/`KEY_NOT_FOUND`.\n\nMessages carrying a `generation` are never coalesced, and coalescing only ever applies within one batch — see `max_in_flight` for what happens across concurrent batches.").
			Default(true).
			Advanced(),

		CommitLevelField(),

		service.NewIntField(fieldMaxRecordBytes).
			Description("Reject a mapped record whose bins exceed this many bytes (approximate). `0` disables the check. Aerospike rewrites the whole record on every update, so treat anything above about 50 KiB as a modeling decision to justify — set a budget here rather than waiting for `RECORD_TOO_BIG`.").
			Default(0).
			LintRule(NonNegativeLint).
			Advanced(),
	)
	spec = spec.Fields(BatchPolicyFieldsWithRetries(0)...)
	return spec.Fields(
		service.NewObjectField(fieldFencing,
			service.NewBoolField(fieldFencingEnabled).
				Description("Enable fencing.").
				Default(false),
			service.NewStringField(fieldFencingBin).
				Description("Bin holding the fence value. Counts toward the 15 character bin name limit. Must match `fence_bin` on `aerospike_lookup`, which strips it from the records it emits.").
				Default(DefaultFenceBin),
			service.NewInterpolatedStringField(fieldFencingValue).
				Description("An integer that increases monotonically for a given record key. The Kafka offset satisfies this when keys map to partitions consistently, which is the default partitioner's behaviour.").
				Default(`${! meta("kafka_offset") }`).
				Example(`${! meta("kafka_timestamp_ms") }`).
				Example(`${! json("version") }`),
			service.NewStringField(fieldFencingTombstoneBin).
				Description("Bin written on a fenced delete to mark the record as a tombstone. Lookups treat a record carrying this bin as missing. Counts toward the 15 character bin name limit.").
				Default(DefaultTombstoneBin),
			service.NewStringField(fieldFencingTombstoneTTL).
				Description(`Time-to-live for fenced tombstone records. Accepts a duration, or `+"`never`"+`. Defaults to `+"`never`"+` so the fence outlives the data TTL; if the tombstone expires, a stale replay can recreate the record.

Note that `+"`never`"+` means fenced deletes accumulate without bound: the records stay small, but Aerospike does not evict records that have no void-time, so a delete-heavy workload grows the namespace monotonically. Set a duration comfortably longer than the longest replay or redelivery window you need to defend against — beyond that point a replay is no longer a realistic risk — and budget the primary index at roughly 64 bytes per record per replica in the meantime.`).
				Default("never").
				Example("168h"),
		).Description(`Guards each write with a server-side filter expression so that a record is only
overwritten by a strictly newer message. Redelivered or out-of-order messages are discarded by the
server and acknowledged as successful, giving effectively-once writes without a transaction.
Those discards increment the `+"`aerospike_filtered_out`"+` counter and are logged at debug.

When fencing is enabled, deletes do not remove the record: they replace it with the fence plus a
tombstone bin, so a later replay cannot resurrect stale data. `+"`aerospike_lookup`"+` treats that
tombstone as not found. `+"`create_only`"+` after a fenced delete fails because the tombstone still exists.`).
			Advanced(),

		service.NewBatchPolicyField(fieldBatching).
			Description(fmt.Sprintf("Allows you to configure a batching policy. When this object is omitted, the output applies a default of %d messages or %s, whichever comes first, because an unbatched database sink round-trips per message. The field reference below shows `count: 0`, which is how the framework renders an unset policy — the default above is what actually runs, and the output logs it on startup. Set `count: 1` to write one message per command.", defaultBatchCount, defaultBatchPeriod)),

		service.NewOutputMaxInFlightField().
			Description("Maximum number of batches to have in flight concurrently. Note that key coalescing only deduplicates within a batch, so a value above `1` allows two concurrent batches to write the same record key, reordering them relative to each other. Set this to `1` when writes to a key must land in stream order."),
	).
		Example(
			"Stream a topic into Aerospike",
			"Consumes a Redpanda topic and writes each message as a record keyed by a JSON field, treating tombstones as deletes.",
			`
input:
  redpanda:
    seed_brokers: [ "localhost:19092" ]
    topics: [ "users" ]
    consumer_group: "aerospike-sink"

output:
  aerospike:
    hosts: [ "localhost:3000" ]
    namespace: test
    set: users
    key: '${! json("user_id") }'
    bins: 'root = this.without("user_id")'
    operation: replace
    ttl: keep
    batching:
      count: 500
      period: 1s
`,
		).
		Example(
			"Effectively-once with offset fencing",
			"Rejects redelivered or out-of-order messages on the server, so at-least-once delivery cannot regress a record to an older state.",
			`
output:
  aerospike:
    hosts: [ "localhost:3000" ]
    namespace: profiles
    set: user
    key: '${! meta("kafka_key") }'
    operation: replace
    bins: 'root = this'
    fencing:
      enabled: true
      bin: _off
      value: '${! meta("kafka_offset") }'
`,
		)
}

// aerospikeConfig is the parsed, validated form of the output configuration.
type aerospikeConfig struct {
	client *ClientConfig
	keys   *KeyConfig

	operation         *service.InterpolatedString
	staticOperation   opKind
	operationIsStatic bool
	tombstoneAsDelete bool

	bins       *bloblang.Executor
	coerceInts bool

	ttl         *service.InterpolatedString
	staticTTL   uint32
	ttlIsStatic bool

	generation *service.InterpolatedString

	coalesce bool

	maxRecordBytes int

	fenceEnabled bool
	fenceBin     string
	fenceValue   *service.InterpolatedString
	tombstoneBin string
	tombstoneTTL uint32

	batchPolicy  *as.BatchPolicy
	writePolicy  *as.BatchWritePolicy
	deletePolicy *as.BatchDeletePolicy
}

func parseOutputConfig(conf *service.ParsedConfig) (*aerospikeConfig, error) {
	c := &aerospikeConfig{}

	var err error
	if c.client, err = ParseClientConfig(conf); err != nil {
		return nil, err
	}
	if c.keys, err = ParseKeyConfig(conf); err != nil {
		return nil, err
	}

	if c.operation, err = conf.FieldInterpolatedString(fieldOperation); err != nil {
		return nil, err
	}
	// Resolve and validate a non-dynamic operation up front, so a typo fails at
	// startup rather than on the first message.
	if lit, ok := c.operation.Static(); ok {
		if c.staticOperation, err = parseOpKind(lit); err != nil {
			return nil, fmt.Errorf("field '%v': %w", fieldOperation, err)
		}
		c.operationIsStatic = true
	}
	if c.tombstoneAsDelete, err = conf.FieldBool(fieldTombstoneAsDelete); err != nil {
		return nil, err
	}

	if c.bins, err = conf.FieldBloblang(fieldBins); err != nil {
		return nil, err
	}
	if c.coerceInts, err = conf.FieldBool(fieldCoerceIntegralFloats); err != nil {
		return nil, err
	}

	if c.ttl, err = conf.FieldInterpolatedString(fieldTTL); err != nil {
		return nil, err
	}
	if lit, ok := c.ttl.Static(); ok {
		if c.staticTTL, err = parseTTL(lit); err != nil {
			return nil, fmt.Errorf("field '%v': %w", fieldTTL, err)
		}
		c.ttlIsStatic = true
	}

	if c.generation, err = conf.FieldInterpolatedString(fieldGeneration); err != nil {
		return nil, err
	}

	if c.coalesce, err = conf.FieldBool(fieldCoalesceBatchKeys); err != nil {
		return nil, err
	}
	if c.maxRecordBytes, err = conf.FieldInt(fieldMaxRecordBytes); err != nil {
		return nil, err
	}
	if c.maxRecordBytes < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", fieldMaxRecordBytes)
	}

	if conf.Contains(fieldFencing) {
		fc := conf.Namespace(fieldFencing)
		if c.fenceEnabled, err = fc.FieldBool(fieldFencingEnabled); err != nil {
			return nil, err
		}
		if c.fenceBin, err = fc.FieldString(fieldFencingBin); err != nil {
			return nil, err
		}
		if c.fenceValue, err = fc.FieldInterpolatedString(fieldFencingValue); err != nil {
			return nil, err
		}
		if c.tombstoneBin, err = fc.FieldString(fieldFencingTombstoneBin); err != nil {
			return nil, err
		}
		ttlStr, err := fc.FieldString(fieldFencingTombstoneTTL)
		if err != nil {
			return nil, err
		}
		if c.tombstoneTTL, err = parseTTL(ttlStr); err != nil {
			return nil, fmt.Errorf("field '%v.%v': %w", fieldFencing, fieldFencingTombstoneTTL, err)
		}
	}
	if c.fenceEnabled {
		if err := ValidateBinName(c.fenceBin); err != nil {
			return nil, fmt.Errorf("field '%v.%v': %w", fieldFencing, fieldFencingBin, err)
		}
		if err := ValidateBinName(c.tombstoneBin); err != nil {
			return nil, fmt.Errorf("field '%v.%v': %w", fieldFencing, fieldFencingTombstoneBin, err)
		}
		if c.tombstoneBin == c.fenceBin {
			return nil, fmt.Errorf("field '%v.%v' must not be the same as '%v.%v'", fieldFencing, fieldFencingTombstoneBin, fieldFencing, fieldFencingBin)
		}
	}

	sendKey, err := conf.FieldBool(fieldSendKey)
	if err != nil {
		return nil, err
	}
	durableDelete, err := conf.FieldBool(fieldDurableDelete)
	if err != nil {
		return nil, err
	}

	if c.batchPolicy, err = ParseBatchPolicy(conf); err != nil {
		return nil, err
	}

	c.writePolicy = as.NewBatchWritePolicy()
	c.writePolicy.SendKey = sendKey
	c.writePolicy.DurableDelete = durableDelete

	c.deletePolicy = as.NewBatchDeletePolicy()
	c.deletePolicy.SendKey = sendKey
	c.deletePolicy.DurableDelete = durableDelete

	commitStr, err := conf.FieldString(fieldCommitLevel)
	if err != nil {
		return nil, err
	}
	commit, err := ParseCommitLevel(commitStr)
	if err != nil {
		return nil, fmt.Errorf("field '%v': %w", fieldCommitLevel, err)
	}
	c.writePolicy.CommitLevel = commit
	c.deletePolicy.CommitLevel = commit

	return c, nil
}

// parseTTL converts a configured TTL into the server's expiration encoding.
func parseTTL(s string) (uint32, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "0", "0s", "default":
		return as.TTLServerDefault, nil
	case "never", "-1":
		return as.TTLDontExpire, nil
	case "keep", "-2":
		return as.TTLDontUpdate, nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid ttl %q: expected a duration, 'never' or 'keep': %w", s, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid ttl %q: must not be negative", s)
	}
	secs := int64(d / time.Second)
	if d > 0 && secs == 0 {
		// A sub-second TTL would round to "use namespace default", which is the
		// opposite of what was asked for.
		return 0, fmt.Errorf("invalid ttl %q: the minimum resolution is one second", s)
	}
	if secs >= math.MaxUint32-1 {
		return 0, fmt.Errorf("invalid ttl %q: exceeds the maximum expiration", s)
	}
	return uint32(secs), nil
}

// formatTTL renders a record's expiration in the form parseTTL accepts, so a
// TTL read by aerospike_lookup can be fed straight back into the output's ttl
// field. The client reports a record that never expires as a sentinel rather
// than a duration, which would otherwise surface to the user as "4294967295".
func formatTTL(expiration uint32) string {
	if expiration == as.TTLDontExpire {
		return "never"
	}
	return (time.Duration(expiration) * time.Second).String()
}
