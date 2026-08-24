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
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const (
	aerospikeImage         = "aerospike/aerospike-server:8.1"
	integrationNamespace   = "test"
	integrationOutputSet   = "rpa_e2e"
	integrationLookupSet   = "rpa_lookup"
	aerospikeContainerPort = "3000/tcp"
)

var (
	startOnce       sync.Once
	integrationAddr string
	startErr        error
)

func integrationHost(t *testing.T) string {
	t.Helper()
	integration.CheckSkip(t)
	startOnce.Do(func() {
		integrationAddr, startErr = startAerospike()
	})
	require.NoError(t, startErr)
	return integrationAddr
}

// startAerospike launches a single-node community server with nsup-period set
// so TTL writes are accepted. The client is pointed at the container IP, not a
// mapped localhost port: Aerospike advertises that IP to the client after the
// seed handshake, and a host-mapped port would then talk to an unreachable
// address.
func startAerospike() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not locate testdata/aerospike.conf")
	}
	cfgPath := filepath.Join(filepath.Dir(thisFile), "testdata", "aerospike.conf")

	// Not t.Context(): the container is shared and outlives any one test.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := testcontainers.Run(ctx, aerospikeImage,
		testcontainers.WithExposedPorts(aerospikeContainerPort),
		testcontainers.WithCmd("--config-file", "/opt/aerospike/etc/aerospike.conf"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			HostFilePath:      cfgPath,
			ContainerFilePath: "/opt/aerospike/etc/aerospike.conf",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("soon there will be cake").WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		return "", err
	}

	ip, err := ctr.ContainerIP(ctx)
	if err != nil {
		return "", err
	}
	addr := net.JoinHostPort(ip, "3000")

	if err := waitForClient(ctx, addr); err != nil {
		return "", err
	}
	return addr, nil
}

func waitForClient(ctx context.Context, addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		client, err := as.NewClient(host, port)
		if err == nil {
			client.Close()
			return nil
		}
		last = err
		time.Sleep(time.Second)
	}
	return fmt.Errorf("aerospike never accepted a client at %s: %w", addr, last)
}

func outputSetup(t *testing.T, extraYAML string) (*aerospikeWriter, *as.Client) {
	t.Helper()
	hosts := integrationHost(t)

	yaml := `
hosts: [ "` + hosts + `" ]
namespace: ` + integrationNamespace + `
set: ` + integrationOutputSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
` + extraYAML

	w := newTestWriter(t, yaml)
	require.NoError(t, w.Connect(t.Context()))
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	client, err := as.NewClientWithPolicyAndHost(w.conf.client.Policy, w.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Truncate(nil, integrationNamespace, integrationOutputSet, nil))
	return w, client
}

func outputRead(t *testing.T, client *as.Client, id string) *as.Record {
	t.Helper()
	key, err := as.NewKey(integrationNamespace, integrationOutputSet, id)
	require.NoError(t, err)
	rec, asErr := client.Get(nil, key)
	if asErr != nil && asErr.Matches(2 /* KEY_NOT_FOUND_ERROR */) {
		return nil
	}
	require.NoError(t, asErr)
	return rec
}

func msg(t *testing.T, body string) *service.Message {
	t.Helper()
	return service.NewMessage([]byte(body))
}

func lookupSetup(t *testing.T, extraYAML string) (*lookupProcessor, *as.Client) {
	t.Helper()
	hosts := integrationHost(t)

	yaml := `
hosts: [ "` + hosts + `" ]
namespace: ` + integrationNamespace + `
set: ` + integrationLookupSet + `
key: '${! json("user_id") }'
` + extraYAML

	p := newTestProcessor(t, yaml)
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	client, err := as.NewClientWithPolicyAndHost(p.conf.client.Policy, p.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Truncate(nil, integrationNamespace, integrationLookupSet, nil))
	return p, client
}

func seed(t *testing.T, client *as.Client, id string, bins as.BinMap) {
	t.Helper()
	key, err := as.NewKey(integrationNamespace, integrationLookupSet, id)
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, key, bins))
}

func structured(t *testing.T, m *service.Message) any {
	t.Helper()
	v, err := m.AsStructured()
	require.NoError(t, err)
	return v
}

func TestIntegrationWriteAndRead(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"a1","name":"Ada","score":10,"ratio":0.5,"tags":["x","y"]}`),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	rec := outputRead(t, client, "a1")
	require.NotNil(t, rec)
	assert.Equal(t, "Ada", rec.Bins["name"])
	// An integral JSON number must land as an Aerospike integer, not a double.
	assert.Equal(t, 10, rec.Bins["score"])
	assert.Equal(t, 0.5, rec.Bins["ratio"])
	assert.Equal(t, []any{"x", "y"}, rec.Bins["tags"])
}

// TestIntegrationCoalescing proves the merge rules against a real server: three
// messages for one key inside one batch produce one record with the merged bins
// and the last writer winning, rather than three contending commands.
func TestIntegrationCoalescing(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"c1","a":1,"shared":"first"}`),
		msg(t, `{"id":"c2","a":1}`),
		msg(t, `{"id":"c1","b":2}`),
		msg(t, `{"id":"c1","shared":"last"}`),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	rec := outputRead(t, client, "c1")
	require.NotNil(t, rec)
	assert.Equal(t, 1, rec.Bins["a"])
	assert.Equal(t, 2, rec.Bins["b"])
	assert.Equal(t, "last", rec.Bins["shared"])

	assert.NotNil(t, outputRead(t, client, "c2"))
}

func TestIntegrationTombstoneDeletes(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
`)

	create := msg(t, `{"v":1}`)
	create.MetaSet("k", "d1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, outputRead(t, client, "d1"))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "d1")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))
	assert.Nil(t, outputRead(t, client, "d1"))

	// Deleting an already absent record must stay a success, otherwise a
	// redelivered tombstone would nack forever.
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))
}

func TestIntegrationFencing(t *testing.T) {
	w, client := outputSetup(t, `
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	newer := msg(t, `{"id":"f1","v":"second"}`)
	newer.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newer}))
	assert.Equal(t, "second", outputRead(t, client, "f1").Bins["v"])

	stale := msg(t, `{"id":"f1","v":"first"}`)
	stale.MetaSet("off", "3")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{stale}))

	rec := outputRead(t, client, "f1")
	assert.Equal(t, "second", rec.Bins["v"], "a stale replay must not overwrite newer data")
	assert.Equal(t, 10, rec.Bins["_off"])

	newest := msg(t, `{"id":"f1","v":"third"}`)
	newest.MetaSet("off", "11")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newest}))
	assert.Equal(t, "third", outputRead(t, client, "f1").Bins["v"])
}

func TestIntegrationFencingDeleteThenStaleWrite(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("k") }'
bins: 'root = this'
fencing:
  enabled: true
  bin: _off
  value: '${! meta("off") }'
`)

	create := msg(t, `{"v":"live"}`)
	create.MetaSet("k", "fd1")
	create.MetaSet("off", "10")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, outputRead(t, client, "fd1"))

	tombstone := msg(t, ``)
	tombstone.MetaSet("k", "fd1")
	tombstone.MetaSet("off", "11")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{tombstone}))

	rec := outputRead(t, client, "fd1")
	require.NotNil(t, rec, "a fenced delete must leave a tombstone record")
	assert.Equal(t, true, rec.Bins["_deleted"])
	assert.Equal(t, 11, rec.Bins["_off"])

	stale := msg(t, `{"v":"resurrect"}`)
	stale.MetaSet("k", "fd1")
	stale.MetaSet("off", "3")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{stale}))

	rec = outputRead(t, client, "fd1")
	require.NotNil(t, rec)
	assert.Equal(t, true, rec.Bins["_deleted"], "a stale write must not resurrect a fenced delete")
	assert.NotContains(t, rec.Bins, "v")

	newer := msg(t, `{"v":"again"}`)
	newer.MetaSet("k", "fd1")
	newer.MetaSet("off", "12")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{newer}))
	rec = outputRead(t, client, "fd1")
	require.NotNil(t, rec)
	assert.Equal(t, "again", rec.Bins["v"])
	assert.NotContains(t, rec.Bins, "_deleted")
}

func TestIntegrationTTL(t *testing.T) {
	w, client := outputSetup(t, "ttl: 1h\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"t1","v":1}`)}))

	rec := outputRead(t, client, "t1")
	require.NotNil(t, rec)
	// Requires nsup-period > 0 on the namespace; with NSUP disabled the write
	// would have been rejected outright.
	assert.InDelta(t, 3600, rec.Expiration, 60)
}

func TestIntegrationReplaceClearsOldBins(t *testing.T) {
	w, client := outputSetup(t, "operation: replace\n")

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"r1","a":1,"b":2}`)}))

	require.NoError(t, w.WriteBatch(t.Context(),
		service.MessageBatch{msg(t, `{"id":"r1","a":9}`)}))

	rec := outputRead(t, client, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, 9, rec.Bins["a"])
	assert.NotContains(t, rec.Bins, "b", "replace must drop bins not named in the write")
}

func TestIntegrationPartialFailure(t *testing.T) {
	w, client := outputSetup(t, "")

	batch := service.MessageBatch{
		msg(t, `{"id":"p1","ok":1}`),
		msg(t, `{"id":"p2","this_bin_name_is_much_too_long":1}`),
		msg(t, `{"id":"p3","ok":1}`),
	}

	err := w.WriteBatch(t.Context(), batch)
	require.Error(t, err)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors(), "only the bad message should be failed")
	assert.Contains(t, err.Error(), "1 of 3")

	assert.NotNil(t, outputRead(t, client, "p1"))
	assert.NotNil(t, outputRead(t, client, "p3"))
}

func TestIntegrationLookupEnriches(t *testing.T) {
	p, client := lookupSetup(t, "")

	seed(t, client, "u1", as.BinMap{"tier": "gold", "ltv": 4200, "country": "GB"})

	batch := service.MessageBatch{service.NewMessage([]byte(`{"user_id":"u1","evt":"click"}`))}
	out, err := p.ProcessBatch(t.Context(), batch)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 1)

	assert.Equal(t, map[string]any{
		"tier": "gold", "ltv": 4200, "country": "GB",
	}, structured(t, out[0][0]))

	assert.Equal(t, map[string]any{"user_id": "u1", "evt": "click"}, structured(t, batch[0]))
}

// Fencing is invisible to the read side: a record written with a fence must
// enrich a message with its data and nothing else.
func TestIntegrationLookupHidesFencingBins(t *testing.T) {
	p, client := lookupSetup(t, "fence_bin: _off\n")

	seed(t, client, "fu1", as.BinMap{"tier": "gold", "_off": 42})

	out, err := p.ProcessBatch(t.Context(),
		service.MessageBatch{service.NewMessage([]byte(`{"user_id":"fu1"}`))})
	require.NoError(t, err)
	require.Len(t, out, 1)

	assert.Equal(t, map[string]any{"tier": "gold"}, structured(t, out[0][0]))
}

func TestIntegrationSelectedBinsOnly(t *testing.T) {
	p, client := lookupSetup(t, "bins: [ tier ]\n")

	seed(t, client, "u1", as.BinMap{"tier": "gold", "ltv": 4200})

	out, err := p.ProcessBatch(t.Context(),
		service.MessageBatch{service.NewMessage([]byte(`{"user_id":"u1"}`))})
	require.NoError(t, err)

	assert.Equal(t, map[string]any{"tier": "gold"}, structured(t, out[0][0]))
}

func TestIntegrationDeduplicatedFanOut(t *testing.T) {
	p, client := lookupSetup(t, "")

	seed(t, client, "u1", as.BinMap{"tier": "gold"})
	seed(t, client, "u2", as.BinMap{"tier": "silver"})

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"user_id":"u1","evt":1}`)),
		service.NewMessage([]byte(`{"user_id":"u2","evt":2}`)),
		service.NewMessage([]byte(`{"user_id":"u1","evt":3}`)),
	}

	out, err := p.ProcessBatch(t.Context(), batch)
	require.NoError(t, err)
	require.Len(t, out[0], 3)

	assert.Equal(t, map[string]any{"tier": "gold"}, structured(t, out[0][0]))
	assert.Equal(t, map[string]any{"tier": "silver"}, structured(t, out[0][1]))
	assert.Equal(t, map[string]any{"tier": "gold"}, structured(t, out[0][2]))
}

func TestIntegrationNestedCDTsBecomeJSON(t *testing.T) {
	p, client := lookupSetup(t, "")

	seed(t, client, "u1", as.BinMap{
		"prefs": map[string]any{"theme": "dark", "n": 3},
		"tags":  []any{"a", "b"},
	})

	out, err := p.ProcessBatch(t.Context(),
		service.MessageBatch{service.NewMessage([]byte(`{"user_id":"u1"}`))})
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"prefs": map[string]any{"theme": "dark", "n": 3},
		"tags":  []any{"a", "b"},
	}, structured(t, out[0][0]))

	_, err = out[0][0].AsBytes()
	require.NoError(t, err)
}

func TestIntegrationNotFoundNull(t *testing.T) {
	p, _ := lookupSetup(t, "")

	out, err := p.ProcessBatch(t.Context(),
		service.MessageBatch{service.NewMessage([]byte(`{"user_id":"missing"}`))})
	require.NoError(t, err)
	require.Len(t, out[0], 1)

	assert.Nil(t, structured(t, out[0][0]))
	assert.NoError(t, out[0][0].GetError())
}

func TestIntegrationNotFoundDrop(t *testing.T) {
	p, client := lookupSetup(t, "not_found: drop\n")

	seed(t, client, "u1", as.BinMap{"tier": "gold"})

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"user_id":"u1"}`)),
		service.NewMessage([]byte(`{"user_id":"missing"}`)),
	}

	out, err := p.ProcessBatch(t.Context(), batch)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0], 1, "the unmatched message should be dropped")
	assert.Equal(t, map[string]any{"tier": "gold"}, structured(t, out[0][0]))
}

func TestIntegrationNotFoundError(t *testing.T) {
	p, _ := lookupSetup(t, "not_found: error\n")

	out, err := p.ProcessBatch(t.Context(),
		service.MessageBatch{service.NewMessage([]byte(`{"user_id":"missing"}`))})
	require.NoError(t, err)

	require.Error(t, out[0][0].GetError())
}

func TestIntegrationEmitsMetadata(t *testing.T) {
	p, client := lookupSetup(t, "emit_metadata: true\n")

	key, err := as.NewKey(integrationNamespace, integrationLookupSet, "u1")
	require.NoError(t, err)
	wp := as.NewWritePolicy(0, 3600)
	require.NoError(t, client.Put(wp, key, as.BinMap{"tier": "gold"}))

	out, procErr := p.ProcessBatch(t.Context(),
		service.MessageBatch{service.NewMessage([]byte(`{"user_id":"u1"}`))})
	require.NoError(t, procErr)

	gen, ok := out[0][0].MetaGet(metaGeneration)
	require.True(t, ok)
	assert.Equal(t, "1", gen)

	// The metadata is documented as feeding the output's generation and ttl
	// fields, so both have to come back in a form those fields accept.
	ttl, ok := out[0][0].MetaGet(metaTTL)
	require.True(t, ok)
	parsed, ttlErr := parseTTL(ttl)
	require.NoError(t, ttlErr)
	assert.InDelta(t, 3600, parsed, 60)
}

func TestIntegrationKeyFailureIsolated(t *testing.T) {
	p, client := lookupSetup(t, "")

	seed(t, client, "u1", as.BinMap{"tier": "gold"})

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"user_id":"u1"}`)),
		service.NewMessage([]byte(`{"nope":true}`)),
	}

	out, err := p.ProcessBatch(t.Context(), batch)
	require.NoError(t, err)
	require.Len(t, out[0], 2)

	assert.Equal(t, map[string]any{"tier": "gold"}, structured(t, out[0][0]))
	require.Error(t, out[0][1].GetError())
	assert.Contains(t, out[0][1].GetError().Error(), "not a usable record key")
}
