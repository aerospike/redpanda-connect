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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// These exercise the behaviour a single-node Community server cannot reach:
// durable deletes, replica placement, strong consistency, access control and
// TLS. They need an Enterprise cluster with replication-factor 2, an AP
// namespace `test` and a strong-consistency namespace `sc`, addressed by
// AEROSPIKE_EE_HOSTS.
const (
	eeAPNamespace = "test"
	eeSCNamespace = "sc"
	eeSet         = "rpa_ee"
)

func eeHosts(t *testing.T) string {
	t.Helper()
	hosts := os.Getenv("AEROSPIKE_EE_HOSTS")
	if hosts == "" {
		t.Skip("AEROSPIKE_EE_HOSTS is unset; Enterprise tests need a licensed multi-node cluster")
	}
	return hosts
}

// eeWriter builds an output against the Enterprise cluster. The caller supplies
// the namespace so strong-consistency tests can target `sc`.
func eeWriter(t *testing.T, namespace, extraYAML string) (*aerospikeWriter, *as.Client) {
	t.Helper()

	yaml := `
hosts: [ ` + quoteHosts(eeHosts(t)) + ` ]
namespace: ` + namespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
` + extraYAML

	w := newTestWriter(t, yaml)
	require.NoError(t, w.Connect(t.Context()))
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	client, err := as.NewClientWithPolicyAndHost(w.conf.client.Policy, w.conf.client.Hosts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NoError(t, client.Truncate(nil, namespace, eeSet, nil))
	return w, client
}

func eeLookup(t *testing.T, namespace, extraYAML string) *lookupProcessor {
	t.Helper()

	yaml := `
hosts: [ ` + quoteHosts(eeHosts(t)) + ` ]
namespace: ` + namespace + `
set: ` + eeSet + `
key: '${! json("id") }'
` + extraYAML

	p := newTestProcessor(t, yaml)
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p
}

func quoteHosts(csv string) string {
	parts := strings.Split(csv, ",")
	for i, p := range parts {
		parts[i] = `"` + strings.TrimSpace(p) + `"`
	}
	return strings.Join(parts, ", ")
}

func eeRead(t *testing.T, client *as.Client, namespace, id string) *as.Record {
	t.Helper()
	key, err := as.NewKey(namespace, eeSet, id)
	require.NoError(t, err)
	rec, asErr := client.Get(nil, key)
	if asErr != nil && asErr.Matches(2 /* KEY_NOT_FOUND_ERROR */) {
		return nil
	}
	require.NoError(t, asErr)
	return rec
}

// nsStat sums a namespace statistic across every node, so replica-side effects
// are counted rather than only whatever the master happened to do.
func nsStat(t *testing.T, client *as.Client, namespace, stat string) int64 {
	t.Helper()

	var total int64
	for _, node := range client.GetNodes() {
		info, err := node.RequestInfo(as.NewInfoPolicy(), "namespace/"+namespace)
		require.NoError(t, err)
		for field := range strings.SplitSeq(info["namespace/"+namespace], ";") {
			name, value, ok := strings.Cut(field, "=")
			if !ok || name != stat {
				continue
			}
			n, err := strconv.ParseInt(value, 10, 64)
			require.NoError(t, err)
			total += n
		}
	}
	return total
}

// settledStat waits for a namespace statistic to stop moving. Truncate reclaims
// records in the background, so a count read straight after one drifts under
// the test and turns tombstone assertions into coin flips.
func settledStat(t *testing.T, client *as.Client, namespace, stat string) int64 {
	t.Helper()

	last := nsStat(t, client, namespace, stat)
	for range 50 {
		time.Sleep(100 * time.Millisecond)
		current := nsStat(t, client, namespace, stat)
		if current == last {
			return current
		}
		last = current
	}
	t.Fatalf("%v.%v never settled", namespace, stat)
	return 0
}

func TestEEClusterIsMultiNode(t *testing.T) {
	_, client := eeWriter(t, eeAPNamespace, "")

	nodes := client.GetNodes()
	require.GreaterOrEqual(t, len(nodes), 2, "these tests are meaningless without replicas")

	// Replication factor has to be above one or commit_level and replica are
	// both no-ops no matter what the tests assert.
	assert.GreaterOrEqual(t, nsStat(t, client, eeAPNamespace, "effective_replication_factor"), int64(2)*int64(len(nodes)))
}

// A durable delete leaves a tombstone behind so the record cannot be
// resurrected by a cold restart. Community Edition rejects the flag outright.
func TestEEDurableDelete(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, `
durable_delete: true
operation: '${! meta("op") }'
`)

	create := msg(t, `{"id":"d1","v":"here"}`)
	create.MetaSet("op", "write")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))
	require.NotNil(t, eeRead(t, client, eeAPNamespace, "d1"))

	before := settledStat(t, client, eeAPNamespace, "tombstones")

	del := msg(t, `{"id":"d1"}`)
	del.MetaSet("op", "delete")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{del}))

	assert.Nil(t, eeRead(t, client, eeAPNamespace, "d1"), "record should be gone")
	assert.Greater(t, settledStat(t, client, eeAPNamespace, "tombstones"), before,
		"a durable delete must leave a tombstone, not expunge the record")
}

// Without durable_delete the delete expunges, leaving no tombstone. This is the
// control that proves the assertion above is actually measuring the flag.
func TestEENonDurableDeleteLeavesNoTombstone(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, `
durable_delete: false
operation: '${! meta("op") }'
`)

	create := msg(t, `{"id":"d2","v":"here"}`)
	create.MetaSet("op", "write")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{create}))

	before := settledStat(t, client, eeAPNamespace, "tombstones")

	del := msg(t, `{"id":"d2"}`)
	del.MetaSet("op", "delete")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{del}))

	assert.Nil(t, eeRead(t, client, eeAPNamespace, "d2"))
	assert.Equal(t, before, settledStat(t, client, eeAPNamespace, "tombstones"),
		"an expunging delete must not create a tombstone")
}

func TestEECommitLevels(t *testing.T) {
	for _, level := range []string{"all", "master"} {
		t.Run(level, func(t *testing.T) {
			w, client := eeWriter(t, eeAPNamespace, "commit_level: "+level+"\n")

			require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
				msg(t, `{"id":"cl1","v":"`+level+`"}`),
			}))
			rec := eeRead(t, client, eeAPNamespace, "cl1")
			require.NotNil(t, rec)
			assert.Equal(t, level, rec.Bins["v"])
		})
	}
}

// Every replica policy has to return the record. With replication-factor 2 and
// two nodes, master_proles and random genuinely reach a non-master copy.
func TestEEReplicaPolicies(t *testing.T) {
	w, client := eeWriter(t, eeAPNamespace, "")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"r1","v":"replicated"}`),
	}))
	require.NotNil(t, eeRead(t, client, eeAPNamespace, "r1"))

	for _, replica := range []string{"sequence", "master", "master_proles", "random"} {
		t.Run(replica, func(t *testing.T) {
			for _, mode := range []string{"one", "all"} {
				p := eeLookup(t, eeAPNamespace, `
replica: `+replica+`
read_mode_ap: `+mode+`
bins: [ "v" ]
`)
				out, err := p.ProcessBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"r1"}`)})
				require.NoError(t, err)
				require.Len(t, out, 1)
				require.Len(t, out[0], 1)
				require.NoError(t, out[0][0].GetError())
				assert.Equal(t, map[string]any{"v": "replicated"}, structured(t, out[0][0]),
					"replica=%v read_mode_ap=%v", replica, mode)
			}
		})
	}
}

// Strong consistency is the mode the connector documents as requiring
// commit_level: all, so both the write and every read mode must work against a
// namespace with strong-consistency enabled and a roster set.
func TestEEStrongConsistency(t *testing.T) {
	w, client := eeWriter(t, eeSCNamespace, "commit_level: all\n")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"sc1","v":"consistent"}`),
	}))
	require.NotNil(t, eeRead(t, client, eeSCNamespace, "sc1"))

	for _, mode := range []string{"session", "linearize", "allow_replica", "allow_unavailable"} {
		t.Run(mode, func(t *testing.T) {
			p := eeLookup(t, eeSCNamespace, `
read_mode_sc: `+mode+`
bins: [ "v" ]
`)
			out, err := p.ProcessBatch(t.Context(), service.MessageBatch{msg(t, `{"id":"sc1"}`)})
			require.NoError(t, err)
			require.Len(t, out, 1)
			require.Len(t, out[0], 1)
			require.NoError(t, out[0][0].GetError())
			assert.Equal(t, map[string]any{"v": "consistent"}, structured(t, out[0][0]))
		})
	}
}

// A batch spanning both nodes must complete whether the client fans out across
// nodes or walks them one at a time.
func TestEEConcurrentNodes(t *testing.T) {
	for _, concurrency := range []string{"0", "1", "2"} {
		t.Run("concurrent_nodes_"+concurrency, func(t *testing.T) {
			w, client := eeWriter(t, eeAPNamespace, "concurrent_nodes: "+concurrency+"\n")

			var batch service.MessageBatch
			for i := range 200 {
				batch = append(batch, msg(t, `{"id":"cn`+strconv.Itoa(i)+`","v":`+strconv.Itoa(i)+`}`))
			}
			require.NoError(t, w.WriteBatch(t.Context(), batch))

			for _, i := range []int{0, 99, 199} {
				rec := eeRead(t, client, eeAPNamespace, "cn"+strconv.Itoa(i))
				require.NotNil(t, rec, "key cn%d missing", i)
				assert.Equal(t, i, rec.Bins["v"])
			}
		})
	}
}

// max_in_flight batches running at once must not outrun the connection pool.
// An empty pool is a hard failure rather than a wait in the Aerospike client,
// and writes default to max_retries: 0, so a pool that fills lazily loses every
// batch that arrives while it is still growing. This only shows up on a
// multi-node cluster, where a batch needs a connection per node at once.
func TestEEConcurrentBatchesDoNotStarveThePool(t *testing.T) {
	const concurrency = 64

	w, _ := eeWriter(t, eeAPNamespace, "max_in_flight: "+strconv.Itoa(concurrency)+"\n")

	var failed atomic.Int64
	var wg sync.WaitGroup
	for g := range concurrency {
		wg.Go(func() {
			var batch service.MessageBatch
			for i := range 20 {
				id := "starve" + strconv.Itoa(g) + "_" + strconv.Itoa(i)
				batch = append(batch, msg(t, `{"id":"`+id+`","v":`+strconv.Itoa(i)+`}`))
			}
			if err := w.WriteBatch(t.Context(), batch); err != nil {
				failed.Add(1)
			}
		})
	}
	wg.Wait()

	assert.Zero(t, failed.Load(), "%d of %d concurrent batches failed on a cold pool", failed.Load(), concurrency)
}

// The cluster name is a guard against pointing a pipeline at the wrong cluster,
// so a mismatch has to be refused rather than silently connecting.
func TestEEClusterNameMismatchIsRefused(t *testing.T) {
	yaml := `
hosts: [ ` + quoteHosts(eeHosts(t)) + ` ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this'
cluster_name: definitely-not-the-cluster
`
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	err := w.Connect(t.Context())
	require.Error(t, err, "a wrong cluster_name must not connect")
}

// secHost addresses a cluster with access control enabled. Community Edition
// has no security subsystem at all, so this only runs against Enterprise.
func secHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("AEROSPIKE_SEC_HOST")
	if host == "" {
		t.Skip("AEROSPIKE_SEC_HOST is unset; access-control tests need a secured Enterprise node")
	}
	return host
}

func secWriter(t *testing.T, extraYAML string) *aerospikeWriter {
	t.Helper()

	yaml := `
hosts: [ "` + secHost(t) + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
` + extraYAML

	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

func TestEEAuthInternal(t *testing.T) {
	w := secWriter(t, `
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
`)
	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"auth1","v":"authenticated"}`),
	}))
}

func TestEEAuthRejectsBadPassword(t *testing.T) {
	w := secWriter(t, `
auth_mode: internal
credentials:
  username: rpcn
  password: definitely-wrong
`)
	require.Error(t, w.Connect(t.Context()), "a wrong password must not connect")
}

func TestEEAuthRequiredWhenSecurityEnabled(t *testing.T) {
	w := secWriter(t, "")
	require.Error(t, w.Connect(t.Context()), "a secured cluster must refuse an unauthenticated client")
}

// TLS needs the host spec to carry the server's tls-name, which is the
// host:tlsname:port form the connector documents.
func TestEETLS(t *testing.T) {
	host := os.Getenv("AEROSPIKE_TLS_HOST")
	ca := os.Getenv("AEROSPIKE_TLS_CA")
	if host == "" || ca == "" {
		t.Skip("AEROSPIKE_TLS_HOST/AEROSPIKE_TLS_CA unset; TLS tests need a TLS-enabled Enterprise node")
	}

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
tls:
  enabled: true
  root_cas_file: ` + ca + `
`
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	require.NoError(t, w.Connect(t.Context()))
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
		msg(t, `{"id":"tls1","v":"encrypted"}`),
	}))
}

// An untrusted CA has to fail the handshake rather than fall back to plaintext
// or skip verification silently.
func TestEETLSRejectsUntrustedServer(t *testing.T) {
	host := os.Getenv("AEROSPIKE_TLS_HOST")
	if host == "" {
		t.Skip("AEROSPIKE_TLS_HOST unset")
	}

	yaml := `
hosts: [ "` + host + `" ]
namespace: ` + eeAPNamespace + `
set: ` + eeSet + `
key: '${! json("id") }'
bins: 'root = this.without("id")'
auth_mode: internal
credentials:
  username: rpcn
  password: rpcnpass
tls:
  enabled: true
`
	w := newTestWriter(t, yaml)
	t.Cleanup(func() { _ = w.Close(context.Background()) })

	require.Error(t, w.Connect(t.Context()), "a server cert signed by an unknown CA must be refused")
}
