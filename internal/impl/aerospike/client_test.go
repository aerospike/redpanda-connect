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
	"encoding/base64"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestToAerospike(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		coerce bool
		want   any
	}{
		// JSON has one number type, so an id or counter arrives as float64.
		// Storing it as an Aerospike double would break integer comparisons,
		// add operations and integer secondary indexes.
		{"integral float coerced to int", float64(42), true, int64(42)},
		{"integral float left alone", float64(42), false, float64(42)},
		{"fractional float stays float", 4.5, true, 4.5},
		{"negative integral float", float64(-7), true, int64(-7)},
		{"zero", float64(0), true, int64(0)},
		{"string", "hello", true, "hello"},
		{"bool", true, true, true},
		{"nil deletes the bin", nil, true, nil},
		{"int64 passes through", int64(9), true, int64(9)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toAerospike(tc.in, tc.coerce)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestToAerospikeNested(t *testing.T) {
	got, err := toAerospike(map[string]any{
		"count": float64(3),
		"ratio": 0.5,
		"tags":  []any{"a", float64(1)},
		"inner": map[string]any{"n": float64(10)},
	}, true)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"count": int64(3),
		"ratio": 0.5,
		"tags":  []any{"a", int64(1)},
		"inner": map[string]any{"n": int64(10)},
	}, got)
}

// TestFromAerospikeMapKeys covers the conversion that actually matters on read:
// Aerospike map keys are not restricted to strings, so a map bin unpacks as
// map[any]any, which encoding/json cannot marshal at all.
func TestFromAerospikeMapKeys(t *testing.T) {
	in := map[any]any{
		"name": "Ada",
		1:      "int key",
		"nested": map[any]any{
			2: []any{map[any]any{"deep": true}},
		},
	}

	got := fromAerospike(in)

	want := map[string]any{
		"name": "Ada",
		"1":    "int key",
		"nested": map[string]any{
			"2": []any{map[string]any{"deep": true}},
		},
	}
	assert.Equal(t, want, got)
}

func TestFromAerospikePassthrough(t *testing.T) {
	assert.Equal(t, "x", fromAerospike("x"))
	assert.Equal(t, 5, fromAerospike(5))
	assert.Nil(t, fromAerospike(nil))
	assert.Equal(t, []any{int64(1), "a"}, fromAerospike([]any{int64(1), "a"}))
}

func TestValidateBinName(t *testing.T) {
	assert.NoError(t, validateBinName("a"))
	assert.NoError(t, validateBinName("123456789012345")) // exactly 15

	err := validateBinName("1234567890123456") // 16
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 15")

	assert.Error(t, validateBinName(""))
}

func TestIsTombstone(t *testing.T) {
	assert.False(t, isFencedTombstone(nil, defaultTombstoneBin))
	assert.False(t, isFencedTombstone(map[string]any{"tier": "gold"}, defaultTombstoneBin))
	assert.False(t, isFencedTombstone(map[string]any{defaultTombstoneBin: false}, defaultTombstoneBin))
	assert.False(t, isFencedTombstone(map[string]any{defaultTombstoneBin: nil}, defaultTombstoneBin))
	assert.True(t, isFencedTombstone(map[string]any{defaultTombstoneBin: true}, defaultTombstoneBin))
}

func parseClientYAML(t *testing.T, yaml string) (*clientConfig, error) {
	t.Helper()
	spec := service.NewConfigSpec().Fields(clientFields()...)
	parsed, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		return nil, err
	}
	return parseClientConfig(parsed)
}

func TestParseClientConfigAuthMode(t *testing.T) {
	t.Run("defaults to internal", func(t *testing.T) {
		c, err := parseClientYAML(t, `hosts: ["localhost:3000"]`)
		require.NoError(t, err)
		assert.Equal(t, as.AuthModeInternal, c.Policy.AuthMode)
	})
	t.Run("external", func(t *testing.T) {
		c, err := parseClientYAML(t, `
hosts: ["localhost:3000"]
auth_mode: external
`)
		require.NoError(t, err)
		assert.Equal(t, as.AuthModeExternal, c.Policy.AuthMode)
	})
	t.Run("pki", func(t *testing.T) {
		c, err := parseClientYAML(t, `
hosts: ["localhost:3000"]
auth_mode: pki
`)
		require.NoError(t, err)
		assert.Equal(t, as.AuthModePKI, c.Policy.AuthMode)
	})
}

func TestParseHost(t *testing.T) {
	tests := []struct {
		in      string
		name    string
		tlsName string
		port    int
		wantErr string
	}{
		{in: "localhost", name: "localhost", port: 3000},
		{in: "as1.internal:3100", name: "as1.internal", port: 3100},
		{in: "as1.internal:clusterA:3000", name: "as1.internal", tlsName: "clusterA", port: 3000},
		{in: "as1.internal:clusterA", name: "as1.internal", tlsName: "clusterA", port: 3000},
		// A TLS certificate name may begin with a digit, so only a wholly
		// numeric component is a port.
		{in: "as1.internal:1cluster", name: "as1.internal", tlsName: "1cluster", port: 3000},
		{in: "as1.internal:1cluster:4333", name: "as1.internal", tlsName: "1cluster", port: 4333},
		{in: "as1.internal:99999", wantErr: "out of range"},
		{in: "127.0.0.1", name: "127.0.0.1", port: 3000},
		{in: "::1", name: "::1", port: 3000},
		{in: "[::1]:3000", name: "::1", port: 3000},
		{in: "[2001:db8::1]:clusterA:4333", name: "2001:db8::1", tlsName: "clusterA", port: 4333},
		{in: "", wantErr: "empty host"},
		{in: "[::1", wantErr: "unclosed"},
		{in: "as1.internal:3000:clusterA", wantErr: "host:tlsname:port"},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			h, err := parseHost(tc.in)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.name, h.Name)
			assert.Equal(t, tc.tlsName, h.TLSName)
			assert.Equal(t, tc.port, h.Port)
		})
	}
}

func TestLimitDuration(t *testing.T) {
	t.Run("no deadline keeps the configured timeout", func(t *testing.T) {
		d, err := limitDuration(context.Background(), 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 10*time.Second, d)
	})
	t.Run("cancelled context fails immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := limitDuration(ctx, 10*time.Second)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("deadline shorter than configured wins", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		d, err := limitDuration(ctx, 10*time.Second)
		require.NoError(t, err)
		assert.Greater(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, 50*time.Millisecond)
	})
	t.Run("configured shorter than deadline wins", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		d, err := limitDuration(ctx, 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, 10*time.Second, d)
	})
}

func TestBatchPolicyForContext(t *testing.T) {
	p := as.NewBatchPolicy()
	p.TotalTimeout = 10 * time.Second
	p.SocketTimeout = 2 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	got, err := batchPolicyForContext(ctx, p)
	require.NoError(t, err)
	assert.LessOrEqual(t, got.TotalTimeout, 200*time.Millisecond)
	assert.LessOrEqual(t, got.SocketTimeout, got.TotalTimeout)
	assert.Equal(t, 10*time.Second, p.TotalTimeout, "must not mutate the shared policy")
}

func TestConnectCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := newConnection(&clientConfig{
		Policy: as.NewClientPolicy(),
		Hosts:  []*as.Host{as.NewHost("127.0.0.1", 1)},
	}, nil)

	err := c.connect(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func parseBatchYAML(t *testing.T, yaml string) (*as.BatchPolicy, error) {
	t.Helper()
	spec := service.NewConfigSpec().Fields(batchPolicyFields()...)
	parsed, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		return nil, err
	}
	return parseBatchPolicy(parsed)
}

// An unlimited total timeout must not be read as "no limit of any kind": the
// socket timeout is the only thing left bounding a stuck connection.
func TestBatchPolicyForContextKeepsSocketTimeoutWhenTotalIsUnlimited(t *testing.T) {
	p := as.NewBatchPolicy()
	p.TotalTimeout = 0
	p.SocketTimeout = 5 * time.Second

	got, err := batchPolicyForContext(context.Background(), p)
	require.NoError(t, err)
	assert.Zero(t, got.TotalTimeout)
	assert.Equal(t, 5*time.Second, got.SocketTimeout)
}

func TestParseClientConfigConnectionPoolBounds(t *testing.T) {
	parse := func(t *testing.T, extra string) (*clientConfig, error) {
		t.Helper()
		conf, err := outputSpec().ParseYAML(baseConfig+extra, nil)
		require.NoError(t, err)
		return parseClientConfig(conf)
	}

	// Warm up fills to the minimum, so a zero minimum means the default config
	// does not open a pool-sized burst of connections to every node on start.
	t.Run("min defaults to zero", func(t *testing.T) {
		c, err := parse(t, "")
		require.NoError(t, err)
		assert.Zero(t, c.Policy.MinConnectionsPerNode)
		assert.Equal(t, 100, c.Policy.ConnectionQueueSize)
	})

	t.Run("min above max is rejected", func(t *testing.T) {
		_, err := parse(t, "max_connections_per_node: 10\nmin_connections_per_node: 20\n")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not exceed")
	})

	t.Run("error rate circuit breaker is configurable", func(t *testing.T) {
		c, err := parse(t, "max_error_rate: 25\nerror_rate_window: 3\n")
		require.NoError(t, err)
		assert.Equal(t, 25, c.Policy.MaxErrorRate)
		assert.Equal(t, 3, c.Policy.ErrorRateWindow)
	})
}

func TestValidateNamespaceName(t *testing.T) {
	require.NoError(t, validateNamespaceName("test"))

	err := validateNamespaceName("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")

	err = validateNamespaceName(strings.Repeat("n", maxNamespaceNameLen+1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the Aerospike limit of 31")
}

func TestParseBatchPolicyRejectsNegatives(t *testing.T) {
	_, err := parseBatchYAML(t, `
concurrent_nodes: -1
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "concurrent_nodes")

	_, err = parseBatchYAML(t, `
max_retries: -1
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_retries")
}

func TestParseBatchPolicyZeroIsAllNodes(t *testing.T) {
	p, err := parseBatchYAML(t, "")
	require.NoError(t, err)
	assert.Equal(t, 0, p.ConcurrentNodes)
	assert.Equal(t, 2, p.MaxRetries)
	assert.True(t, p.RespondAllKeys)
}

func TestWriteBatchPolicyDefaultsToNoRetries(t *testing.T) {
	spec := service.NewConfigSpec().Fields(batchPolicyFieldsWithRetries(0)...)
	parsed, err := spec.ParseYAML("", nil)
	require.NoError(t, err)
	p, err := parseBatchPolicy(parsed)
	require.NoError(t, err)
	assert.Equal(t, 0, p.MaxRetries)
}

func TestEstimateSize(t *testing.T) {
	assert.Equal(t, 0, estimateSize(nil))
	assert.Equal(t, 5, estimateSize("hello"))
	assert.Greater(t, estimateSize([]any{"abcdefghij", "abcdefghij", "abcdefghij"}), 20)
}

func TestParseCommitLevel(t *testing.T) {
	got, err := parseCommitLevel("all")
	require.NoError(t, err)
	assert.Equal(t, as.COMMIT_ALL, got)

	got, err = parseCommitLevel("master")
	require.NoError(t, err)
	assert.Equal(t, as.COMMIT_MASTER, got)

	_, err = parseCommitLevel("quorum")
	require.Error(t, err)
}

func TestParseReplicaPolicy(t *testing.T) {
	got, err := parseReplicaPolicy("sequence")
	require.NoError(t, err)
	assert.Equal(t, as.SEQUENCE, got)

	got, err = parseReplicaPolicy("master")
	require.NoError(t, err)
	assert.Equal(t, as.MASTER, got)

	_, err = parseReplicaPolicy("nearest")
	require.Error(t, err)
}

func parseKeyYAML(t *testing.T, yaml string) (*keyConfig, error) {
	t.Helper()
	spec := service.NewConfigSpec().Fields(keyFields()...)
	parsed, err := spec.ParseYAML(yaml, nil)
	if err != nil {
		return nil, err
	}
	return parseKeyConfig(parsed)
}

func resolveKey(t *testing.T, yaml string, msg *service.Message) (*as.Key, error) {
	t.Helper()
	k, err := parseKeyYAML(t, yaml)
	require.NoError(t, err)
	return k.resolver(service.MessageBatch{msg}).resolve(0)
}

func TestKeyRejectsOverlongSetName(t *testing.T) {
	msg := service.NewMessage([]byte(`{"id":"u1"}`))
	msg.MetaSet("set", strings.Repeat("s", maxSetNameLen+1))

	_, err := resolveKey(t, `
namespace: test
set: '${! meta("set") }'
key: '${! json("id") }'
`, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "63")
}

func TestKeyRejectsColonInSetName(t *testing.T) {
	msg := service.NewMessage([]byte(`{"id":"u1"}`))
	msg.MetaSet("set", "users:v2")

	_, err := resolveKey(t, `
namespace: test
set: '${! meta("set") }'
key: '${! json("id") }'
`, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "colon")
}

func TestKeyAllowsEmptySet(t *testing.T) {
	msg := service.NewMessage([]byte(`{"id":"u1"}`))
	key, err := resolveKey(t, `
namespace: test
key: '${! json("id") }'
`, msg)
	require.NoError(t, err)
	assert.Empty(t, key.SetName())
}

func TestKeyBytesBase64(t *testing.T) {
	raw := []byte{0x00, 0xff, 0x10, 0x80}
	msg := service.NewMessage(nil)
	msg.MetaSet("k", base64.StdEncoding.EncodeToString(raw))

	key, err := resolveKey(t, `
namespace: test
set: users
key: '${! meta("k") }'
key_type: bytes
key_encoding: base64
`, msg)
	require.NoError(t, err)
	assert.Equal(t, raw, key.Value().GetObject())
}

func TestKeyBytesHex(t *testing.T) {
	msg := service.NewMessage(nil)
	msg.MetaSet("k", "00ff1080")

	key, err := resolveKey(t, `
namespace: test
key: '${! meta("k") }'
key_type: bytes
key_encoding: hex
`, msg)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0xff, 0x10, 0x80}, key.Value().GetObject())
}

func TestKeyBytesUTF8Default(t *testing.T) {
	msg := service.NewMessage(nil)
	msg.MetaSet("k", "hello")

	key, err := resolveKey(t, `
namespace: test
key: '${! meta("k") }'
key_type: bytes
`, msg)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), key.Value().GetObject())
}

func TestKeyBytesRejectsBadBase64(t *testing.T) {
	msg := service.NewMessage(nil)
	msg.MetaSet("k", "not base64")

	_, err := resolveKey(t, `
namespace: test
key: '${! meta("k") }'
key_type: bytes
key_encoding: base64
`, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base64")
}
