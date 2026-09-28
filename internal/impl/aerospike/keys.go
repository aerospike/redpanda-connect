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
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Configuration field names that address an Aerospike record.
const (
	fieldNamespace   = "namespace"
	fieldSet         = "set"
	fieldKey         = "key"
	fieldKeyType     = "key_type"
	fieldKeyEncoding = "key_encoding"

	// maxSetNameLen is the server limit on set names. Sets are also capped per
	// namespace (1024 on older servers, 32767 on 7+) and cannot be dropped
	// except by truncating, so interpolating an unbounded value is costly.
	maxSetNameLen = 63

	// maxNamespaceNameLen is the server limit on namespace names.
	maxNamespaceNameLen = 31
)

// keyFields returns the fields that address an Aerospike record.
func keyFields(keyExamples ...string) []*service.ConfigField {
	keyField := service.NewInterpolatedStringField(fieldKey).
		Description("The record primary key.")
	for _, ex := range keyExamples {
		keyField = keyField.Example(ex)
	}

	return []*service.ConfigField{
		service.NewInterpolatedStringField(fieldNamespace).
			Description("The Aerospike namespace. Namespaces are declared in the server config and cannot be created at runtime.").
			Example("test").
			Example(`${! meta("as_namespace") }`),

		service.NewInterpolatedStringField(fieldSet).
			Description("The set within the namespace. Sets are created implicitly on first write and cannot be dropped except by truncating. Names are at most 63 bytes and must not contain a colon. A namespace also has a hard cap on how many sets it can hold, so do not interpolate an unbounded value such as a Kafka topic name. Leave empty for the null set.").
			Default("").
			Example("users").
			Example(`${! meta("as_set") }`),

		keyField,

		service.NewStringEnumField(fieldKeyType, "string", "int", "bytes").
			Description("How to interpret the resolved `key`. Aerospike addresses records by a digest of the key, and the digest differs between a string `\"123\"` and an integer `123` — so this must match whatever else reads or writes these records.").
			Default("string"),

		service.NewStringEnumField(fieldKeyEncoding, "utf8", "base64", "hex").
			Description("How to decode the interpolated `key` when `key_type` is `bytes`. Interpolation always yields a string, so a genuine binary key must be carried as base64 or hex. `utf8` uses the string's raw bytes and only matches other clients that also stored UTF-8.").
			Default("utf8").
			Advanced(),
	}
}

// keyConfig is the parsed record addressing configuration.
type keyConfig struct {
	Namespace   *service.InterpolatedString
	Set         *service.InterpolatedString
	Key         *service.InterpolatedString
	KeyType     string
	KeyEncoding string
}

// parseKeyConfig reads the fields produced by keyFields.
func parseKeyConfig(conf *service.ParsedConfig) (*keyConfig, error) {
	k := &keyConfig{}

	var err error
	if k.Namespace, err = conf.FieldInterpolatedString(fieldNamespace); err != nil {
		return nil, err
	}
	if k.Set, err = conf.FieldInterpolatedString(fieldSet); err != nil {
		return nil, err
	}
	if k.Key, err = conf.FieldInterpolatedString(fieldKey); err != nil {
		return nil, err
	}
	if k.KeyType, err = conf.FieldString(fieldKeyType); err != nil {
		return nil, err
	}
	if k.KeyEncoding, err = conf.FieldString(fieldKeyEncoding); err != nil {
		return nil, err
	}
	return k, nil
}

// keyResolver resolves record keys for one batch.
//
// Interpolation is bound to the batch rather than to individual messages so
// that batch-aware functions such as `batch_index()` and windowed Bloblang
// queries behave as they do everywhere else in Redpanda Connect.
type keyResolver struct {
	namespace   *service.MessageBatchInterpolationExecutor
	set         *service.MessageBatchInterpolationExecutor
	key         *service.MessageBatchInterpolationExecutor
	keyType     string
	keyEncoding string
}

// resolver binds key interpolation to a batch so batch-aware Bloblang works.
func (k *keyConfig) resolver(batch service.MessageBatch) *keyResolver {
	return &keyResolver{
		namespace:   batch.InterpolationExecutor(k.Namespace),
		set:         batch.InterpolationExecutor(k.Set),
		key:         batch.InterpolationExecutor(k.Key),
		keyType:     k.KeyType,
		keyEncoding: k.KeyEncoding,
	}
}

// resolve builds the Aerospike key for the message at the given batch index.
func (r *keyResolver) resolve(index int) (*as.Key, error) {
	namespace, err := r.namespace.TryString(index)
	if err != nil {
		return nil, fmt.Errorf("interpolating '%v': %w", fieldNamespace, err)
	}
	if err := validateNamespaceName(namespace); err != nil {
		return nil, fmt.Errorf("field '%v': %w", fieldNamespace, err)
	}

	setName, err := r.set.TryString(index)
	if err != nil {
		return nil, fmt.Errorf("interpolating '%v': %w", fieldSet, err)
	}
	if err := validateSetName(setName); err != nil {
		return nil, fmt.Errorf("field '%v': %w", fieldSet, err)
	}

	keyStr, err := r.key.TryString(index)
	if err != nil {
		return nil, fmt.Errorf("interpolating '%v': %w", fieldKey, err)
	}
	// An interpolation over a missing field yields the literal "null" rather
	// than an error. Left alone, every message with an absent key would address
	// the same record — silently, and only discovered much later.
	if keyStr == "" || keyStr == "null" {
		return nil, fmt.Errorf(
			"field '%v' resolved to %q, which is not a usable record key; the source field is probably missing from this message",
			fieldKey, keyStr)
	}

	keyVal, err := coerceKey(keyStr, r.keyType, r.keyEncoding)
	if err != nil {
		return nil, err
	}

	key, asErr := as.NewKey(namespace, setName, keyVal)
	if asErr != nil {
		return nil, fmt.Errorf("building record key: %w", asErr)
	}
	return key, nil
}

func coerceKey(s, keyType, encoding string) (any, error) {
	switch keyType {
	case "int":
		if encoding != "" && encoding != "utf8" {
			return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is bytes", fieldKeyEncoding, encoding, fieldKeyType)
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("field '%v' resolved to %q, which is not an integer as required by '%v': %w", fieldKey, s, fieldKeyType, err)
		}
		return v, nil
	case "bytes":
		return decodeBytesKey(s, encoding)
	default:
		if encoding != "" && encoding != "utf8" {
			return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is bytes", fieldKeyEncoding, encoding, fieldKeyType)
		}
		return s, nil
	}
}

func decodeBytesKey(s, encoding string) (any, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf8":
		return []byte(s), nil
	case "base64":
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("field '%v' resolved to %q, which is not valid base64: %w", fieldKey, s, err)
		}
		return b, nil
	case "hex":
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("field '%v' resolved to %q, which is not valid hex: %w", fieldKey, s, err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("field '%v': unknown encoding %q", fieldKeyEncoding, encoding)
	}
}

// validateNamespaceName checks a resolved namespace against the server's
// limits. An over-long name is rejected by the server as INVALID_NAMESPACE,
// which reads like a missing namespace rather than a malformed one.
func validateNamespaceName(name string) error {
	if name == "" {
		return errors.New("resolved to an empty string")
	}
	if len(name) > maxNamespaceNameLen {
		return fmt.Errorf("namespace %q is %d bytes, which exceeds the Aerospike limit of %d", name, len(name), maxNamespaceNameLen)
	}
	return nil
}

// validateSetName checks a resolved set name against the server's limits.
func validateSetName(name string) error {
	if name == "" {
		return nil
	}
	if len(name) > maxSetNameLen {
		return fmt.Errorf("set name %q is %d bytes, which exceeds the Aerospike limit of %d", name, len(name), maxSetNameLen)
	}
	if strings.Contains(name, ":") {
		return fmt.Errorf("set name %q contains a colon, which Aerospike does not allow", name)
	}
	return nil
}

// keyID identifies a record for deduplication. The digest is what the server
// addresses records by, so two user keys with the same digest are one record.
func keyID(k *as.Key) string {
	var b strings.Builder
	b.WriteString(k.Namespace())
	b.WriteByte(0)
	b.WriteString(k.SetName())
	b.WriteByte(0)
	b.Write(k.Digest())
	return b.String()
}
