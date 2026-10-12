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

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Configuration field names that address an Aerospike record.
const (
	fieldNamespace   = "namespace"
	fieldSet         = "set"
	fieldKey         = "key"
	fieldDigest      = "digest"
	fieldKeyType     = "key_type"
	fieldKeyEncoding = "key_encoding"

	// digestLen is the RIPEMD-160 length Aerospike uses to address a record.
	// The client rejects any other length.
	digestLen = 20

	// maxSetNameLen is the server limit on set names. Sets are also capped per
	// namespace (1024 on older servers, 32767 on 7+) and cannot be dropped
	// except by truncating, so interpolating an unbounded value is costly.
	maxSetNameLen = 63

	// maxNamespaceNameLen is the server limit on namespace names.
	maxNamespaceNameLen = 31
)

// keyFields returns the fields that address an Aerospike record by user key.
func keyFields(keyExamples ...string) []*service.ConfigField {
	return addressFields(false, keyExamples...)
}

// outputKeyFields is keyFields plus an optional digest. The output accepts one
// of key or digest. Lookup stays on keyFields, so it cannot address a digest.
func outputKeyFields(keyExamples ...string) []*service.ConfigField {
	return addressFields(true, keyExamples...)
}

func addressFields(withDigest bool, keyExamples ...string) []*service.ConfigField {
	keyDesc := "The record primary key."
	if withDigest {
		keyDesc = "The record primary key. Set this or `digest`, not both. Aerospike hashes this value into the record digest; to write a digest the producer already has, use `digest`."
	}
	keyField := service.NewInterpolatedStringField(fieldKey).
		Description(keyDesc)
	if withDigest {
		keyField = keyField.Optional()
	}
	for _, ex := range keyExamples {
		keyField = keyField.Example(ex)
	}

	fields := []*service.ConfigField{
		service.NewInterpolatedStringField(fieldNamespace).
			Description("The Aerospike namespace. Namespaces are declared in the server config and cannot be created at runtime. A resolved value of `null` is rejected, because a missing interpolated field stringifies to that. Use `meta(\"kafka_topic\")` when the topic name is the namespace.").
			Example("test").
			Example(`${! meta("kafka_topic") }`).
			Example(`${! json("namespace_name") }`).
			Example(`${! meta("as_namespace") }`),

		service.NewInterpolatedStringField(fieldSet).
			Description("The set within the namespace. Sets are created implicitly on first write and cannot be dropped except by truncating. Names are at most 63 bytes and must not contain a colon. A namespace also has a hard cap on how many sets it can hold, so do not interpolate an unbounded value. A small fixed list, such as the topics this pipeline consumes, is fine. A payload field is only safe when the mapping throws for anything outside that list, so a bad value fails before a write can create a set. Leave empty for the null set. The literal `null` is rejected, because a missing interpolated field stringifies to that and would create a set that cannot be dropped except by truncating.").
			Default("").
			Example("users").
			Example(`${! meta("kafka_topic") }`).
			Example(`${! if ["clicks", "views"].contains(json("set_name")) { json("set_name") } else { throw("set_name is not allowed") } }`).
			Example(`${! meta("as_set") }`),

		keyField,

		service.NewStringEnumField(fieldKeyType, "string", "int", "bytes").
			Description("How to interpret the resolved `key`. Aerospike addresses records by a digest of the key, and the digest differs between a string `\"123\"` and an integer `123`. This must match whatever else reads or writes these records.").
			Default("string"),

		service.NewStringEnumField(fieldKeyEncoding, "utf8", "base64", "hex").
			Description("How to decode the interpolated `key` when `key_type` is `bytes`. Interpolation always yields a string, so a genuine binary key must be carried as base64 or hex. `utf8` uses the string's raw bytes and only matches other clients that also stored UTF-8.").
			Default("utf8").
			Advanced(),
	}
	if !withDigest {
		return fields
	}

	digestField := service.NewInterpolatedStringField(fieldDigest).
		Description("The 20-byte record digest, as produced by another Aerospike client. The user key stored with the record is null. Set this or `key`, not both. `key_type` and `key_encoding` apply only to `key`.\n\nA string is decoded as standard Base64 and must yield 20 bytes. A string that is already 20 bytes is the raw digest, which is what `meta(\"kafka_key\")` produces for a binary Kafka key. A byte array, from `metadata(\"kafka_key\")` or `content()`, is used as-is and must be 20 bytes. Any other type is rejected.\n\nThis does not hash the value. `key` with `key_type: bytes` hashes those bytes and addresses a different record.").
		Optional().
		Example(`${! json("digest") }`).
		Example(`${! meta("kafka_key") }`).
		Example(`${! content().string() }`)
	// Sit digest next to key. key_type and key_encoding stay at the end because
	// they describe the user key only.
	ordered := make([]*service.ConfigField, 0, len(fields)+1)
	ordered = append(ordered, fields[:3]...)
	ordered = append(ordered, digestField)
	ordered = append(ordered, fields[3:]...)
	return ordered
}

// keyConfig is the parsed record addressing configuration.
type keyConfig struct {
	Namespace   *service.InterpolatedString
	Set         *service.InterpolatedString
	Key         *service.InterpolatedString
	Digest      *digestConfig
	KeyType     string
	KeyEncoding string
}

// digestConfig is how a digest field is evaluated.
//
// A single Bloblang interpolation is executed as a query so a string stays a
// string and a byte array stays a byte array. Interpolation itself stringifies
// both, which would make those two cases indistinguishable. A static value or
// a composite interpolation is always a string.
type digestConfig struct {
	query  *bloblang.Executor
	text   *service.InterpolatedString
	static []byte
}

// parseKeyConfig reads the fields produced by keyFields or outputKeyFields.
//
// Lookup always has a key. The output has exactly one of key and digest; the
// lookup spec does not declare digest, so Contains reports it absent there.
func parseKeyConfig(conf *service.ParsedConfig) (*keyConfig, error) {
	k := &keyConfig{}

	var err error
	if k.Namespace, err = conf.FieldInterpolatedString(fieldNamespace); err != nil {
		return nil, err
	}
	if k.Set, err = conf.FieldInterpolatedString(fieldSet); err != nil {
		return nil, err
	}
	if k.KeyType, err = conf.FieldString(fieldKeyType); err != nil {
		return nil, err
	}
	if k.KeyEncoding, err = conf.FieldString(fieldKeyEncoding); err != nil {
		return nil, err
	}

	keySet := conf.Contains(fieldKey)
	digestSet := conf.Contains(fieldDigest)
	if keySet == digestSet {
		return nil, errors.New("exactly one of fields 'key' and 'digest' must be set")
	}
	if keySet {
		if k.Key, err = conf.FieldInterpolatedString(fieldKey); err != nil {
			return nil, err
		}
	} else {
		if k.KeyType != "string" {
			return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is set", fieldKeyType, k.KeyType, fieldKey)
		}
		if k.KeyEncoding != "" && k.KeyEncoding != "utf8" {
			return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is set", fieldKeyEncoding, k.KeyEncoding, fieldKey)
		}
		raw, err := conf.FieldString(fieldDigest)
		if err != nil {
			return nil, err
		}
		if k.Digest, err = parseDigest(raw); err != nil {
			return nil, err
		}
	}

	if ns, ok := k.Namespace.Static(); ok {
		if err := validateNamespaceName(ns); err != nil {
			return nil, fmt.Errorf("field '%v': %w", fieldNamespace, err)
		}
	}
	if set, ok := k.Set.Static(); ok {
		if err := validateSetName(set); err != nil {
			return nil, fmt.Errorf("field '%v': %w", fieldSet, err)
		}
	}
	return k, nil
}

// keyResolver resolves record keys for one batch.
//
// Interpolation is bound to the batch rather than to individual messages so
// that batch-aware functions such as `batch_index()` and windowed Bloblang
// queries behave as they do everywhere else in Redpanda Connect.
type keyResolver struct {
	namespace    *service.MessageBatchInterpolationExecutor
	set          *service.MessageBatchInterpolationExecutor
	key          *service.MessageBatchInterpolationExecutor
	keyType      string
	keyEncoding  string
	digestStatic []byte
	digestQuery  *service.MessageBatchBloblangExecutor
	digestText   *service.MessageBatchInterpolationExecutor
}

// resolver binds key interpolation to a batch so batch-aware Bloblang works.
func (k *keyConfig) resolver(batch service.MessageBatch) *keyResolver {
	r := &keyResolver{
		namespace:   batch.InterpolationExecutor(k.Namespace),
		set:         batch.InterpolationExecutor(k.Set),
		keyType:     k.KeyType,
		keyEncoding: k.KeyEncoding,
	}
	if k.Key != nil {
		r.key = batch.InterpolationExecutor(k.Key)
	}
	if k.Digest != nil {
		r.digestStatic = k.Digest.static
		if k.Digest.query != nil {
			r.digestQuery = batch.BloblangExecutor(k.Digest.query)
		}
		if k.Digest.text != nil {
			r.digestText = batch.InterpolationExecutor(k.Digest.text)
		}
	}
	return r
}

// resolve builds the Aerospike key for the message at the given batch index.
func (r *keyResolver) resolve(index int) (*as.Key, error) {
	namespace, setName, err := r.namespaceAndSet(index)
	if err != nil {
		return nil, err
	}
	if r.digesting() {
		return r.resolveDigest(index, namespace, setName)
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

func (r *keyResolver) namespaceAndSet(index int) (string, string, error) {
	namespace, err := r.namespace.TryString(index)
	if err != nil {
		return "", "", fmt.Errorf("interpolating '%v': %w", fieldNamespace, err)
	}
	if err := validateNamespaceName(namespace); err != nil {
		return "", "", fmt.Errorf("field '%v': %w", fieldNamespace, err)
	}

	setName, err := r.set.TryString(index)
	if err != nil {
		return "", "", fmt.Errorf("interpolating '%v': %w", fieldSet, err)
	}
	if err := validateSetName(setName); err != nil {
		return "", "", fmt.Errorf("field '%v': %w", fieldSet, err)
	}
	return namespace, setName, nil
}

func (r *keyResolver) digesting() bool {
	return r.digestStatic != nil || r.digestQuery != nil || r.digestText != nil
}

func (r *keyResolver) resolveDigest(index int, namespace, setName string) (*as.Key, error) {
	raw, err := r.digestBytes(index)
	if err != nil {
		return nil, err
	}
	// A nil user key is stored as Aerospike null and is not sent, even when
	// send_key is set. The digest is the record address.
	key, asErr := as.NewKeyWithDigest(namespace, setName, nil, raw)
	if asErr != nil {
		return nil, fmt.Errorf("building record digest: %w", asErr)
	}
	return key, nil
}

func (r *keyResolver) digestBytes(index int) ([]byte, error) {
	if r.digestStatic != nil {
		return r.digestStatic, nil
	}
	if r.digestQuery != nil {
		v, err := r.digestQuery.QueryValue(index)
		if err != nil {
			return nil, fmt.Errorf("interpolating '%v': %w", fieldDigest, err)
		}
		return decodeDigestValue(v)
	}
	s, err := r.digestText.TryString(index)
	if err != nil {
		return nil, fmt.Errorf("interpolating '%v': %w", fieldDigest, err)
	}
	return decodeDigestString(s)
}

// parseDigest classifies a digest field. A single ${! query } keeps the
// Bloblang type. Anything else resolves to a string and is decoded as Base64,
// or as 20 raw bytes when the string is already that long.
func parseDigest(raw string) (*digestConfig, error) {
	if query, ok, err := singleDigestQuery(raw); err != nil {
		return nil, err
	} else if ok {
		exe, err := bloblang.Parse("root = " + query)
		if err != nil {
			return nil, fmt.Errorf("field '%v': %w", fieldDigest, err)
		}
		return &digestConfig{query: exe}, nil
	}

	text, err := service.NewInterpolatedString(raw)
	if err != nil {
		return nil, fmt.Errorf("field '%v': %w", fieldDigest, err)
	}
	if lit, ok := text.Static(); ok {
		decoded, err := decodeDigestString(lit)
		if err != nil {
			return nil, err
		}
		return &digestConfig{static: decoded}, nil
	}
	return &digestConfig{text: text}, nil
}

// singleDigestQuery returns the expression inside a field that is exactly one
// interpolation. A composite interpolation is a string, so ok is false and the
// caller decodes the interpolated text.
func singleDigestQuery(raw string) (string, bool, error) {
	s := strings.TrimSpace(raw)
	if !strings.HasPrefix(s, "${!") || !strings.HasSuffix(s, "}") {
		return "", false, nil
	}
	inner := strings.TrimSpace(s[len("${!") : len(s)-1])
	if strings.Contains(inner, "${!") {
		return "", false, nil
	}
	if inner == "" {
		return "", false, fmt.Errorf("field '%v': interpolation is empty", fieldDigest)
	}
	if _, err := bloblang.Parse("root = " + inner); err != nil {
		return "", false, fmt.Errorf("field '%v': %w", fieldDigest, err)
	}
	return inner, true, nil
}

func decodeDigestValue(v any) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		return nil, fmt.Errorf("field '%v' resolved to null; the source field is probably missing from this message", fieldDigest)
	case string:
		return decodeDigestString(t)
	case []byte:
		return checkDigestLen(t)
	default:
		return nil, fmt.Errorf("field '%v' resolved to %T, which is not a Base64 string or raw bytes", fieldDigest, v)
	}
}

func decodeDigestString(s string) ([]byte, error) {
	if s == "" || s == "null" {
		return nil, fmt.Errorf("field '%v' resolved to %q, which is not a usable digest; the source field is probably missing from this message", fieldDigest, s)
	}
	// Standard Base64 of 20 bytes is 28 characters, so it never collides with
	// the raw 20-byte string that meta() produces from a binary Kafka key.
	decoded, err := base64.StdEncoding.DecodeString(s)
	switch {
	case err == nil && len(decoded) == digestLen:
		return decoded, nil
	case len(s) == digestLen:
		return []byte(s), nil
	case err == nil:
		return nil, fmt.Errorf("field '%v' decoded to %d bytes, which is not the Aerospike digest length of %d", fieldDigest, len(decoded), digestLen)
	default:
		return nil, fmt.Errorf("field '%v' resolved to a %d byte string, which is not standard Base64 of a %d byte digest and is not %d raw bytes: %w", fieldDigest, len(s), digestLen, digestLen, err)
	}
}

func checkDigestLen(b []byte) ([]byte, error) {
	if len(b) != digestLen {
		return nil, fmt.Errorf("field '%v' is %d bytes, which is not the Aerospike digest length of %d", fieldDigest, len(b), digestLen)
	}
	return b, nil
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
	// Interpolation of a missing field yields the literal "null", which would
	// otherwise look like a namespace that does not exist.
	if name == "null" {
		return errors.New(`resolved to "null"; the source field is probably missing`)
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
	// The literal "null" is what a missing interpolated field becomes, and
	// writing it would create a set that can only be removed by truncating.
	if name == "null" {
		return errors.New(`resolved to "null"; the source field is probably missing`)
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
