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
	fieldNamespace      = "namespace"
	fieldSet            = "set"
	fieldKey            = "key"
	fieldDigest         = "digest"
	fieldDigestEncoding = "digest_encoding"
	fieldKeyType        = "key_type"
	fieldKeyEncoding    = "key_encoding"

	digestEncodingBase64 = "base64"
	digestEncodingHex    = "hex"
	digestEncodingRaw    = "raw"

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
	}
	if withDigest {
		fields = append(fields,
			service.NewInterpolatedStringField(fieldDigest).
				Description("The 20-byte record digest, as produced by another Aerospike client. The user key stored with the record is null. Set this or `key`, not both. `key_type` and `key_encoding` apply only to `key`, and `digest_encoding` applies only to `digest`.\n\nThis does not hash the value. `key` with `key_type: bytes` hashes those bytes and addresses a different record.").
				Optional().
				Example(`${! json("digest") }`).
				Example(`${! meta("kafka_key") }`).
				Example(`${! content() }`),

			service.NewStringEnumField(fieldDigestEncoding, digestEncodingBase64, digestEncodingHex, digestEncodingRaw).
				Description("How to decode the interpolated `digest`. `base64` is standard Base64, the form Aerospike's Kafka outbound connector writes to JSON. `hex` is the form `aql` and `asadm` print. `raw` uses the resolved bytes as the digest, for a binary Kafka key from `meta(\"kafka_key\")` or a binary payload from `content()`. Each must yield exactly 20 bytes.").
				Default(digestEncodingBase64),
		)
	}
	return append(fields,
		service.NewStringEnumField(fieldKeyType, "string", "int", "bytes").
			Description("How to interpret the resolved `key`. Aerospike addresses records by a digest of the key, and the digest differs between a string `\"123\"` and an integer `123`. This must match whatever else reads or writes these records.").
			Default("string"),

		service.NewStringEnumField(fieldKeyEncoding, "utf8", "base64", "hex").
			Description("How to decode the interpolated `key` when `key_type` is `bytes`. Interpolation always yields a string, so a genuine binary key must be carried as base64 or hex. `utf8` uses the string's raw bytes and only matches other clients that also stored UTF-8.").
			Default("utf8").
			Advanced(),
	)
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

// digestConfig is the parsed digest field. A static value is decoded once at
// startup; otherwise text is interpolated and decoded per message.
type digestConfig struct {
	text     *service.InterpolatedString
	encoding string
	static   []byte
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
		// Lookup does not declare digest_encoding.
		if conf.Contains(fieldDigestEncoding) {
			enc, err := conf.FieldString(fieldDigestEncoding)
			if err != nil {
				return nil, err
			}
			if enc != digestEncodingBase64 {
				return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is set", fieldDigestEncoding, enc, fieldDigest)
			}
		}
	} else {
		if k.KeyType != "string" {
			return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is set", fieldKeyType, k.KeyType, fieldKey)
		}
		if k.KeyEncoding != "" && k.KeyEncoding != "utf8" {
			return nil, fmt.Errorf("field '%v' is %q, which only applies when '%v' is set", fieldKeyEncoding, k.KeyEncoding, fieldKey)
		}
		if k.Digest, err = parseDigest(conf); err != nil {
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
	namespace   *service.MessageBatchInterpolationExecutor
	set         *service.MessageBatchInterpolationExecutor
	key         *service.MessageBatchInterpolationExecutor
	keyType     string
	keyEncoding string
	digest      *digestConfig
	digestText  *service.MessageBatchInterpolationExecutor
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
		r.digest = k.Digest
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
	if r.digest != nil {
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

func (r *keyResolver) resolveDigest(index int, namespace, setName string) (*as.Key, error) {
	raw := r.digest.static
	if raw == nil {
		b, err := r.digestText.TryBytes(index)
		if err != nil {
			return nil, fmt.Errorf("interpolating '%v': %w", fieldDigest, err)
		}
		if raw, err = decodeDigest(b, r.digest.encoding); err != nil {
			return nil, err
		}
	}
	// A nil user key is stored as Aerospike null and is not sent, even when
	// send_key is set. The digest is the record address.
	key, asErr := as.NewKeyWithDigest(namespace, setName, nil, raw)
	if asErr != nil {
		return nil, fmt.Errorf("building record digest: %w", asErr)
	}
	return key, nil
}

// parseDigest reads the digest fields of the output spec. A static value is
// decoded here so a malformed literal fails at startup.
func parseDigest(conf *service.ParsedConfig) (*digestConfig, error) {
	text, err := conf.FieldInterpolatedString(fieldDigest)
	if err != nil {
		return nil, err
	}
	encoding, err := conf.FieldString(fieldDigestEncoding)
	if err != nil {
		return nil, err
	}
	d := &digestConfig{text: text, encoding: encoding}
	if lit, ok := text.Static(); ok {
		if d.static, err = decodeDigest([]byte(lit), encoding); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// decodeDigest turns a resolved digest into the 20 bytes Aerospike addresses a
// record by. The encoding is explicit because a 20-byte string can also be
// valid Base64 or plain text, and guessing would address the wrong record.
func decodeDigest(b []byte, encoding string) ([]byte, error) {
	// A missing interpolated field stringifies to "null".
	if len(b) == 0 || string(b) == "null" {
		return nil, fmt.Errorf("field '%v' resolved to %q, which is not a usable digest; the source field is probably missing from this message", fieldDigest, b)
	}

	var decoded []byte
	switch encoding {
	case digestEncodingRaw:
		decoded = b
	case digestEncodingHex:
		decoded = make([]byte, hex.DecodedLen(len(b)))
		if _, err := hex.Decode(decoded, b); err != nil {
			return nil, fmt.Errorf("field '%v' is not valid hex as required by '%v': %w", fieldDigest, fieldDigestEncoding, err)
		}
	case digestEncodingBase64:
		decoded = make([]byte, base64.StdEncoding.DecodedLen(len(b)))
		n, err := base64.StdEncoding.Decode(decoded, b)
		if err != nil {
			return nil, fmt.Errorf("field '%v' is not standard Base64 as required by '%v': %w", fieldDigest, fieldDigestEncoding, err)
		}
		decoded = decoded[:n]
	default:
		return nil, fmt.Errorf("field '%v': unknown encoding %q", fieldDigestEncoding, encoding)
	}

	if len(decoded) != digestLen {
		return nil, fmt.Errorf("field '%v' decoded to %d bytes with '%v' %q, which is not the Aerospike digest length of %d", fieldDigest, len(decoded), fieldDigestEncoding, encoding, digestLen)
	}
	return decoded, nil
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
