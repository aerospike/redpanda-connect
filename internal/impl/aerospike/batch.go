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

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	FieldConcurrentNodes = "concurrent_nodes"
	FieldSocketTimeout   = "socket_timeout"
	FieldTotalTimeout    = "total_timeout"
	FieldMaxRetries      = "max_retries"
)

// NonNegativeLint rejects negative integers in config fields that must be >= 0.
const NonNegativeLint = `root = if this < 0 { [ "must not be negative" ] }`

// BatchPolicyFields returns the Aerospike batch-command fields shared by the
// output and the lookup processor. Reads default to two retries.
func BatchPolicyFields() []*service.ConfigField {
	return BatchPolicyFieldsWithRetries(2)
}

// BatchPolicyFieldsWithRetries is BatchPolicyFields with an explicit retry
// default. Writes should pass 0: create_only is not safe to repeat, and a
// retry after an uncertain timeout can insert a second record.
func BatchPolicyFieldsWithRetries(maxRetries int) []*service.ConfigField {
	retryDesc := "Client-side retries per batch command."
	if maxRetries == 0 {
		retryDesc += " Defaults to 0 because some write operations (`create_only`, counters) are not idempotent. Raise this only for `replace`/`write` that you have made safe to repeat, for example with fencing."
	}

	return []*service.ConfigField{
		service.NewIntField(FieldConcurrentNodes).
			Description("How many cluster nodes to issue batch sub-requests to concurrently. `0` means all of them in parallel.").
			Default(0).
			LintRule(NonNegativeLint).
			Advanced(),

		service.NewDurationField(FieldSocketTimeout).
			Description("Per-attempt socket timeout for a batch command. Capped by the remaining pipeline context deadline when one is set.").
			Default("1s").
			Advanced(),

		service.NewDurationField(FieldTotalTimeout).
			Description("Total timeout for a batch command including retries. Capped by the remaining pipeline context deadline when one is set, so a shutdown cannot wait for the full configured timeout.").
			Default("10s").
			Advanced(),

		service.NewIntField(FieldMaxRetries).
			Description(retryDesc).
			Default(maxRetries).
			LintRule(NonNegativeLint).
			Advanced(),
	}
}

// ParseBatchPolicy reads the fields produced by BatchPolicyFields.
func ParseBatchPolicy(conf *service.ParsedConfig) (*as.BatchPolicy, error) {
	p := as.NewBatchPolicy()

	var err error
	if p.ConcurrentNodes, err = conf.FieldInt(FieldConcurrentNodes); err != nil {
		return nil, err
	}
	if p.ConcurrentNodes < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", FieldConcurrentNodes)
	}
	if p.SocketTimeout, err = conf.FieldDuration(FieldSocketTimeout); err != nil {
		return nil, err
	}
	if p.TotalTimeout, err = conf.FieldDuration(FieldTotalTimeout); err != nil {
		return nil, err
	}
	if p.MaxRetries, err = conf.FieldInt(FieldMaxRetries); err != nil {
		return nil, err
	}
	if p.MaxRetries < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", FieldMaxRetries)
	}
	// Per-key result codes are how individual messages are nacked, so require
	// the server to report on every key rather than short-circuiting the batch.
	p.RespondAllKeys = true
	return p, nil
}
