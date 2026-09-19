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
)

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
