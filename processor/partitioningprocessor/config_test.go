// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package partitioningprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "no keys",
			cfg:     Config{},
			wantErr: "at least one partition key must be specified",
		},
		{
			name: "empty name",
			cfg: Config{Keys: []PartitionKeyConfig{
				{Name: "", Value: "resource.attributes[\"foo\"]"},
			}},
			wantErr: "keys[0]: name must not be empty",
		},
		{
			name: "empty value",
			cfg: Config{Keys: []PartitionKeyConfig{
				{Name: "tenant", Value: ""},
			}},
			wantErr: "keys[0]: value must not be empty",
		},
		{
			name: "duplicate name",
			cfg: Config{Keys: []PartitionKeyConfig{
				{Name: "tenant", Value: "resource.attributes[\"a\"]"},
				{Name: "tenant", Value: "resource.attributes[\"b\"]"},
			}},
			wantErr: `keys[1]: duplicate key name "tenant"`,
		},
		{
			name: "valid",
			cfg: Config{Keys: []PartitionKeyConfig{
				{Name: "tenant_id", Value: "resource.attributes[\"tenant.id\"]"},
				{Name: "service", Value: "resource.attributes[\"service.name\"]"},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
