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

package partitioningprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/partitioningprocessor"

import (
	"errors"
	"fmt"
)

// Config is the configuration for the partitioning processor.
type Config struct {
	Keys []PartitionKeyConfig `mapstructure:"keys"`
}

// PartitionKeyConfig defines a single partition key.
type PartitionKeyConfig struct {
	Name  string `mapstructure:"name"`
	Value string `mapstructure:"value"`
}

// Validate validates the configuration.
func (cfg *Config) Validate() error {
	if len(cfg.Keys) == 0 {
		return errors.New("at least one partition key must be specified")
	}
	seen := make(map[string]struct{}, len(cfg.Keys))
	for i, key := range cfg.Keys {
		if key.Name == "" {
			return fmt.Errorf("keys[%d]: name must not be empty", i)
		}
		if key.Value == "" {
			return fmt.Errorf("keys[%d]: value must not be empty", i)
		}
		if _, ok := seen[key.Name]; ok {
			return fmt.Errorf("keys[%d]: duplicate key name %q", i, key.Name)
		}
		seen[key.Name] = struct{}{}
	}
	return nil
}
