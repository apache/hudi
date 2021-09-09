/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;

/**
 * Flink sql indexing related config.
 */
@ConfigClassProperty(name = "Flink Index Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Configurations that control flink Index behavior on Hudi tables are listed below.")
public class FlinkIndexOptions {

  public static final ConfigOption<Boolean> INDEX_BOOTSTRAP_ENABLED = ConfigOptions
      .key("index.bootstrap.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to bootstrap the index state from existing hoodie table, default false.");

  public static final ConfigOption<Double> INDEX_STATE_TTL = ConfigOptions
      .key("index.state.ttl")
      .doubleType()
      .defaultValue(1.5D)
      .withDescription("Index state ttl in days, default 1.5 day.");

  public static final ConfigOption<Boolean> INDEX_GLOBAL_ENABLED = ConfigOptions
      .key("index.global.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Whether to update index for the old partition path\n"
          + "if same key record with different partition path came in, default false.");

  public static final ConfigOption<String> INDEX_PARTITION_REGEX = ConfigOptions
      .key("index.partition.regex")
      .stringType()
      .defaultValue(".*")
      .withDescription("Whether to load partitions in state if partition path matching， default *.");

}
