/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * Configurations controlling the Debezium CDC transformers (e.g.
 * {@code PostgresDebeziumTransformer}, {@code MysqlDebeziumTransformer}).
 */
@Immutable
@ConfigClassProperty(name = "Debezium Transformer Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.NONE,
    description = "Configurations controlling the Debezium CDC transformers that flatten "
        + "Debezium change-event envelopes into Hudi rows.")
public class DebeziumTransformerConfig extends HoodieConfig {

  private static final String PREFIX = STREAMER_CONFIG_PREFIX + "transformer.debezium.";
  private static final String OLD_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "transformer.debezium.";

  public static final ConfigProperty<Boolean> ENABLE_NESTED_FIELDS = ConfigProperty
      .key(PREFIX + "nested.fields.enable")
      .defaultValue(false)
      .withAlternatives(OLD_PREFIX + "nested.fields.enable")
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("When enabled, the Debezium transformer packs the CDC metadata columns "
          + "under a single `_debezium_metadata` struct column instead of flattening them to the "
          + "root level. The change-operation-type column and the log-position column (e.g. the "
          + "Postgres LSN) are kept at the root level so that payload ordering keeps working. When "
          + "this property is not set explicitly, the per-database transformer default is used "
          + "(PostgresDebeziumTransformer defaults to true).");

  public static final ConfigProperty<Boolean> SCHEMA_AS_NULLABLE = ConfigProperty
      .key(PREFIX + "schema.nullable.enable")
      .defaultValue(true)
      .withAlternatives(OLD_PREFIX + "schema.nullable.enable")
      .markAdvanced()
      .sinceVersion("1.3.0")
      .withDocumentation("When enabled, all columns in the transformed Debezium schema are marked "
          + "as nullable. This keeps the output schema compatible with the nullable columns that "
          + "Debezium change events produce.");
}
