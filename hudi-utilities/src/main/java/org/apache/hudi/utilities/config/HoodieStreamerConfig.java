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
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;

/**
 * Hudi Streamer related config.
 */
@Immutable
@ConfigClassProperty(name = "Hudi Streamer Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    areCommonConfigs = true,
    description = "")
public class HoodieStreamerConfig extends HoodieConfig {
  public static final String INGESTION_PREFIX = STREAMER_CONFIG_PREFIX + "ingestion.";
  @Deprecated
  public static final String OLD_INGESTION_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "ingestion.";

  public static final ConfigProperty<String> CHECKPOINT_PROVIDER_PATH = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "checkpoint.provider.path")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "checkpoint.provider.path")
      .markAdvanced()
      .withDocumentation("The path for providing the checkpoints.");

  // TODO: consolidate all checkpointing configs into HoodieCheckpointStrategy (HUDI-5906)
  public static final ConfigProperty<Boolean> CHECKPOINT_FORCE_SKIP = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "checkpoint.force.skip")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "checkpoint.force.skip")
      .markAdvanced()
      .withDocumentation("Config to force to skip saving checkpoint in the commit metadata."
          + "It is typically used in one-time backfill scenarios, where checkpoints are not "
          + "to be persisted.");

  public static final ConfigProperty<String> KAFKA_TOPIC = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.kafka.topic")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.topic")
      .withDocumentation("Kafka topic name. The config is specific to HoodieMultiTableStreamer");

  public static final ConfigProperty<String> KAFKA_APPEND_OFFSETS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.kafka.append.offsets")
      .defaultValue("false")
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.append.offsets")
      .markAdvanced()
      .withDocumentation("When enabled, appends kafka offset info like source offset(_hoodie_kafka_source_offset), "
          + "partition (_hoodie_kafka_source_partition) and timestamp (_hoodie_kafka_source_timestamp) to the records. "
          + "By default its disabled and no kafka offsets are added");

  public static final ConfigProperty<Boolean> SANITIZE_SCHEMA_FIELD_NAMES = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.sanitize.invalid.schema.field.names")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.sanitize.invalid.schema.field.names")
      .markAdvanced()
      .withDocumentation("Sanitizes names of invalid schema fields both in the data read from source and also in the schema "
          + "Replaces invalid characters with hoodie.streamer.source.sanitize.invalid.char.mask. Invalid characters are by "
          + "goes by avro naming convention (https://avro.apache.org/docs/current/spec.html#names).");

  public static final ConfigProperty<String> SCHEMA_FIELD_NAME_INVALID_CHAR_MASK = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.sanitize.invalid.char.mask")
      .defaultValue("__")
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.sanitize.invalid.char.mask")
      .markAdvanced()
      .withDocumentation("Defines the character sequence that replaces invalid characters in schema field names if "
          + "hoodie.streamer.source.sanitize.invalid.schema.field.names is enabled.");

  public static final ConfigProperty<String> TABLES_TO_BE_INGESTED = ConfigProperty
      .key(INGESTION_PREFIX + "tablesToBeIngested")
      .noDefaultValue()
      .withAlternatives(OLD_INGESTION_PREFIX + "tablesToBeIngested")
      .markAdvanced()
      .withDocumentation("Comma separated names of tables to be ingested in the format <database>.<table>, for example db1.table1,db1.table2");

  public static final ConfigProperty<String> TARGET_BASE_PATH = ConfigProperty
      .key(INGESTION_PREFIX + "targetBasePath")
      .defaultValue("")
      .withAlternatives(OLD_INGESTION_PREFIX + "targetBasePath")
      .markAdvanced()
      .withDocumentation("The path to which a particular table is ingested. The config is specific to HoodieMultiTableStreamer"
          + " and overrides path determined using option `--base-path-prefix` for a table. This config is ignored for a single"
          + " table streamer");

  public static final ConfigProperty<String> TRANSFORMER_CLASS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "transformer.class")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "transformer.class")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Names of transformer classes to apply. The config is specific to HoodieMultiTableStreamer.");
  public static final ConfigProperty<Boolean> SAMPLE_WRITES_ENABLED = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "sample.writes.enabled")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "sample.writes.enabled")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Set this to true to sample from the first batch of records and write to the auxiliary path, before writing to the table."
          + "The sampled records are used to calculate the average record size. The relevant write client will have `" + COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key()
          + "` being overwritten by the calculated result.");
  public static final ConfigProperty<Integer> SAMPLE_WRITES_SIZE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "sample.writes.size")
      .defaultValue(5000)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "sample.writes.size")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Number of records to sample from the first write. To improve the estimation's accuracy, "
          + "for smaller or more compressable record size, set the sample size bigger. For bigger or less compressable record size, set smaller.");

  public static final ConfigProperty<Boolean> ROW_THROW_EXPLICIT_EXCEPTIONS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "row.throw.explicit.exceptions")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("When enabled, the dataframe generated from reading source data is wrapped with an exception handler to explicitly surface exceptions.");
}
