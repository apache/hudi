/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * Kinesis Source Configs for Hudi Streamer.
 */
@Immutable
@ConfigClassProperty(name = "Kinesis Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of Kinesis source in Hudi Streamer.")
public class KinesisSourceConfig extends HoodieConfig {

  public static final String KINESIS_CHECKPOINT_TYPE_STRING = "string";

  private static final String PREFIX = STREAMER_CONFIG_PREFIX + "source.kinesis.";
  private static final String OLD_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "source.kinesis.";

  public static final ConfigProperty<String> KINESIS_STREAM_NAME = ConfigProperty
      .key(PREFIX + "stream.name")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "stream.name")
      .withDocumentation("Kinesis Data Streams stream name.");

  public static final ConfigProperty<String> KINESIS_REGION = ConfigProperty
      .key(PREFIX + "region")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "region")
      .withDocumentation("AWS region for the Kinesis stream (e.g., us-east-1).");

  public static final ConfigProperty<String> KINESIS_ENDPOINT_URL = ConfigProperty
      .key(PREFIX + "endpoint.url")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "endpoint.url")
      .markAdvanced()
      .withDocumentation("Custom endpoint URL for Kinesis (e.g., for localstack). "
          + "If not set, uses the default AWS endpoint for the region.");

  public static final ConfigProperty<Long> MAX_EVENTS_FROM_KINESIS_SOURCE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "kinesis.source.maxEvents")
      .defaultValue(5000000L)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "kinesis.source.maxEvents")
      .markAdvanced()
      .withDocumentation("Maximum number of records obtained in each batch from Kinesis.");

  public static final ConfigProperty<Long> KINESIS_SOURCE_MIN_PARTITIONS = ConfigProperty
      .key(PREFIX + "minPartitions")
      .defaultValue(0L)
      .withAlternatives(OLD_PREFIX + "minPartitions")
      .markAdvanced()
      .withDocumentation("Desired minimum number of Spark partitions when reading from Kinesis. "
          + "By default, Hudi has a 1-1 mapping of Kinesis shards to Spark partitions. "
          + "If set to a value greater than the number of shards, the result RDD will be repartitioned "
          + "to increase downstream parallelism. Use 0 for 1-1 mapping.");

  public static final ConfigProperty<Boolean> KINESIS_APPEND_OFFSETS = ConfigProperty
      .key(PREFIX + "append.offsets")
      .defaultValue(false)
      .withAlternatives(OLD_PREFIX + "append.offsets")
      .markAdvanced()
      .withDocumentation("When enabled, appends Kinesis metadata (sequence number, shard id, arrival timestamp, partition key) to records.");

  public static final ConfigProperty<Boolean> ENABLE_FAIL_ON_DATA_LOSS = ConfigProperty
      .key(PREFIX + "enable.failOnDataLoss")
      .defaultValue(false)
      .withAlternatives(OLD_PREFIX + "enable.failOnDataLoss")
      .markAdvanced()
      .withDocumentation("Fail when checkpoint references an expired shard instead of seeking to TRIM_HORIZON.");

  public static final ConfigProperty<String> KINESIS_STARTING_POSITION = ConfigProperty
      .key(PREFIX + "starting.position")
      .defaultValue("LATEST")
      .withAlternatives(OLD_PREFIX + "starting.position")
      .markAdvanced()
      .withDocumentation("Starting position when no checkpoint exists. TRIM_HORIZON (or EARLIEST), or LATEST. Default: LATEST.");

  public static final ConfigProperty<Integer> KINESIS_GET_RECORDS_MAX_RECORDS = ConfigProperty
      .key(PREFIX + "getRecords.maxRecords")
      .defaultValue(10000)
      .withAlternatives(OLD_PREFIX + "getRecords.maxRecords")
      .markAdvanced()
      .withDocumentation("Maximum number of records to fetch per GetRecords API call. Kinesis limit is 10000.");

  public static final ConfigProperty<Long> KINESIS_GET_RECORDS_INTERVAL_MS = ConfigProperty
      .key(PREFIX + "getRecords.intervalMs")
      .defaultValue(200L)
      .withAlternatives(OLD_PREFIX + "getRecords.intervalMs")
      .markAdvanced()
      .withDocumentation("Minimum interval in ms between two GetRecords API calls per shard.");

  /**
   * Kinesis starting position strategies.
   */
  public enum KinesisStartingPosition {
    /** Start from the oldest record (TRIM_HORIZON). */
    TRIM_HORIZON,
    /** Alias for TRIM_HORIZON. */
    EARLIEST,
    /** Start from the newest record (LATEST). */
    LATEST
  }
}
