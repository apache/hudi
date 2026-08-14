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

  private static final String PREFIX = STREAMER_CONFIG_PREFIX + "source.kinesis.";

  public static final ConfigProperty<String> KINESIS_STREAM_NAME = ConfigProperty
      .key(PREFIX + "stream.name")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .withDocumentation("Kinesis Data Streams stream name.");

  public static final ConfigProperty<String> KINESIS_REGION = ConfigProperty
      .key(PREFIX + "region")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("AWS region for the Kinesis stream (e.g., us-east-1).");

  public static final ConfigProperty<String> KINESIS_ENDPOINT_URL = ConfigProperty
      .key(PREFIX + "endpoint.url")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Custom endpoint URL for Kinesis (e.g., for localstack). "
          + "If not set, uses the default AWS endpoint for the region.");

  public static final ConfigProperty<String> KINESIS_ACCESS_KEY = ConfigProperty
      .key(PREFIX + "access.key")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("AWS access key for Kinesis. Used when connecting to custom endpoints (e.g., LocalStack). "
          + "If not set with endpoint, uses the default AWS credential chain.");

  public static final ConfigProperty<String> KINESIS_SECRET_KEY = ConfigProperty
      .key(PREFIX + "secret.key")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("AWS secret key for Kinesis. Used when connecting to custom endpoints (e.g., LocalStack). "
          + "If not set with endpoint, uses the default AWS credential chain.");

  public static final ConfigProperty<Long> MAX_EVENTS_FROM_KINESIS_SOURCE = ConfigProperty
      .key(PREFIX + "max.events")
      .defaultValue(5000000L)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Maximum number of records obtained in each batch from Kinesis.");

  public static final ConfigProperty<Long> KINESIS_SOURCE_MANUAL_PARTITIONS = ConfigProperty
      .key(PREFIX + "partitions")
      .defaultValue(0L)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Desired number of Spark partitions when reading from Kinesis. "
          + "By default, Hudi has a 1-1 mapping of Kinesis shards to Spark partitions. "
          + "If set to a value greater than 0, the result RDD will be repartitioned "
          + "to increase/decrease downstream parallelism. Use 0 for 1-1 mapping.");

  public static final ConfigProperty<Boolean> KINESIS_APPEND_OFFSETS = ConfigProperty
      .key(PREFIX + "append.offsets")
      .defaultValue(false)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("When enabled, appends Kinesis metadata (sequence number, shard id, arrival timestamp, partition key) to records.");

  public static final ConfigProperty<Boolean> KINESIS_ENABLE_DEAGGREGATION = ConfigProperty
      .key(PREFIX + "enable.deaggregation")
      .defaultValue(true)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("When enabled, de-aggregates records produced by Kinesis Producer Library (KPL). "
          + "Non-aggregated records pass through unchanged. Set to false if producers do not use KPL.");

  public static final ConfigProperty<Boolean> ENABLE_FAIL_ON_DATA_LOSS = ConfigProperty
      .key(PREFIX + "fail.on.data.loss.enable")
      .defaultValue(false)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Fail when checkpoint references an expired shard which has not been fully consumed.");

  public static final ConfigProperty<String> KINESIS_STARTING_POSITION = ConfigProperty
      .key(PREFIX + "starting.position")
      .defaultValue("LATEST")
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Starting position when no checkpoint exists. EARLIEST or LATEST. Default: LATEST.");

  public static final ConfigProperty<Integer> KINESIS_MAX_RECORDS_PER_REQUEST = ConfigProperty
      .key(PREFIX + "max.records.per.request")
      .defaultValue(10000)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Maximum number of records to fetch per GetRecords API call. Kinesis limit is 10000.");

  public static final ConfigProperty<Long> KINESIS_GET_RECORDS_INTERVAL_MS = ConfigProperty
      .key(PREFIX + "get.records.interval.ms")
      .defaultValue(200L)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Minimum interval in ms between two GetRecords API calls per shard.");

  public static final ConfigProperty<Long> KINESIS_RETRY_INITIAL_INTERVAL_MS = ConfigProperty
      .key(PREFIX + "retry.initial.interval.ms")
      .defaultValue(1000L)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Initial backoff in ms when Kinesis returns ProvisionedThroughputExceededException. "
          + "Backoff doubles each retry up to retry.max.interval.ms.");

  public static final ConfigProperty<Long> KINESIS_RETRY_MAX_INTERVAL_MS = ConfigProperty
      .key(PREFIX + "retry.max.interval.ms")
      .defaultValue(10000L)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Maximum backoff in ms between retries for throughput exceeded.");

  public static final ConfigProperty<Long> KINESIS_THROTTLE_TIMEOUT_MS = ConfigProperty
      .key(PREFIX + "retry.throttle.timeout.ms")
      .defaultValue(600000L)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("Maximum time in ms to keep retrying GetRecords calls after ProvisionedThroughputExceededException "
          + "with no successful fetch. When exceeded, the read fails. Default: 600000 (10 minutes).");

  public static final ConfigProperty<Boolean> KINESIS_PERSIST_FETCH_RDD = ConfigProperty
      .key(PREFIX + "persist.fetch.rdd")
      .defaultValue(true)
      .sinceVersion("1.2.0")
      .markAdvanced()
      .withDocumentation("When enabled, the fetch RDD is persisted (MEMORY_AND_DISK) so it can be used "
          + "both for record RDD and checkpoint summaries without recomputation. When false, the fetch "
          + "is computed twice (once for checkpoint, once when the record RDD is consumed), which can "
          + "cause duplicate records to be written. Set to true for correct, duplicate-free ingestion.");

  /**
   * Kinesis starting position strategies.
   */
  public enum KinesisStartingPositionStrategy {
    /** Start from the oldest record (TRIM_HORIZON). */
    EARLIEST,
    /** Start from the newest record (LATEST). */
    LATEST;

    public static KinesisStartingPositionStrategy fromString(String value) {
      String normalized = value.toUpperCase().replace("TRIM_HORIZON", "EARLIEST");
      return valueOf(normalized);
    }
  }
}
