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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * Cloud Source Configs
 */
@Immutable
@ConfigClassProperty(name = "Kafka Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of Kafka source in Hudi Streamer.")
public class KafkaSourceConfig extends HoodieConfig {

  public static final String KAFKA_CHECKPOINT_TYPE_STRING = "string";
  public static final String KAFKA_CHECKPOINT_TYPE_TIMESTAMP = "timestamp";
  public static final String KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET = "single_offset";

  private static final String PREFIX = STREAMER_CONFIG_PREFIX + "source.kafka.";
  private static final String OLD_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.";

  public static final ConfigProperty<String> KAFKA_CHECKPOINT_TYPE = ConfigProperty
      .key(PREFIX + "checkpoint.type")
      .defaultValue(KAFKA_CHECKPOINT_TYPE_STRING)
      .withAlternatives(OLD_PREFIX + "checkpoint.type")
      .markAdvanced()
      .withDocumentation("Kafka checkpoint type. Value must be one of the following: "
          + KAFKA_CHECKPOINT_TYPE_STRING + ", " + KAFKA_CHECKPOINT_TYPE_TIMESTAMP + ", " + KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET
          + ". Default type is " + KAFKA_CHECKPOINT_TYPE_STRING + ". "
          + "For type " + KAFKA_CHECKPOINT_TYPE_STRING + ", checkpoint should be provided as: topicName,0:offset0,1:offset1,2:offset2. "
          + "For type " + KAFKA_CHECKPOINT_TYPE_TIMESTAMP + ", checkpoint should be provided as long value of desired timestamp. "
          + "For type " + KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET + ", we assume that topic consists of a single partition, "
          + "so checkpoint should be provided as long value of desired offset.");

  public static final ConfigProperty<String> KAFKA_AVRO_VALUE_DESERIALIZER_CLASS = ConfigProperty
      .key(PREFIX + "value.deserializer.class")
      .defaultValue("io.confluent.kafka.serializers.KafkaAvroDeserializer")
      .withAlternatives(OLD_PREFIX + "value.deserializer.class")
      .markAdvanced()
      .sinceVersion("0.9.0")
      .withDocumentation("This class is used by kafka client to deserialize the records.");

  public static final ConfigProperty<String> KAFKA_VALUE_DESERIALIZER_SCHEMA = ConfigProperty
      .key(PREFIX + "value.deserializer.schema")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "value.deserializer.schema")
      .markAdvanced()
      .withDocumentation("Schema to deserialize the records.");

  @Deprecated
  public static final ConfigProperty<Long> KAFKA_FETCH_PARTITION_TIME_OUT = ConfigProperty
      .key(PREFIX + "fetch_partition.time.out")
      .defaultValue(300 * 1000L)
      .withAlternatives(OLD_PREFIX + "fetch_partition.time.out")
      .markAdvanced()
      .withDocumentation("Time out for fetching partitions. 5min by default");

  public static final ConfigProperty<Boolean> ENABLE_KAFKA_COMMIT_OFFSET = ConfigProperty
      .key(PREFIX + "enable.commit.offset")
      .defaultValue(false)
      .withAlternatives(OLD_PREFIX + "enable.commit.offset")
      .markAdvanced()
      .withDocumentation("Automatically submits offset to kafka.");

  public static final ConfigProperty<Boolean> ENABLE_FAIL_ON_DATA_LOSS = ConfigProperty
      .key(PREFIX + "enable.failOnDataLoss")
      .defaultValue(false)
      .withAlternatives(OLD_PREFIX + "enable.failOnDataLoss")
      .markAdvanced()
      .withDocumentation("Fail when checkpoint goes out of bounds instead of seeking to earliest offsets.");

  public static final ConfigProperty<Long> MAX_EVENTS_FROM_KAFKA_SOURCE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "kafka.source.maxEvents")
      .defaultValue(5000000L)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "kafka.source.maxEvents")
      .markAdvanced()
      .withDocumentation("Maximum number of records obtained in each batch.");

  // the documentation is inspired by the minPartition definition of kafka structured streaming
  public static final ConfigProperty<Long> KAFKA_SOURCE_MIN_PARTITIONS = ConfigProperty
      .key(PREFIX + "minPartitions")
      .defaultValue(0L)
      .withAlternatives(OLD_PREFIX + "minPartitions")
      .markAdvanced()
      .sinceVersion("0.14.0")
          .withDocumentation("Desired minimum number of partitions to read from Kafka. "
              + "By default, Hudi has a 1-1 mapping of topicPartitions to Hudi partitions consuming from Kafka. "
              + "If set this option to a value greater than topicPartitions, "
              + "Hudi will divvy up large Kafka partitions to smaller pieces. "
              + "Please note that this configuration is like a hint: the number of input tasks will be approximately minPartitions. "
              + "It can be less or more depending on rounding errors or Kafka partitions that didn't receive any new data.");

  public static final ConfigProperty<String> KAFKA_TOPIC_NAME = ConfigProperty
      .key(PREFIX + "topic")
      .noDefaultValue()
      .withAlternatives(OLD_PREFIX + "topic")
      .withDocumentation("Kafka topic name.");

  // "auto.offset.reset" is kafka native config param. Do not change the config param name.
  public static final ConfigProperty<KafkaResetOffsetStrategies> KAFKA_AUTO_OFFSET_RESET = ConfigProperty
      .key("auto.offset.reset")
      .defaultValue(KafkaResetOffsetStrategies.LATEST)
      .markAdvanced()
      .withDocumentation("Kafka consumer strategy for reading data.");

  public static final ConfigProperty<String> KAFKA_PROTO_VALUE_DESERIALIZER_CLASS = ConfigProperty
      .key(PREFIX + "proto.value.deserializer.class")
      .defaultValue(ByteArrayDeserializer.class.getName())
      .sinceVersion("0.15.0")
      .withDocumentation("Kafka Proto Payload Deserializer Class");

  public static final ConfigProperty<Long> INITIAL_RETRY_INTERVAL_MS = ConfigProperty
      .key(PREFIX + "retry.initial_interval_ms")
      .defaultValue(100L)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Amount of time (in ms) to wait, before retry to do operations on KafkaConsumer.");

  public static final ConfigProperty<Long> MAX_RETRY_INTERVAL_MS = ConfigProperty
      .key(PREFIX + "retry.max_interval_ms")
      .defaultValue(2000L)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Maximum amount of time (in ms), to wait for next retry.");

  public static final ConfigProperty<Integer> MAX_RETRY_COUNT = ConfigProperty
      .key(PREFIX + "retry.max_count")
      .defaultValue(4)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Maximum number of retry actions to perform, with exponential backoff.");

  public static final ConfigProperty<String> RETRY_EXCEPTIONS = ConfigProperty
      .key(PREFIX + "retry.exceptions")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("The class name of the Exception that needs to be retried, separated by commas. "
          + "Default is empty which means retry all the IOException and RuntimeException from KafkaConsumer");

  public static final ConfigProperty<Long> OFFSET_SKIP_BUFFER_MINUTES = ConfigProperty
      .key(PREFIX + "offset.skip.buffer.minutes")
      .defaultValue(-1L)
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Time in minutes to move forward from retention time for reading offsets "
          + "when the checkpoints are not available or out-of-bound to avoid potential "
          + "OffsetOutOfRange exception, as it is possible that the earliest Kafka offsets may "
          + "expire soon while the job is progressing.");

  public static final ConfigProperty<String> IGNORE_PREFIX_CONFIG_LIST = ConfigProperty
      .key(PREFIX + "ignore.prefix.config.list")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("1.1.0")
      .withDocumentation("Comma-separated list of prefixes for config keys that should be dropped from the configs when passed to the Kafka consumer.");

  /**
   * Kafka reset offset strategies.
   */
  public enum KafkaResetOffsetStrategies {
    LATEST, EARLIEST, GROUP
  }
}
