/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.callback.kafka;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import static org.apache.hudi.config.HoodieWriteCommitCallbackConfig.CALLBACK_PREFIX;

/**
 * Kafka write callback related config.
 */
@ConfigClassProperty(name = "Write commit Kafka callback configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.COMMIT_CALLBACK,
    description = "Controls notifications sent to Kafka, on events happening to a hudi table.")
public class HoodieWriteCommitKafkaCallbackConfig extends HoodieConfig {

  public static final ConfigProperty<String> BOOTSTRAP_SERVERS = ConfigProperty
      .key(CALLBACK_PREFIX + "kafka.bootstrap.servers")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Bootstrap servers of kafka cluster, to be used for publishing commit metadata.");

  public static final ConfigProperty<String> TOPIC = ConfigProperty
      .key(CALLBACK_PREFIX + "kafka.topic")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Kafka topic name to publish timeline activity into.");

  public static final ConfigProperty<String> PARTITION = ConfigProperty
      .key(CALLBACK_PREFIX + "kafka.partition")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("It may be desirable to serialize all changes into a single Kafka partition "
          + " for providing strict ordering. By default, Kafka messages are keyed by table name, which "
          + " guarantees ordering at the table level, but not globally (or when new partitions are added)");

  public static final ConfigProperty<String> ACKS = ConfigProperty
      .key(CALLBACK_PREFIX + "kafka.acks")
      .defaultValue("all")
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("kafka acks level, all by default to ensure strong durability.");

  public static final ConfigProperty<Integer> RETRIES = ConfigProperty
      .key(CALLBACK_PREFIX + "kafka.retries")
      .defaultValue(3)
      .markAdvanced()
      .sinceVersion("0.7.0")
      .withDocumentation("Times to retry the produce. 3 by default");

  /**
   * Set default value for {@link HoodieWriteCommitKafkaCallbackConfig} if needed.
   */
  public static void setCallbackKafkaConfigIfNeeded(HoodieConfig config) {
    config.setDefaultValue(ACKS);
    config.setDefaultValue(RETRIES);
  }

  /**
   * @deprecated Use {@link #BOOTSTRAP_SERVERS} and its methods.
   */
  @Deprecated
  public static final String CALLBACK_KAFKA_BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS.key();
  /**
   * @deprecated Use {@link #TOPIC} and its methods.
   */
  @Deprecated
  public static final String CALLBACK_KAFKA_TOPIC = TOPIC.key();
  /**
   * @deprecated Use {@link #PARTITION} and its methods.
   */
  @Deprecated
  public static final String CALLBACK_KAFKA_PARTITION = PARTITION.key();
  /**
   * @deprecated Use {@link #ACKS} and its methods.
   */
  @Deprecated
  public static final String CALLBACK_KAFKA_ACKS = ACKS.key();
  /**
   * @deprecated Use {@link #ACKS} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_CALLBACK_KAFKA_ACKS = ACKS.defaultValue();
  /**
   * @deprecated Use {@link #RETRIES} and its methods.
   */
  @Deprecated
  public static final String CALLBACK_KAFKA_RETRIES = RETRIES.key();
  /**
   * @deprecated Use {@link #RETRIES} and its methods.
   */
  @Deprecated
  public static final int DEFAULT_CALLBACK_KAFKA_RETRIES = RETRIES.defaultValue();
}
