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

package org.apache.hudi.utilities.callback.pulsar;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import static org.apache.hudi.config.HoodieWriteCommitCallbackConfig.CALLBACK_PREFIX;

/**
 * pulsar write callback related config.
 */
@ConfigClassProperty(name = "Write commit pulsar callback configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Controls notifications sent to pulsar, on events happening to a hudi table.")
public class HoodieWriteCommitPulsarCallbackConfig extends HoodieConfig {

  public static final ConfigProperty<String> BROKER_SERVICE_URL = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.broker.service.url")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("Server's url of pulsar cluster, to be used for publishing commit metadata.");

  public static final ConfigProperty<String> TOPIC = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.topic")
      .noDefaultValue()
      .sinceVersion("0.11.0")
      .withDocumentation("pulsar topic name to publish timeline activity into.");

  public static final ConfigProperty<String> PRODUCER_ROUTE_MODE = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.producer.route-mode")
      .defaultValue("RoundRobinPartition")
      .sinceVersion("0.11.0")
      .withDocumentation("Message routing logic for producers on partitioned topics.");

  public static final ConfigProperty<Integer> PRODUCER_PENDING_QUEUE_SIZE = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.producer.pending-queue-size")
      .defaultValue(1000)
      .sinceVersion("0.11.0")
      .withDocumentation("The maximum size of a queue holding pending messages.");

  public static final ConfigProperty<Integer> PRODUCER_PENDING_SIZE = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.producer.pending-total-size")
      .defaultValue(50000)
      .sinceVersion("0.11.0")
      .withDocumentation("The maximum number of pending messages across partitions.");

  public static final ConfigProperty<Boolean> PRODUCER_BLOCK_QUEUE_FULL = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.producer.block-if-queue-full")
      .defaultValue(true)
      .sinceVersion("0.11.0")
      .withDocumentation("When the queue is full, the method is blocked "
          + "instead of an exception is thrown.");

  public static final ConfigProperty<String> PRODUCER_SEND_TIMEOUT = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.producer.send-timeout")
      .defaultValue("30s")
      .sinceVersion("0.11.0")
      .withDocumentation("The timeout in each sending to pulsar.");

  public static final ConfigProperty<String> OPERATION_TIMEOUT = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.operation-timeout")
      .defaultValue("30s")
      .sinceVersion("0.11.0")
      .withDocumentation("Duration of waiting for completing an operation.");

  public static final ConfigProperty<String> CONNECTION_TIMEOUT = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.connection-timeout")
      .defaultValue("10s")
      .sinceVersion("0.11.0")
      .withDocumentation("Duration of waiting for a connection to a "
          + "broker to be established.");

  public static final ConfigProperty<String> REQUEST_TIMEOUT = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.request-timeout")
      .defaultValue("60s")
      .sinceVersion("0.11.0")
      .withDocumentation("Duration of waiting for completing a request.");

  public static final ConfigProperty<String> KEEPALIVE_INTERVAL = ConfigProperty
      .key(CALLBACK_PREFIX + "pulsar.keepalive-interval")
      .defaultValue("30s")
      .sinceVersion("0.11.0")
      .withDocumentation("Duration of keeping alive interval for each "
          + "client broker connection.");

  /**
   * Set default value for {@link HoodieWriteCommitPulsarCallbackConfig} if needed.
   */
  public static void setCallbackPulsarConfigIfNeeded(HoodieConfig config) {
    config.setDefaultValue(PRODUCER_ROUTE_MODE);
    config.setDefaultValue(OPERATION_TIMEOUT);
    config.setDefaultValue(CONNECTION_TIMEOUT);
    config.setDefaultValue(REQUEST_TIMEOUT);
    config.setDefaultValue(KEEPALIVE_INTERVAL);
    config.setDefaultValue(PRODUCER_SEND_TIMEOUT);
    config.setDefaultValue(PRODUCER_PENDING_QUEUE_SIZE);
    config.setDefaultValue(PRODUCER_PENDING_SIZE);
    config.setDefaultValue(PRODUCER_BLOCK_QUEUE_FULL);
  }
}
