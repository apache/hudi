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

import java.util.Properties;

import static org.apache.hudi.common.config.DefaultHoodieConfig.setDefaultOnCondition;
import static org.apache.hudi.config.HoodieWriteCommitCallbackConfig.CALLBACK_PREFIX;

/**
 * Kafka write callback related config.
 */
public class HoodieWriteCommitKafkaCallbackConfig {

  public static final String CALLBACK_KAFKA_BOOTSTRAP_SERVERS = CALLBACK_PREFIX + "kafka.bootstrap.servers";
  public static final String CALLBACK_KAFKA_TOPIC = CALLBACK_PREFIX + "kafka.topic";
  public static final String CALLBACK_KAFKA_PARTITION = CALLBACK_PREFIX + "kafka.partition";
  public static final String CALLBACK_KAFKA_ACKS = CALLBACK_PREFIX + "kafka.acks";
  public static final String DEFAULT_CALLBACK_KAFKA_ACKS = "all";
  public static final String CALLBACK_KAFKA_RETRIES = CALLBACK_PREFIX + "kafka.retries";
  public static final int DEFAULT_CALLBACK_KAFKA_RETRIES = 3;

  /**
   * Set default value for {@link HoodieWriteCommitKafkaCallbackConfig} if needed.
   */
  public static void setCallbackKafkaConfigIfNeeded(Properties props) {
    setDefaultOnCondition(props, !props.containsKey(CALLBACK_KAFKA_ACKS), CALLBACK_KAFKA_ACKS,
        DEFAULT_CALLBACK_KAFKA_ACKS);
    setDefaultOnCondition(props, !props.containsKey(CALLBACK_KAFKA_RETRIES), CALLBACK_KAFKA_RETRIES,
        String.valueOf(DEFAULT_CALLBACK_KAFKA_RETRIES));
  }

}
