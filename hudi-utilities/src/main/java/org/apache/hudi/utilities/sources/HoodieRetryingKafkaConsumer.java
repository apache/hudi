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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Retrying client for {@link KafkaConsumer} operations.
 */
public class HoodieRetryingKafkaConsumer extends KafkaConsumer {

  private final long maxRetryIntervalMs;
  private final int maxRetryCount;
  private final long initialRetryIntervalMs;
  private final String retryExceptionsList;

  public HoodieRetryingKafkaConsumer(TypedProperties config, Map<String, Object> kafkaParams) {
    super(kafkaParams);
    this.maxRetryIntervalMs = config.getLong(KafkaSourceConfig.MAX_RETRY_INTERVAL_MS.key(),
        KafkaSourceConfig.MAX_RETRY_INTERVAL_MS.defaultValue());
    this.maxRetryCount = config.getInteger(KafkaSourceConfig.MAX_RETRY_COUNT.key(),
        KafkaSourceConfig.MAX_RETRY_COUNT.defaultValue());
    this.initialRetryIntervalMs = config.getLong(KafkaSourceConfig.INITIAL_RETRY_INTERVAL_MS.key(),
        KafkaSourceConfig.INITIAL_RETRY_INTERVAL_MS.defaultValue());
    this.retryExceptionsList = config.getString(KafkaSourceConfig.RETRY_EXCEPTIONS.key(),
        KafkaSourceConfig.RETRY_EXCEPTIONS.defaultValue());
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection partitions) {
    return new RetryHelper<Map<TopicPartition, Long>, KafkaException>(
        maxRetryIntervalMs, maxRetryCount, initialRetryIntervalMs, retryExceptionsList)
        .start(() -> super.beginningOffsets(partitions));
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection partitions) {
    return new RetryHelper<Map<TopicPartition, Long>, KafkaException>(
        maxRetryIntervalMs, maxRetryCount, initialRetryIntervalMs, retryExceptionsList)
        .start(() -> super.endOffsets(partitions));
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return new RetryHelper<List<PartitionInfo>, KafkaException>(
        maxRetryIntervalMs, maxRetryCount, initialRetryIntervalMs, retryExceptionsList)
        .start(() -> super.partitionsFor(topic));
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map timestampsToSearch) {
    return new RetryHelper<Map<TopicPartition, OffsetAndTimestamp>, KafkaException>(
        maxRetryIntervalMs, maxRetryCount, initialRetryIntervalMs, retryExceptionsList)
        .start(() -> super.offsetsForTimes(timestampsToSearch));
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return new RetryHelper<Map<String, List<PartitionInfo>>, KafkaException>(
        maxRetryIntervalMs, maxRetryCount, initialRetryIntervalMs, retryExceptionsList)
        .start(() -> super.listTopics());
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    return new RetryHelper<OffsetAndMetadata, KafkaException>(
        maxRetryIntervalMs, maxRetryCount, initialRetryIntervalMs, retryExceptionsList)
        .start(() -> super.committed(partition));
  }
}
