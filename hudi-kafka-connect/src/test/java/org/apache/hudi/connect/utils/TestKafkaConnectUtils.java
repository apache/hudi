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

package org.apache.hudi.connect.utils;

import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestKafkaConnectUtils {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String TOPIC_NAME = "test-topic";

  @Test
  void closesAdminClientAfterFetchingLatestNumberOfPartitions() {
    AdminClient adminClient = mock(AdminClient.class);
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    TopicDescription topicDescription = mock(TopicDescription.class);
    TopicPartitionInfo partitionInfo = mock(TopicPartitionInfo.class);

    try (MockedStatic<AdminClient> mockedAdminClient = mockStatic(AdminClient.class)) {
      mockedAdminClient.when(() -> AdminClient.create(any(Properties.class))).thenReturn(adminClient);
      when(adminClient.describeTopics(Collections.singletonList(TOPIC_NAME))).thenReturn(describeTopicsResult);
      when(describeTopicsResult.values())
          .thenReturn(Collections.singletonMap(TOPIC_NAME, KafkaFuture.completedFuture(topicDescription)));
      when(topicDescription.partitions()).thenReturn(Collections.nCopies(3, partitionInfo));

      assertEquals(3, KafkaConnectUtils.getLatestNumPartitions(BOOTSTRAP_SERVERS, TOPIC_NAME));
      verify(adminClient).close();
    }
  }

  @Test
  void closesAdminClientWhenFetchingLatestNumberOfPartitionsFails() {
    AdminClient adminClient = mock(AdminClient.class);

    try (MockedStatic<AdminClient> mockedAdminClient = mockStatic(AdminClient.class)) {
      mockedAdminClient.when(() -> AdminClient.create(any(Properties.class))).thenReturn(adminClient);
      when(adminClient.describeTopics(Collections.singletonList(TOPIC_NAME)))
          .thenThrow(new IllegalStateException("Failed to describe topic"));

      assertThrows(
          HoodieException.class,
          () -> KafkaConnectUtils.getLatestNumPartitions(BOOTSTRAP_SERVERS, TOPIC_NAME));
      verify(adminClient).close();
    }
  }
}
