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

package org.apache.hudi.connect.transaction;

import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.writers.ConnectTransactionServices;
import org.apache.hudi.connect.writers.KafkaConnectConfigs;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestConnectTransactionCoordinatorLifecycle {

  private static final String TOPIC_NAME = "coordinator-lifecycle-test-topic";

  @Test
  void stopTerminatesEventLoopAndIgnoresFurtherEvents() throws Exception {
    KafkaControlAgent controlAgent = mock(KafkaControlAgent.class);
    ConnectTransactionServices transactionServices = mock(ConnectTransactionServices.class);
    CountDownLatch eventProcessed = new CountDownLatch(1);
    when(transactionServices.fetchLatestExtraCommitMetadata()).thenReturn(Collections.emptyMap());
    when(transactionServices.startCommit()).thenAnswer(invocation -> {
      eventProcessed.countDown();
      return "001";
    });

    KafkaConnectConfigs configs = KafkaConnectConfigs.newBuilder()
        .withCommitIntervalSecs(60L)
        .withCoordinatorWriteTimeoutSecs(60L)
        .build();
    ConnectTransactionCoordinator coordinator = new ConnectTransactionCoordinator(
        configs,
        new TopicPartition(TOPIC_NAME, ConnectTransactionCoordinator.COORDINATOR_KAFKA_PARTITION),
        controlAgent,
        transactionServices,
        (bootstrapServers, topicName) -> 1);

    try {
      coordinator.start();
      assertTrue(eventProcessed.await(5, TimeUnit.SECONDS));

      coordinator.stop();

      assertTrue(coordinator.isEventLoopTerminated());
      assertDoesNotThrow(coordinator::stop);
      assertDoesNotThrow(() -> coordinator.processControlEvent(writeStatusEvent()));
      verify(controlAgent, times(1)).deregisterTransactionCoordinator(coordinator);
    } finally {
      coordinator.stop();
    }
  }

  private static ControlMessage writeStatusEvent() {
    return ControlMessage.newBuilder()
        .setType(ControlMessage.EventType.WRITE_STATUS)
        .setTopicName(TOPIC_NAME)
        .setCommitTime("001")
        .build();
  }
}
