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

package org.apache.hudi.helper;

import org.apache.hudi.connect.ControlMessage;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.transaction.TransactionCoordinator;
import org.apache.hudi.connect.transaction.TransactionParticipant;
import org.apache.hudi.exception.HoodieException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A mock Kafka Control Agent that supports the testing
 * of a {@link TransactionCoordinator} with multiple
 * instances of {@link TransactionParticipant}.
 */
public class MockKafkaControlAgent implements KafkaControlAgent {

  private final Map<String, TransactionCoordinator> coordinators;
  private final Map<String, List<TransactionParticipant>> participants;

  public MockKafkaControlAgent() {
    coordinators = new HashMap<>();
    participants = new HashMap<>();
  }

  @Override
  public void registerTransactionCoordinator(TransactionCoordinator coordinator) {
    coordinators.put(coordinator.getPartition().topic(), coordinator);
  }

  @Override
  public void registerTransactionParticipant(TransactionParticipant participant) {
    if (!participants.containsKey(participant.getPartition().topic())) {
      participants.put(participant.getPartition().topic(), new ArrayList<>());
    }
    participants.get(participant.getPartition().topic()).add(participant);
  }

  @Override
  public void deregisterTransactionCoordinator(TransactionCoordinator coordinator) {
    coordinators.remove(coordinator.getPartition().topic());
  }

  @Override
  public void deregisterTransactionParticipant(TransactionParticipant worker) {
    if (participants.containsKey(worker.getPartition().topic())) {
      participants.get(worker.getPartition().topic()).remove(worker);
    }
  }

  @Override
  public void publishMessage(ControlMessage message) {
    try {
      String topic = message.getTopicName();
      if (message.getSenderType().equals(ControlMessage.EntityType.COORDINATOR)) {
        for (TransactionParticipant participant : participants.get(topic)) {
          participant.processControlEvent(message);
        }
      } else {
        coordinators.get(topic).processControlEvent(message);
      }
    } catch (Exception exception) {
      throw new HoodieException("Fatal error trying to relay Kafka Control Messages for Testing.");
    }
  }
}
