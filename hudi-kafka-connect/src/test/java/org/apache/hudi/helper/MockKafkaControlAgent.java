package org.apache.hudi.helper;

import org.apache.hudi.connect.core.ControlEvent;
import org.apache.hudi.connect.core.TransactionCoordinator;
import org.apache.hudi.connect.core.TransactionParticipant;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
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
  public void publishMessage(ControlEvent message) {
    try {
      String topic = message.senderPartition().topic();
      if (message.getSenderType().equals(ControlEvent.SenderType.COORDINATOR)) {
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
