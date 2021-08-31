package org.apache.hudi.connect;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.core.ControlEvent;
import org.apache.hudi.connect.core.HudiTransactionCoordinator;
import org.apache.hudi.connect.core.TransactionCoordinator;
import org.apache.hudi.connect.core.TransactionParticipant;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.connect.writers.TransactionServices;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHudiTransactionCoordinator {

  private TopicPartition partition;
  private HudiConnectConfigs configs;
  private TestKafkaControlAgent kafkaControlAgent;
  private TransactionServices transactionServices;

  @BeforeEach
  public void setUp() throws Exception {
    //initPath();
    //initFileSystem();
    //initMetaClient();
    partition = new TopicPartition("test-topic", 0);
    configs = HudiConnectConfigs.newBuilder().build();
    kafkaControlAgent = new TestKafkaControlAgent();
    transactionServices = new TestTransactionServices();
  }

  @AfterEach
  public void tearDown() throws Exception {
    //cleanupResources();
  }

  @Test
  public void testSingleCommitNormalScenario() {
    HudiTransactionCoordinator coordinator = new HudiTransactionCoordinator(
        configs,
        partition,
        kafkaControlAgent,
        transactionServices);
    coordinator.start();

  }

  private static class TestKafkaControlAgent implements KafkaControlAgent {
    @Override
    public void registerTransactionCoordinator(TransactionCoordinator leader) {

    }

    @Override
    public void registerTransactionParticipant(TransactionParticipant worker) {

    }

    @Override
    public void deregisterTransactionCoordinator(TransactionCoordinator leader) {

    }

    @Override
    public void deregisterTransactionParticipant(TransactionParticipant worker) {

    }

    @Override
    public void publishMessage(ControlEvent message) {

    }
  }

  private static class TestTransactionServices implements TransactionServices {
    @Override
    public String startCommit() {
      return null;
    }

    @Override
    public void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {

    }

    @Override
    public Map<String, String> loadLatestCommitMetadata() {
      return new HashMap<>();
    }
  }
}
