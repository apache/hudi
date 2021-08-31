package org.apache.hudi.connect;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.core.ControlEvent;
import org.apache.hudi.connect.core.HudiTransactionCoordinator;
import org.apache.hudi.connect.core.TransactionCoordinator;
import org.apache.hudi.connect.core.TransactionParticipant;
import org.apache.hudi.connect.kafka.KafkaControlAgent;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.connect.writers.TransactionServices;
import org.apache.hudi.exception.HoodieException;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

public class TestHudiTransactionCoordinator {

  private TopicPartition partition;
  private HudiConnectConfigs configs;
  private TestKafkaControlAgent kafkaControlAgent;
  private TestTransactionServices transactionServices;
  private CountDownLatch latch;

  @BeforeEach
  public void setUp() throws Exception {
    //initPath();
    //initFileSystem();
    //initMetaClient();
    partition = new TopicPartition("test-topic", 0);
    configs = HudiConnectConfigs.newBuilder().withCommitIntervalSecs(1L).build();
    latch = new CountDownLatch(1);
    kafkaControlAgent = new TestKafkaControlAgent(latch);
    transactionServices = new TestTransactionServices();
  }

  @AfterEach
  public void tearDown() throws Exception {
    //cleanupResources();
  }

  @Test
  public void testSingleCommitNormalScenario() throws InterruptedException {
    HudiTransactionCoordinator coordinator = new HudiTransactionCoordinator(
        configs,
        partition,
        kafkaControlAgent,
        transactionServices,
        (String bootstrapServers, String topicName) -> 1);
    kafkaControlAgent.setCoordinator(coordinator);
    coordinator.start();

    latch.await(10, TimeUnit.SECONDS);

    if (latch.getCount() > 0) {
      System.out.println("Test Timedout");
      coordinator.stop();
      assertTrue(kafkaControlAgent.hasDeRegistered);
    }

  }

  private static class TestKafkaControlAgent implements KafkaControlAgent {

    private final CountDownLatch latch;
    private TransactionCoordinator coordinator;
    private boolean hasRegistered;
    private boolean hasDeRegistered;
    private ControlEvent.MsgType expectedMsgType;

    public TestKafkaControlAgent(CountDownLatch latch) {
      this.latch = latch;
      hasRegistered = false;
      hasDeRegistered = false;
      expectedMsgType = ControlEvent.MsgType.START_COMMIT;
    }

    public void setCoordinator(TransactionCoordinator coordinator) {
      this.coordinator = coordinator;
    }

    @Override
    public void registerTransactionCoordinator(TransactionCoordinator leader) {
      assertEquals(leader, coordinator);
      System.out.println("YES 1");
      hasRegistered = true;
    }

    @Override
    public void registerTransactionParticipant(TransactionParticipant worker) {
      // no-op
    }

    @Override
    public void deregisterTransactionCoordinator(TransactionCoordinator leader) {
      assertEquals(leader, coordinator);
      hasDeRegistered = true;
    }

    @Override
    public void deregisterTransactionParticipant(TransactionParticipant worker) {
      // no-op
    }

    @Override
    public void publishMessage(ControlEvent message) {
      System.out.println("YES MSG " + message.getMsgType());
      assertEquals(expectedMsgType, message.getMsgType());

      switch (message.getMsgType()) {
        case START_COMMIT:
          expectedMsgType = ControlEvent.MsgType.END_COMMIT;
          break;
        case END_COMMIT:
          // send WS
          WriteStatus writeStatus = new WriteStatus();
          WriteStatus status = new WriteStatus(false, 1.0);
          //Throwable t = new Exception("some error in writing");
          for (int i = 0; i < 1000; i++) {
            status.markSuccess(mock(HoodieRecord.class), Option.empty());
            //status.markFailure(mock(HoodieRecord.class), t, Option.empty());
          }
          try {
            ControlEvent writeStatusEvent = new ControlEvent.Builder(ControlEvent.MsgType.WRITE_STATUS,
                message.getCommitTime(),
                new TopicPartition("test", 1))
                .setParticipantInfo(new ControlEvent.ParticipantInfo(
                    Collections.singletonList(writeStatus),
                    10,
                    ControlEvent.OutcomeType.WRITE_SUCCESS))
                .build();
            coordinator.publishControlEvent(writeStatusEvent);
            expectedMsgType = ControlEvent.MsgType.ACK_COMMIT;
          } catch (Exception exception) {
            throw new HoodieException("Fatal error sending control event to Coordinator");
          }
          break;
        case ACK_COMMIT:
          latch.countDown();
          break;
        default:
          throw new HoodieException("Illegal control message type " + message.getMsgType());
      }

      if (message.getMsgType().equals(ControlEvent.MsgType.START_COMMIT)) {
        expectedMsgType = ControlEvent.MsgType.END_COMMIT;
      } else if (message.getMsgType().equals(ControlEvent.MsgType.ACK_COMMIT)) {
        latch.countDown();
      }
    }
  }

  private static class TestTransactionServices implements TransactionServices {
    @Override
    public String startCommit() {
      return "101";
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
