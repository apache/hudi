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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
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
import static org.mockito.Mockito.mock;

public class TestHudiTransactionCoordinator {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int NUM_PARTITIONS = 4;

  private HudiTransactionCoordinator coordinator;
  private TopicPartition partition;
  private HudiConnectConfigs configs;
  private TestKafkaControlAgent kafkaControlAgent;
  private TestTransactionServices transactionServices;
  private CountDownLatch latch;

  @BeforeEach
  public void setUp() throws Exception {
    transactionServices = new TestTransactionServices();
    partition = new TopicPartition(TOPIC_NAME, 0);
    configs = HudiConnectConfigs.newBuilder()
        .withCommitIntervalSecs(1L)
        .withCoordinatorWriteTimeoutSecs(1L)
        .build();
    latch = new CountDownLatch(1);
  }

  @AfterEach
  public void tearDown() throws Exception {
    //cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(value = TestScenarios.class)
  public void testSingleCommitScenario(TestScenarios scenario) throws InterruptedException {
    kafkaControlAgent = new TestKafkaControlAgent(latch, scenario, 5);
    coordinator = new HudiTransactionCoordinator(
        configs,
        partition,
        kafkaControlAgent,
        transactionServices,
        (bootstrapServers, topicName) -> NUM_PARTITIONS);
    kafkaControlAgent.setCoordinator(coordinator);

    coordinator.start();

    latch.await(60, TimeUnit.SECONDS);

    if (latch.getCount() > 0) {
      throw new HoodieException("Test timedout resulting in failure");
    }

    coordinator.stop();
    assertTrue(kafkaControlAgent.hasDeRegistered);
  }

  private static class TestKafkaControlAgent implements KafkaControlAgent {

    private final CountDownLatch latch;
    private final TestScenarios testScenario;
    private final int maxNumberCommitRounds;

    private TransactionCoordinator coordinator;
    private Map<Integer, Long> kafkaOffsetsCommitted;
    private boolean hasRegistered;
    private boolean hasDeRegistered;
    private ControlEvent.MsgType expectedMsgType;
    private int numberCommitRounds;

    public TestKafkaControlAgent(CountDownLatch latch,
                                 TestScenarios testScenario,
                                 int maxNumberCommitRounds) {
      this.latch = latch;
      this.testScenario = testScenario;
      this.maxNumberCommitRounds = maxNumberCommitRounds;
      kafkaOffsetsCommitted = new HashMap<>();
      hasRegistered = false;
      hasDeRegistered = false;
      expectedMsgType = ControlEvent.MsgType.START_COMMIT;
      numberCommitRounds = 0;
    }

    public void setCoordinator(TransactionCoordinator coordinator) {
      this.coordinator = coordinator;
    }

    @Override
    public void registerTransactionCoordinator(TransactionCoordinator leader) {
      assertEquals(leader, coordinator);
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
      assertTrue(hasRegistered);
      testScenarios(message);
    }

    private void testScenarios(ControlEvent message) {
      System.out.println("Received Event Type: " + message.getMsgType() + " " + message.getCommitTime() + " " + numberCommitRounds);
      assertEquals(expectedMsgType, message.getMsgType());

      switch (message.getMsgType()) {
        case START_COMMIT:
          expectedMsgType = ControlEvent.MsgType.END_COMMIT;
          break;
        case END_COMMIT:
          System.out.println("WNI RIMP " + kafkaOffsetsCommitted + " " + message.getCoordinatorInfo().getGlobalKafkaCommitOffsets());
          assertEquals(kafkaOffsetsCommitted, message.getCoordinatorInfo().getGlobalKafkaCommitOffsets());
          int numSuccessPartitions;
          Map<Integer, Long> kafkaOffsets = new HashMap<>();
          List<ControlEvent> controlEvents = new ArrayList<>();
          // Prepare the WriteStatuses for all partitions
          for (int i = 1; i <= NUM_PARTITIONS; i++) {
            try {
              long kafkaOffset = (long) (Math.random() * 10000);
              kafkaOffsets.put(i, kafkaOffset);
              ControlEvent event = successWriteStatus(
                  message.getCommitTime(),
                  new TopicPartition(TOPIC_NAME, i),
                  kafkaOffset);
              controlEvents.add(event);
            } catch (Exception exception) {
              throw new HoodieException("Fatal error sending control event to Coordinator");
            }
          }

          switch (testScenario) {
            case ALL_CONNECT_TASKS_SUCCESS:
              numSuccessPartitions = NUM_PARTITIONS;
              kafkaOffsetsCommitted.putAll(kafkaOffsets);
              expectedMsgType = ControlEvent.MsgType.ACK_COMMIT;
              break;
            case SUBSET_CONNECT_TASKS_FAILED:
              numSuccessPartitions = NUM_PARTITIONS/2;
              expectedMsgType = ControlEvent.MsgType.START_COMMIT;
              break;
            default:
              throw new HoodieException("Unknown test scenario " + testScenario);
          }

          // Send events based on test scenario
          for (int i = 0; i < numSuccessPartitions; i++) {
            coordinator.publishControlEvent(controlEvents.get(i));
          }
          break;
        case ACK_COMMIT:
          if (numberCommitRounds >= maxNumberCommitRounds) {
            latch.countDown();
          }
          expectedMsgType = ControlEvent.MsgType.START_COMMIT;
          break;
        default:
          throw new HoodieException("Illegal control message type " + message.getMsgType());
      }

      if (message.getMsgType().equals(ControlEvent.MsgType.START_COMMIT)) {
        if (numberCommitRounds >= maxNumberCommitRounds) {
          latch.countDown();
        }
        numberCommitRounds++;
        expectedMsgType = ControlEvent.MsgType.END_COMMIT;
      }
    }
  }

  private static ControlEvent successWriteStatus(String commitTime,
                                                 TopicPartition partition,
                                                 long kafkaOffset) throws Exception {
    // send WS
    WriteStatus writeStatus = new WriteStatus();
    WriteStatus status = new WriteStatus(false, 1.0);
    //Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(mock(HoodieRecord.class), Option.empty());
      //status.markFailure(mock(HoodieRecord.class), t, Option.empty());
    }
    return new ControlEvent.Builder(ControlEvent.MsgType.WRITE_STATUS,
        commitTime,
        partition)
        .setParticipantInfo(new ControlEvent.ParticipantInfo(
            Collections.singletonList(writeStatus),
            kafkaOffset,
            ControlEvent.OutcomeType.WRITE_SUCCESS))
        .build();
  }

  private static ControlEvent failedWriteStatus(String commitTime,
                                                 TopicPartition partition,
                                                 long kafkaOffset) throws Exception {
    // send WS
    WriteStatus writeStatus = new WriteStatus();
    WriteStatus status = new WriteStatus(false, 1.0);
    Throwable t = new Exception("Some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(mock(HoodieRecord.class), Option.empty());
      status.markFailure(mock(HoodieRecord.class), t, Option.empty());
    }
    return new ControlEvent.Builder(ControlEvent.MsgType.WRITE_STATUS,
        commitTime,
        partition)
        .setParticipantInfo(new ControlEvent.ParticipantInfo(
            Collections.singletonList(writeStatus),
            kafkaOffset,
            ControlEvent.OutcomeType.WRITE_SUCCESS))
        .build();
  }

  private enum TestScenarios {
    SUBSET_CONNECT_TASKS_FAILED,
    ALL_CONNECT_TASKS_SUCCESS
  }

  private static class TestTransactionServices implements TransactionServices {

    private int commitTime;

    public TestTransactionServices() {
      commitTime = 100;
    }

    @Override
    public String startCommit() {
      commitTime++;
      return String.valueOf(commitTime);
    }

    @Override
    public void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
      assertEquals(String.valueOf(this.commitTime), commitTime);
    }

    @Override
    public Map<String, String> loadLatestCommitMetadata() {
      return new HashMap<>();
    }
  }
}
