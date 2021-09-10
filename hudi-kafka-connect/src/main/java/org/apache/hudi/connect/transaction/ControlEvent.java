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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.SerializationUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The events sent over the Kafka Control Topic between the
 * coordinator and the followers, in order to ensure
 * coordination across all the writes.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class ControlEvent implements Serializable {

  private static final Logger LOG = LogManager.getLogger(ControlEvent.class);
  private static final int CURRENT_VERSION = 0;

  private final int version = CURRENT_VERSION;
  private MsgType msgType;
  private SenderType senderType;
  private String commitTime;
  private byte[] senderPartition;
  private CoordinatorInfo coordinatorInfo;
  private ParticipantInfo participantInfo;

  public ControlEvent() {
  }

  public ControlEvent(MsgType msgType,
                      SenderType senderType,
                      String commitTime,
                      byte[] senderPartition,
                      CoordinatorInfo coordinatorInfo,
                      ParticipantInfo participantInfo) {
    this.msgType = msgType;
    this.senderType = senderType;
    this.commitTime = commitTime;
    this.senderPartition = senderPartition;
    this.coordinatorInfo = coordinatorInfo;
    this.participantInfo = participantInfo;
  }

  public String key() {
    return msgType.name().toLowerCase(Locale.ROOT);
  }

  public MsgType getMsgType() {
    return msgType;
  }

  public SenderType getSenderType() {
    return senderType;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public byte[] getSenderPartition() {
    return senderPartition;
  }

  public TopicPartition senderPartition() {
    return SerializationUtils.deserialize(senderPartition);
  }

  public CoordinatorInfo getCoordinatorInfo() {
    return coordinatorInfo;
  }

  public ParticipantInfo getParticipantInfo() {
    return participantInfo;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s %s %s %s", version, msgType.name(), commitTime,
        Arrays.toString(senderPartition), coordinatorInfo.toString(), participantInfo.toString());
  }

  /**
   * Builder that helps build {@link ControlEvent}.
   */
  public static class Builder {

    private final MsgType msgType;
    private SenderType senderType;
    private final String commitTime;
    private final byte[] senderPartition;
    private CoordinatorInfo coordinatorInfo;
    private ParticipantInfo participantInfo;

    public Builder(MsgType msgType, SenderType senderType, String commitTime, TopicPartition senderPartition) throws IOException {
      this.msgType = msgType;
      this.senderType = senderType;
      this.commitTime = commitTime;
      this.senderPartition = SerializationUtils.serialize(senderPartition);
    }

    public Builder setCoordinatorInfo(CoordinatorInfo coordinatorInfo) {
      this.coordinatorInfo = coordinatorInfo;
      return this;
    }

    public Builder setParticipantInfo(ParticipantInfo participantInfo) {
      this.participantInfo = participantInfo;
      return this;
    }

    public ControlEvent build() {
      return new ControlEvent(msgType, senderType, commitTime, senderPartition, coordinatorInfo, participantInfo);
    }
  }

  /**
   * The info sent by the {@link TransactionCoordinator} to one or more
   * {@link TransactionParticipant}s.
   */
  public static class CoordinatorInfo implements Serializable {

    private Map<Integer, Long> globalKafkaCommitOffsets;

    public CoordinatorInfo() {
    }

    public CoordinatorInfo(Map<Integer, Long> globalKafkaCommitOffsets) {
      this.globalKafkaCommitOffsets = globalKafkaCommitOffsets;
    }

    public Map<Integer, Long> getGlobalKafkaCommitOffsets() {
      return (globalKafkaCommitOffsets == null) ? new HashMap<>() : globalKafkaCommitOffsets;
    }
  }

  /**
   * The info sent by a {@link TransactionParticipant} instances to the
   * {@link TransactionCoordinator}.
   */
  public static class ParticipantInfo implements Serializable {

    private byte[] writeStatusList;
    private long kafkaCommitOffset;
    private OutcomeType outcomeType;

    public ParticipantInfo() {
    }

    public ParticipantInfo(List<WriteStatus> writeStatuses, long kafkaCommitOffset, OutcomeType outcomeType) throws IOException {
      this.writeStatusList = SerializationUtils.serialize(writeStatuses);
      this.kafkaCommitOffset = kafkaCommitOffset;
      this.outcomeType = outcomeType;
    }

    public byte[] getWriteStatusList() {
      return writeStatusList;
    }

    public List<WriteStatus> writeStatuses() {
      return SerializationUtils.deserialize(writeStatusList);
    }

    public long getKafkaCommitOffset() {
      return kafkaCommitOffset;
    }

    public OutcomeType getOutcomeType() {
      return outcomeType;
    }
  }

  /**
   * Type of Control Event.
   */
  public enum MsgType {
    START_COMMIT,
    END_COMMIT,
    ACK_COMMIT,
    WRITE_STATUS,
  }

  public enum SenderType {
    COORDINATOR,
    PARTICIPANT
  }

  public enum OutcomeType {
    WRITE_SUCCESS,
  }
}
