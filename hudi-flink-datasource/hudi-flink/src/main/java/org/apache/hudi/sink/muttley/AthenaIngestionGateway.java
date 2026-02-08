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

package org.apache.hudi.sink.muttley;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Client for interacting with Athena Ingestion Gateway using MuttleyClient.
 */
public class AthenaIngestionGateway extends FlinkHudiMuttleyClient {

  private static final Logger LOG = LoggerFactory.getLogger(AthenaIngestionGateway.class);
  protected static final String PATH_URL = "";
  protected static final String ATHENA_INGESTION_BASE =
      "uber.data.athena.manager.protos.CheckpointService::";
  protected static final String GET_KAFKA_OFFSETS_PROCEDURE = ATHENA_INGESTION_BASE + "GetKafkaOffsetsByCheckpointId";
  private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_READWRITE_TIMEOUT = Duration.ofSeconds(90);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String rpcCallee;

  public AthenaIngestionGateway(final String rpcCaller, final String rpcCallee,
                                final Duration connectionTimeout, final Duration readWriteTimeout) {
    super(rpcCaller, DEFAULT_PORT, connectionTimeout, readWriteTimeout);
    this.rpcCallee = rpcCallee;
  }

  public AthenaIngestionGateway(final String rpcCaller, final String rpcCallee) {
    this(rpcCaller, rpcCallee, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_READWRITE_TIMEOUT);
  }

  @Override
  public String getService() {
    return rpcCallee;
  }

  /**
   * Get kafka offsets from checkpoints for a given jobName, checkpointId.
   * Replaces the functionality from the marmaray implementation using checkpoint IDs.
   */
  public Option<CheckpointKafkaOffsetInfo> getKafkaCheckpointsInfo(final String dc,
                                                                    final String env,
                                                                    final long checkpointId,
                                                                    final String jobName,
                                                                    final String hadoopUser,
                                                                    final String sourceCluster,
                                                                    final String targetCluster,
                                                                    final int checkpointLookback,
                                                                    final Map<String, String> topicOperatorIds,
                                                                    final String serviceTier,
                                                                    final String serviceName) throws IOException {
    ValidationUtils.checkArgument(dc != null && !dc.isEmpty());
    ValidationUtils.checkArgument(env != null && !env.isEmpty());
    ValidationUtils.checkArgument(jobName != null && !jobName.isEmpty());
    ValidationUtils.checkArgument(hadoopUser != null && !hadoopUser.isEmpty());
    ValidationUtils.checkArgument(sourceCluster != null && !sourceCluster.isEmpty());
    ValidationUtils.checkArgument(targetCluster != null && !targetCluster.isEmpty());
    ValidationUtils.checkArgument(topicOperatorIds != null && !topicOperatorIds.isEmpty());
    ValidationUtils.checkArgument(serviceTier != null && !serviceTier.isEmpty());
    ValidationUtils.checkArgument(serviceName != null && !serviceName.isEmpty());

    final CheckpointInfoRequest getKafkaOffsetsRequest = new CheckpointInfoRequest(
        new CheckpointLocator(dc, env, jobName, hadoopUser),
        checkpointId,
        serviceTier,
        serviceName,
        sourceCluster,
        targetCluster,
        topicOperatorIds,
        checkpointLookback);
    try {
      final String requestJson = OBJECT_MAPPER.writeValueAsString(getKafkaOffsetsRequest);
      try (Response response = post(PATH_URL, requestJson, GET_KAFKA_OFFSETS_PROCEDURE)) {
        String resultAsJson = response.body().string();
        LOG.info("Get Kafka Offsets response: {} \nfor request: {}", resultAsJson, requestJson);

        final CheckpointKafkaOffsetInfoResponse checkpointKafkaResponse = OBJECT_MAPPER
            .readValue(resultAsJson, CheckpointKafkaOffsetInfoResponse.class);
        if (checkpointKafkaResponse != null && checkpointKafkaResponse.isValid()) {
          return Option.of(checkpointKafkaResponse.kafkaOffsetsInfo);
        } else {
          LOG.warn(
              "Empty kafka checkpoints info received for topic: {}, job: {}, "
                  + "env: {}, request: {}, \nresponse: {}",
              topicOperatorIds, jobName, env, requestJson, checkpointKafkaResponse);
        }
      }
    } catch (FlinkHudiMuttleyException | IOException e) {
      throw new IOException(String.format(
          "Error getting offset for topic: %s, job: %s",
          topicOperatorIds, jobName), e);
    }

    return Option.empty();
  }

  // Request/Response classes
  static class CheckpointInfoRequest {
    @JsonProperty("checkpointLocator")
    private CheckpointLocator checkpointLocator;
    @JsonProperty("checkpointId")
    private long checkpointId;
    @JsonProperty("serviceTier")
    private String serviceTier;
    @JsonProperty("serviceName")
    private String serviceName;
    @JsonProperty("sourceKafkaCluster")
    private String sourceKafkaCluster;
    @JsonProperty("targetKafkaCluster")
    private String targetKafkaCluster;
    @JsonProperty("topicOperatorIdMap")
    private Map<String, String> topicOperatorIdMap;
    @JsonProperty("checkpointLookback")
    private int checkpointLookback;
    public CheckpointInfoRequest(CheckpointLocator checkpointLocator, long checkpointId,
                               String serviceTier, String serviceName, String sourceKafkaCluster,
                               String targetKafkaCluster, Map<String, String> topicOperatorIdMap,
                               int checkpointLookback) {
      this.checkpointLocator = checkpointLocator;
      this.checkpointId = checkpointId;
      this.serviceTier = serviceTier;
      this.serviceName = serviceName;
      this.sourceKafkaCluster = sourceKafkaCluster;
      this.targetKafkaCluster = targetKafkaCluster;
      this.topicOperatorIdMap = topicOperatorIdMap;
      this.checkpointLookback = checkpointLookback;
    }

    @Override
    public String toString() {
      return "CheckpointInfoRequest{"
          + "checkpointLocator=" + checkpointLocator
          + ", checkpointId=" + checkpointId
          + ", serviceTier='" + serviceTier + '\''
          + ", serviceName='" + serviceName + '\''
          + ", sourceKafkaCluster='" + sourceKafkaCluster + '\''
          + ", targetKafkaCluster='" + targetKafkaCluster + '\''
          + ", topicOperatorIdMap=" + topicOperatorIdMap
          + ", checkpointLookback=" + checkpointLookback
          + '}';
    }
  }

  static class CheckpointKafkaOffsetInfoResponse {
    @JsonProperty("kafkaOffsetsInfo")
    private CheckpointKafkaOffsetInfo kafkaOffsetsInfo;
    @JsonProperty("__statusCode__")
    private int statusCode;
    @JsonProperty("__responseHeaders__")
    private Map<String, String> responseHeaders;

    public boolean isValid() {
      return kafkaOffsetsInfo != null;
    }

    @Override
    public String toString() {
      return "CheckpointKafkaOffsetInfoResponse{"
          + "kafkaOffsetsInfo=" + kafkaOffsetsInfo
          + ", statusCode=" + statusCode
          + ", responseHeaders=" + responseHeaders
          + '}';
    }
  }

  public static class CheckpointKafkaOffsetInfo {
    @JsonProperty("checkpointTimestamp")
    private String checkpointTimestamp;

    @JsonProperty("kafkaOffsetsInfo")
    private List<KafkaOffsetsInfo> kafkaOffsetsInfo;

    @JsonProperty("hudiCommitInstantTime")
    private String hudiCommitInstantTime;

    @JsonProperty("checkpointId")
    private String checkpointId;

    public CheckpointKafkaOffsetInfo() {
    }

    public CheckpointKafkaOffsetInfo(String checkpointTimestamp, List<KafkaOffsetsInfo> kafkaOffsetsInfo,
                                    String hudiCommitInstantTime, String checkpointId) {
      this.checkpointTimestamp = checkpointTimestamp;
      this.kafkaOffsetsInfo = kafkaOffsetsInfo;
      this.hudiCommitInstantTime = hudiCommitInstantTime;
      this.checkpointId = checkpointId;
    }

    public String getCheckpointTimestamp() {
      return checkpointTimestamp;
    }

    public List<KafkaOffsetsInfo> getKafkaOffsetsInfo() {
      return kafkaOffsetsInfo;
    }

    public String getHudiCommitInstantTime() {
      return hudiCommitInstantTime;
    }

    public String getCheckpointId() {
      return checkpointId;
    }

    public void setCheckpointId(String checkpointId) {
      this.checkpointId = checkpointId;
    }

    @Override
    public String toString() {
      return "CheckpointKafkaOffsetInfo{"
          + "checkpointTimestamp='" + checkpointTimestamp + '\''
          + ", kafkaOffsetsInfo=" + kafkaOffsetsInfo
          + ", hudiCommitInstantTime='" + hudiCommitInstantTime + '\''
          + ", checkpointId='" + checkpointId + '\''
          + '}';
    }

    public static class KafkaOffsetsInfo {
      @JsonProperty("topicName")
      private String topicName;

      @JsonProperty("clusterName")
      private String clusterName;

      @JsonProperty("offsets")
      private Offsets offsets;

      public KafkaOffsetsInfo() {
      }

      public KafkaOffsetsInfo(String topicName, String clusterName, Offsets offsets) {
        this.topicName = topicName;
        this.clusterName = clusterName;
        this.offsets = offsets;
      }

      public String getTopicName() {
        return topicName;
      }

      public String getClusterName() {
        return clusterName;
      }

      public Offsets getOffsets() {
        return offsets;
      }

      public void setOffsets(Offsets offsets) {
        this.offsets = offsets;
      }

      @Override
      public String toString() {
        return "KafkaOffsetsInfo{"
            + "topicName ='" + topicName + '\''
            + ", clusterName ='" + clusterName + '\''
            + ", offsets=" + offsets
            + '}';
      }

      public static class Offsets {
        @JsonProperty("offsets")
        private Map<Integer, Long> offsets;

        public Offsets() {
        }

        public Offsets(Map<Integer, Long> offsets) {
          this.offsets = offsets;
        }

        public Map<Integer, Long> getOffsets() {
          return offsets;
        }

        public void setOffsets(Map<Integer, Long> offsets) {
          this.offsets = offsets;
        }

        @Override
        public String toString() {
          return "Offsets{"
              + "offsets=" + offsets
              + '}';
        }
      }
    }
  }

  static class CheckpointLocator {
    @JsonProperty("dc")
    private String dc;
    @JsonProperty("env")
    private String env;
    @JsonProperty("jobName")
    private String jobName;
    @JsonProperty("hadoopUserName")
    private String hadoopUserName;
    public CheckpointLocator(String dc, String env, String jobName, String hadoopUserName) {
      this.dc = dc;
      this.env = env;
      this.jobName = jobName;
      this.hadoopUserName = hadoopUserName;
    }

    @Override
    public String toString() {
      return "CheckpointLocator{"
          + "dc='" + dc + '\''
          + ", env='" + env + '\''
          + ", jobName='" + jobName + '\''
          + ", hadoopUserName='" + hadoopUserName + '\''
          + '}';
    }
  }
}