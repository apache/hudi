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

package org.apache.hudi.sink.checkpoint;

import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Interface for checkpoint services that track Kafka offsets and other streaming metadata.
 *
 * <p>This abstraction allows different implementations for checkpoint tracking:
 * <ul>
 *   <li>Default no-op implementation for open-source users</li>
 *   <li>Custom implementations for enterprise/internal checkpoint systems</li>
 * </ul>
 */
public interface CheckpointService extends Serializable {

  /**
   * Fetches checkpoint information for a streaming job.
   *
   * @param request The checkpoint request containing job and offset metadata
   * @return Optional containing checkpoint info if available, empty otherwise
   * @throws IOException if fetching checkpoint information fails
   */
  Option<CheckpointInfo> getCheckpointInfo(CheckpointRequest request) throws IOException;

  /**
   * Request object containing parameters for fetching checkpoint information.
   */
  class CheckpointRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String dc;
    private String env;
    private long checkpointId;
    private String jobName;
    private String hadoopUser;
    private String sourceCluster;
    private String targetCluster;
    private int checkpointLookback;
    private Map<String, String> topicOperatorIds;
    private String serviceTier;
    private String serviceName;

    // Default constructor
    public CheckpointRequest() {
    }

    // All args constructor
    public CheckpointRequest(String dc, String env, long checkpointId, String jobName,
                           String hadoopUser, String sourceCluster, String targetCluster,
                           int checkpointLookback, Map<String, String> topicOperatorIds,
                           String serviceTier, String serviceName) {
      this.dc = dc;
      this.env = env;
      this.checkpointId = checkpointId;
      this.jobName = jobName;
      this.hadoopUser = hadoopUser;
      this.sourceCluster = sourceCluster;
      this.targetCluster = targetCluster;
      this.checkpointLookback = checkpointLookback;
      this.topicOperatorIds = topicOperatorIds;
      this.serviceTier = serviceTier;
      this.serviceName = serviceName;
    }

    // Getters
    public String getDc() {
      return dc;
    }

    public String getEnv() {
      return env;
    }

    public long getCheckpointId() {
      return checkpointId;
    }

    public String getJobName() {
      return jobName;
    }

    public String getHadoopUser() {
      return hadoopUser;
    }

    public String getSourceCluster() {
      return sourceCluster;
    }

    public String getTargetCluster() {
      return targetCluster;
    }

    public int getCheckpointLookback() {
      return checkpointLookback;
    }

    public Map<String, String> getTopicOperatorIds() {
      return topicOperatorIds;
    }

    public String getServiceTier() {
      return serviceTier;
    }

    public String getServiceName() {
      return serviceName;
    }

    // Setters
    public void setDc(String dc) {
      this.dc = dc;
    }

    public void setEnv(String env) {
      this.env = env;
    }

    public void setCheckpointId(long checkpointId) {
      this.checkpointId = checkpointId;
    }

    public void setJobName(String jobName) {
      this.jobName = jobName;
    }

    public void setHadoopUser(String hadoopUser) {
      this.hadoopUser = hadoopUser;
    }

    public void setSourceCluster(String sourceCluster) {
      this.sourceCluster = sourceCluster;
    }

    public void setTargetCluster(String targetCluster) {
      this.targetCluster = targetCluster;
    }

    public void setCheckpointLookback(int checkpointLookback) {
      this.checkpointLookback = checkpointLookback;
    }

    public void setTopicOperatorIds(Map<String, String> topicOperatorIds) {
      this.topicOperatorIds = topicOperatorIds;
    }

    public void setServiceTier(String serviceTier) {
      this.serviceTier = serviceTier;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    // Builder pattern
    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private String dc;
      private String env;
      private long checkpointId;
      private String jobName;
      private String hadoopUser;
      private String sourceCluster;
      private String targetCluster;
      private int checkpointLookback;
      private Map<String, String> topicOperatorIds;
      private String serviceTier;
      private String serviceName;

      public Builder dc(String dc) {
        this.dc = dc;
        return this;
      }

      public Builder env(String env) {
        this.env = env;
        return this;
      }

      public Builder checkpointId(long checkpointId) {
        this.checkpointId = checkpointId;
        return this;
      }

      public Builder jobName(String jobName) {
        this.jobName = jobName;
        return this;
      }

      public Builder hadoopUser(String hadoopUser) {
        this.hadoopUser = hadoopUser;
        return this;
      }

      public Builder sourceCluster(String sourceCluster) {
        this.sourceCluster = sourceCluster;
        return this;
      }

      public Builder targetCluster(String targetCluster) {
        this.targetCluster = targetCluster;
        return this;
      }

      public Builder checkpointLookback(int checkpointLookback) {
        this.checkpointLookback = checkpointLookback;
        return this;
      }

      public Builder topicOperatorIds(Map<String, String> topicOperatorIds) {
        this.topicOperatorIds = topicOperatorIds;
        return this;
      }

      public Builder serviceTier(String serviceTier) {
        this.serviceTier = serviceTier;
        return this;
      }

      public Builder serviceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
      }

      public CheckpointRequest build() {
        return new CheckpointRequest(dc, env, checkpointId, jobName, hadoopUser,
            sourceCluster, targetCluster, checkpointLookback, topicOperatorIds,
            serviceTier, serviceName);
      }
    }

    @Override
    public String toString() {
      return "CheckpointRequest{"
          + "dc='" + dc + '\''
          + ", env='" + env + '\''
          + ", checkpointId=" + checkpointId
          + ", jobName='" + jobName + '\''
          + ", hadoopUser='" + hadoopUser + '\''
          + ", sourceCluster='" + sourceCluster + '\''
          + ", targetCluster='" + targetCluster + '\''
          + ", checkpointLookback=" + checkpointLookback
          + ", topicOperatorIds=" + topicOperatorIds
          + ", serviceTier='" + serviceTier + '\''
          + ", serviceName='" + serviceName + '\''
          + '}';
    }
  }

  /**
   * Response object containing checkpoint information.
   */
  class CheckpointInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> kafkaOffsets;
    private final long checkpointId;

    public CheckpointInfo(Map<String, String> kafkaOffsets, long checkpointId) {
      this.kafkaOffsets = kafkaOffsets;
      this.checkpointId = checkpointId;
    }

    public Map<String, String> getKafkaOffsets() {
      return kafkaOffsets;
    }

    public long getCheckpointId() {
      return checkpointId;
    }

    @Override
    public String toString() {
      return "CheckpointInfo{"
          + "kafkaOffsets=" + kafkaOffsets
          + ", checkpointId=" + checkpointId
          + '}';
    }
  }
}
