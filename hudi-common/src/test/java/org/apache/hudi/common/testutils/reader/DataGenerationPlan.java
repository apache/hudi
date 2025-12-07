/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils.reader;

import lombok.Getter;

import java.util.List;

/**
 * The blueprint of records that will be generated
 * by the data generator.
 *
 * Current limitations:
 * 1. One plan generates one file, either a base file, or a log file.
 * 2. One file contains one operation, e.g., insert, delete, or update.
 */
public class DataGenerationPlan {
  // The values for "_row_key" field.
  @Getter
  private final List<String> recordKeys;
  // The partition path for all records.
  @Getter
  private final String partitionPath;
  // The ordering field.
  @Getter
  private final long timestamp;
  // The operation type of the record.
  @Getter
  private final OperationType operationType;
  @Getter
  private final String instantTime;
  private final boolean writePositions;

  public enum OperationType {
    INSERT,
    UPDATE,
    DELETE
  }

  public DataGenerationPlan(List<String> recordKeys,
                            String partitionPath,
                            long timestamp,
                            OperationType operationType,
                            String instantTime,
                            boolean writePositions) {
    this.recordKeys = recordKeys;
    this.partitionPath = partitionPath;
    this.timestamp = timestamp;
    this.operationType = operationType;
    this.instantTime = instantTime;
    this.writePositions = writePositions;
  }

  public boolean getWritePositions() {
    return writePositions;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> recordKeys;
    private String partitionPath;
    private long timestamp;
    private OperationType operationType;
    private String instantTime;
    private boolean writePositions;

    public Builder withRecordKeys(List<String> recordKeys) {
      this.recordKeys = recordKeys;
      return this;
    }

    public Builder withPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
      return this;
    }

    public Builder withTimeStamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder withOperationType(OperationType operationType) {
      this.operationType = operationType;
      return this;
    }

    public Builder withInstantTime(String instantTime) {
      this.instantTime = instantTime;
      return this;
    }

    public Builder withWritePositions(boolean writePositions) {
      this.writePositions = writePositions;
      return this;
    }

    public DataGenerationPlan build() {
      return new DataGenerationPlan(
          recordKeys,
          partitionPath,
          timestamp,
          operationType,
          instantTime,
          writePositions);
    }
  }
}
