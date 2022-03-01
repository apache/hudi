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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HoodieRollbackRequest in HoodieRollbackPlan (avro pojo) is not operable direclty within spark parallel engine.
 * Hence converting the same to this {@link SerializableHoodieRollbackRequest} and then using it within spark.parallelize.
 */
public class SerializableHoodieRollbackRequest implements Serializable {

  private final String partitionPath;
  private final String fileId;
  private final String latestBaseInstant;
  private final List<String> filesToBeDeleted = new ArrayList<>();
  private final Map<String, Long> logBlocksToBeDeleted = new HashMap<>();

  public SerializableHoodieRollbackRequest(HoodieRollbackRequest rollbackRequest) {
    this.partitionPath = rollbackRequest.getPartitionPath();
    this.fileId = rollbackRequest.getFileId();
    this.latestBaseInstant = rollbackRequest.getLatestBaseInstant();
    this.filesToBeDeleted.addAll(rollbackRequest.getFilesToBeDeleted());
    this.logBlocksToBeDeleted.putAll(rollbackRequest.getLogBlocksToBeDeleted());
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getFileId() {
    return fileId;
  }

  public String getLatestBaseInstant() {
    return latestBaseInstant;
  }

  public List<String> getFilesToBeDeleted() {
    return filesToBeDeleted;
  }

  public Map<String, Long> getLogBlocksToBeDeleted() {
    return logBlocksToBeDeleted;
  }
}
