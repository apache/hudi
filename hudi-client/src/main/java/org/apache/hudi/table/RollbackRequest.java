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

package org.apache.hudi.table;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

/**
 * Request for performing one rollback action.
 */
public class RollbackRequest {

  /**
   * Rollback Action Types.
   */
  public enum RollbackAction {
    DELETE_DATA_FILES_ONLY, DELETE_DATA_AND_LOG_FILES, APPEND_ROLLBACK_BLOCK
  }

  /**
   * Partition path that needs to be rolled-back.
   */
  private final String partitionPath;

  /**
   * Rollback Instant.
   */
  private final HoodieInstant rollbackInstant;

  /**
   * FileId in case of appending rollback block.
   */
  private final Option<String> fileId;

  /**
   * Latest base instant needed for appending rollback block instant.
   */
  private final Option<String> latestBaseInstant;

  /**
   * Rollback Action.
   */
  private final RollbackAction rollbackAction;

  public RollbackRequest(String partitionPath, HoodieInstant rollbackInstant, Option<String> fileId,
      Option<String> latestBaseInstant, RollbackAction rollbackAction) {
    this.partitionPath = partitionPath;
    this.rollbackInstant = rollbackInstant;
    this.fileId = fileId;
    this.latestBaseInstant = latestBaseInstant;
    this.rollbackAction = rollbackAction;
  }

  public static RollbackRequest createRollbackRequestWithDeleteDataFilesOnlyAction(String partitionPath,
      HoodieInstant rollbackInstant) {
    return new RollbackRequest(partitionPath, rollbackInstant, Option.empty(), Option.empty(),
        RollbackAction.DELETE_DATA_FILES_ONLY);
  }

  public static RollbackRequest createRollbackRequestWithDeleteDataAndLogFilesAction(String partitionPath,
      HoodieInstant rollbackInstant) {
    return new RollbackRequest(partitionPath, rollbackInstant, Option.empty(), Option.empty(),
        RollbackAction.DELETE_DATA_AND_LOG_FILES);
  }

  public static RollbackRequest createRollbackRequestWithAppendRollbackBlockAction(String partitionPath, String fileId,
      String baseInstant, HoodieInstant rollbackInstant) {
    return new RollbackRequest(partitionPath, rollbackInstant, Option.of(fileId), Option.of(baseInstant),
        RollbackAction.APPEND_ROLLBACK_BLOCK);
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public HoodieInstant getRollbackInstant() {
    return rollbackInstant;
  }

  public Option<String> getFileId() {
    return fileId;
  }

  public Option<String> getLatestBaseInstant() {
    return latestBaseInstant;
  }

  public RollbackAction getRollbackAction() {
    return rollbackAction;
  }
}
