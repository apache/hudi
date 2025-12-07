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

import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import lombok.Getter;

import java.io.Serializable;

/**
 * Request for performing one rollback action.
 */
public class ListingBasedRollbackRequest implements Serializable {

  /**
   * Rollback commands, that trigger a specific handling for rollback.
   */
  public enum Type {
    DELETE_DATA_FILES_ONLY,
    DELETE_DATA_AND_LOG_FILES,
    APPEND_ROLLBACK_BLOCK
  }

  /**
   * Partition path that needs to be rolled-back.
   */
  @Getter
  private final String partitionPath;

  /**
   * FileId in case of appending rollback block.
   */
  @Getter
  private final Option<String> fileId;

  /**
   * Latest base instant needed for appending rollback block instant.
   */
  @Getter
  private final Option<String> latestBaseInstant;

  /**
   * TODO
   */
  @Getter
  private final Option<HoodieWriteStat> writeStat;

  @Getter
  private final Type type;

  public ListingBasedRollbackRequest(String partitionPath, Type type) {
    this(partitionPath, Option.empty(), Option.empty(), Option.empty(), type);
  }

  public ListingBasedRollbackRequest(String partitionPath,
                                     Option<String> fileId,
                                     Option<String> latestBaseInstant,
                                     Option<HoodieWriteStat> writeStat,
                                     Type type) {
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.latestBaseInstant = latestBaseInstant;
    this.writeStat = writeStat;
    this.type = type;
  }

  public static ListingBasedRollbackRequest createRollbackRequestWithDeleteDataFilesOnlyAction(String partitionPath) {
    return new ListingBasedRollbackRequest(partitionPath, Type.DELETE_DATA_FILES_ONLY);
  }

  public static ListingBasedRollbackRequest createRollbackRequestWithDeleteDataAndLogFilesAction(String partitionPath) {
    return new ListingBasedRollbackRequest(partitionPath, Type.DELETE_DATA_AND_LOG_FILES);
  }

  public static ListingBasedRollbackRequest createRollbackRequestWithAppendRollbackBlockAction(String partitionPath,
                                                                                               String fileId,
                                                                                               String baseInstant,
                                                                                               HoodieWriteStat writeStat) {
    return new ListingBasedRollbackRequest(partitionPath, Option.of(fileId), Option.of(baseInstant), Option.of(writeStat), Type.APPEND_ROLLBACK_BLOCK);
  }
}
