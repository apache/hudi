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

package org.apache.hudi.table.marker;

import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.stream.Collectors;

/**
 * This strategy is used for direct marker writers, trying to do early conflict detection.
 * It will use fileSystem api like list and exist directly to check if there is any marker file conflict.
 */
public class SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy extends SimpleDirectMarkerBasedEarlyConflictDetectionStrategy {

  private static final Logger LOG = LogManager.getLogger(SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy.class);

  public SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy(String basePath, HoodieWrapperFileSystem fs, String partitionPath, String fileId, String instantTime,
                                                                          HoodieActiveTimeline activeTimeline, HoodieWriteConfig config, Boolean checkCommitConflict) {
    super(basePath, fs, partitionPath, fileId, instantTime, activeTimeline, config, checkCommitConflict);
  }

  @Override
  public void detectAndResolveConflictIfNecessary() {
    TransactionManager txnManager = new TransactionManager((HoodieWriteConfig) config, fs, partitionPath, fileId);
    try {
      // Need to do transaction before create marker file when using early conflict detection
      txnManager.beginTransaction(partitionPath, fileId);
      this.completedCommitInstants = activeTimeline.reload().getCommitsTimeline().filterCompletedInstants().getInstants().collect(Collectors.toSet());
      super.detectAndResolveConflictIfNecessary();

    } catch (Exception e) {
      LOG.warn("Exception occurs during create marker file in early conflict detection mode within transaction.");
      throw e;
    } finally {
      // End transaction after created marker file.
      txnManager.endTransaction(partitionPath, fileId);
      txnManager.close();
    }
  }
}
