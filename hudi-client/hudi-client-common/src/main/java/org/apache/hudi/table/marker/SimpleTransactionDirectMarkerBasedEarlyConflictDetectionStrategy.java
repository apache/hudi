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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.conflict.detection.HoodieTransactionDirectMarkerBasedEarlyConflictDetectionStrategy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Set;

/**
 * This strategy is used for direct marker writers, trying to do early conflict detection.
 * It will use fileSystem api like list and exist directly to check if there is any marker file conflict.
 */
public class SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy extends HoodieTransactionDirectMarkerBasedEarlyConflictDetectionStrategy {
  private static final Logger LOG = LogManager.getLogger(SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy.class);

  @Override
  public boolean hasMarkerConflict(String basePath, FileSystem fs, String partitionPath, String fileId, String instantTime,
                                   Set<HoodieInstant> completedCommitInstants, HoodieTableMetaClient metaClient) {
    try {
      return checkMarkerConflict(basePath, partitionPath, fileId, fs, instantTime) || checkCommitConflict(metaClient, completedCommitInstants, fileId);
    } catch (IOException e) {
      LOG.warn("Exception occurs during create marker file in eager conflict detection mode.");
      throw new HoodieIOException("Exception occurs during create marker file in eager conflict detection mode.", e);
    }
  }

  @Override
  public void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName) {
    throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
  }
}
