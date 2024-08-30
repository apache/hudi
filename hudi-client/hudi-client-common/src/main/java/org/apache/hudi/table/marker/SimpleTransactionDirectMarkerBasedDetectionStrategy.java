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

import org.apache.hudi.client.transaction.DirectMarkerTransactionManager;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This strategy is used for direct marker writers, trying to do early conflict detection.
 * It will use fileSystem api like list and exist directly to check if there is any marker file
 * conflict, with transaction locks using {@link DirectMarkerTransactionManager}.
 */
public class SimpleTransactionDirectMarkerBasedDetectionStrategy
    extends SimpleDirectMarkerBasedDetectionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(
      SimpleTransactionDirectMarkerBasedDetectionStrategy.class);

  public SimpleTransactionDirectMarkerBasedDetectionStrategy(
      HoodieStorage storage, String partitionPath, String fileId, String instantTime,
      HoodieActiveTimeline activeTimeline, HoodieWriteConfig config) {
    super(storage, partitionPath, fileId, instantTime, activeTimeline, config);
  }

  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    DirectMarkerTransactionManager txnManager =
        new DirectMarkerTransactionManager((HoodieWriteConfig) config,
            (FileSystem) storage.getFileSystem(), partitionPath, fileId);
    try {
      // Need to do transaction before create marker file when using early conflict detection
      txnManager.beginTransaction(instantTime);
      super.detectAndResolveConflictIfNecessary();

    } catch (Exception e) {
      LOG.warn("Exception occurs during create marker file in early conflict detection mode within transaction.");
      throw e;
    } finally {
      // End transaction after created marker file.
      txnManager.endTransaction(instantTime);
      txnManager.close();
    }
  }
}
