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

package org.apache.hudi.client.transaction;

import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hadoop.fs.FileSystem;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * This class allows clients to start and end transactions for creating direct marker, used by
 * `SimpleTransactionDirectMarkerBasedDetectionStrategy`, when early conflict
 * detection is enabled.  Anything done between a start and end transaction is guaranteed to be
 * atomic.
 */
public class DirectMarkerTransactionManager extends TransactionManager {
  private final String filePath;

  public DirectMarkerTransactionManager(HoodieWriteConfig config, FileSystem fs, String partitionPath, String fileId) {
    super(new LockManager(config, fs, createUpdatedLockProps(config, partitionPath, fileId)), config.needsLockGuard());
    this.filePath = partitionPath + "/" + fileId;
  }

  public void beginTransaction(String newTxnOwnerInstantTime) {
    if (needsLockGuard) {
      LOG.info("Transaction starting for " + newTxnOwnerInstantTime + " and " + filePath);
      lockManager.lock();

      reset(currentTxnOwnerInstant, Option.of(getInstant(newTxnOwnerInstantTime)), Option.empty());
      LOG.info("Transaction started for " + newTxnOwnerInstantTime + " and " + filePath);
    }
  }

  public void endTransaction(String currentTxnOwnerInstantTime) {
    if (needsLockGuard) {
      LOG.info("Transaction ending with transaction owner " + currentTxnOwnerInstantTime
          + " for " + filePath);
      if (reset(Option.of(getInstant(currentTxnOwnerInstantTime)), Option.empty(), Option.empty())) {
        lockManager.unlock();
        LOG.info("Transaction ended with transaction owner " + currentTxnOwnerInstantTime
            + " for " + filePath);
      }
    }
  }

  /**
   * Rebuilds lock related configs. Only support ZK related lock for now.
   *
   * @param writeConfig   Hudi write configs.
   * @param partitionPath Relative partition path.
   * @param fileId        File ID.
   * @return Updated lock related configs.
   */
  private static TypedProperties createUpdatedLockProps(
      HoodieWriteConfig writeConfig, String partitionPath, String fileId) {
    if (!ZookeeperBasedLockProvider.class.getName().equals(writeConfig.getLockProviderClass())) {
      throw new HoodieNotSupportedException("Only Support ZK-based lock for DirectMarkerTransactionManager now.");
    }
    TypedProperties props = new TypedProperties(writeConfig.getProps());
    props.setProperty(LockConfiguration.ZK_LOCK_KEY_PROP_KEY, partitionPath + "/" + fileId);
    return props;
  }

  private HoodieInstant getInstant(String instantTime) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, EMPTY_STRING, instantTime);
  }
}
