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

package org.apache.hudi.client.transaction;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * This class allows clients to start and end transactions. Anything done between a start and end transaction is
 * guaranteed to be atomic.
 */
public class TransactionManager implements Serializable {

  private static final Logger LOG = LogManager.getLogger(TransactionManager.class);
  private final LockManager lockManager;
  private final boolean isOptimisticConcurrencyControlEnabled;
  private Option<HoodieInstant> currentTxnOwnerInstant = Option.empty();
  private Option<HoodieInstant> lastCompletedTxnOwnerInstant = Option.empty();

  public TransactionManager(HoodieWriteConfig config, FileSystem fs) {
    this.lockManager = new LockManager(config, fs);
    this.isOptimisticConcurrencyControlEnabled = config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl();
  }

  public TransactionManager(HoodieWriteConfig config, FileSystem fs, String partitionPath, String fileId) {
    this.lockManager = new LockManager(config, fs, partitionPath, fileId);
    this.isOptimisticConcurrencyControlEnabled = config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl();
  }

  public void beginTransaction(Option<HoodieInstant> newTxnOwnerInstant,
                               Option<HoodieInstant> lastCompletedTxnOwnerInstant) {
    if (isOptimisticConcurrencyControlEnabled) {
      LOG.info("Transaction starting for " + newTxnOwnerInstant
          + " with latest completed transaction instant " + lastCompletedTxnOwnerInstant);
      lockManager.lock();
      reset(currentTxnOwnerInstant, newTxnOwnerInstant, lastCompletedTxnOwnerInstant);
      LOG.info("Transaction started for " + newTxnOwnerInstant
          + " with latest completed transaction instant " + lastCompletedTxnOwnerInstant);
    }
  }

  public void beginTransaction(String partitionPath, String fileId) {
    if (isOptimisticConcurrencyControlEnabled) {
      LOG.info("Transaction starting for " + partitionPath + "/" + fileId);
      lockManager.lock();
      LOG.info("Transaction started for " + partitionPath + "/" + fileId);
    }
  }

  public void endTransaction(Option<HoodieInstant> currentTxnOwnerInstant) {
    if (isOptimisticConcurrencyControlEnabled) {
      LOG.info("Transaction ending with transaction owner " + currentTxnOwnerInstant);
      if (reset(currentTxnOwnerInstant, Option.empty(), Option.empty())) {
        lockManager.unlock();
        LOG.info("Transaction ended with transaction owner " + currentTxnOwnerInstant);
      }
    }
  }

  public void endTransaction(String filePath) {
    if (isOptimisticConcurrencyControlEnabled) {
      LOG.info("Transaction ending with transaction for " + filePath);
      lockManager.unlock();
      LOG.info("Transaction ended with transaction for " + filePath);
    }
  }

  private synchronized boolean reset(Option<HoodieInstant> callerInstant,
                                  Option<HoodieInstant> newTxnOwnerInstant,
                                  Option<HoodieInstant> lastCompletedTxnOwnerInstant) {
    if (!this.currentTxnOwnerInstant.isPresent() || this.currentTxnOwnerInstant.get().equals(callerInstant.get())) {
      this.currentTxnOwnerInstant = newTxnOwnerInstant;
      this.lastCompletedTxnOwnerInstant = lastCompletedTxnOwnerInstant;
      return true;
    }
    return false;
  }

  public void close() {
    if (isOptimisticConcurrencyControlEnabled) {
      lockManager.close();
      LOG.info("Transaction manager closed");
    }
  }

  public Option<HoodieInstant> getLastCompletedTransactionOwner() {
    return lastCompletedTxnOwnerInstant;
  }

  public Option<HoodieInstant> getCurrentTransactionOwner() {
    return currentTxnOwnerInstant;
  }

}
