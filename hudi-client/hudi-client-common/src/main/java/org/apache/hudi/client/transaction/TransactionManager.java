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
  private Option<HoodieInstant> currentTxnOwnerInstant;
  private Option<HoodieInstant> lastCompletedTxnOwnerInstant;
  private boolean supportsOptimisticConcurrency;

  public TransactionManager(HoodieWriteConfig config, FileSystem fs) {
    this.lockManager = new LockManager(config, fs);
    this.supportsOptimisticConcurrency = config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl();
  }

  public synchronized void beginTransaction() {
    if (supportsOptimisticConcurrency) {
      LOG.info("Transaction starting without a transaction owner");
      lockManager.lock();
      LOG.info("Transaction started");
    }
  }

  public synchronized void beginTransaction(Option<HoodieInstant> currentTxnOwnerInstant, Option<HoodieInstant> lastCompletedTxnOwnerInstant) {
    if (supportsOptimisticConcurrency) {
      this.lastCompletedTxnOwnerInstant = lastCompletedTxnOwnerInstant;
      lockManager.setLatestCompletedWriteInstant(lastCompletedTxnOwnerInstant);
      LOG.info("Latest completed transaction instant " + lastCompletedTxnOwnerInstant);
      this.currentTxnOwnerInstant = currentTxnOwnerInstant;
      LOG.info("Transaction starting with transaction owner " + currentTxnOwnerInstant);
      lockManager.lock();
      LOG.info("Transaction started");
    }
  }

  public synchronized void endTransaction() {
    if (supportsOptimisticConcurrency) {
      LOG.info("Transaction ending with transaction owner " + currentTxnOwnerInstant);
      lockManager.unlock();
      LOG.info("Transaction ended");
      this.lastCompletedTxnOwnerInstant = Option.empty();
      lockManager.resetLatestCompletedWriteInstant();
    }
  }

  public void close() {
    if (supportsOptimisticConcurrency) {
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
