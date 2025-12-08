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

import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * This class allows clients to start and end transactions. Anything done between a start and end transaction is
 * guaranteed to be atomic.
 */
public class TransactionManager implements Serializable, AutoCloseable {

  protected static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);
  @Getter
  protected final LockManager lockManager;
  @Getter
  protected final boolean isLockRequired;
  protected Option<HoodieInstant> changeActionInstant = Option.empty();
  private Option<HoodieInstant> lastCompletedActionInstant = Option.empty();

  public TransactionManager(HoodieWriteConfig config, HoodieStorage storage) {
    this(new LockManager(config, storage), config.isLockRequired());
  }

  protected TransactionManager(LockManager lockManager, boolean isLockRequired) {
    this.lockManager = lockManager;
    this.isLockRequired = isLockRequired;
  }

  public void beginStateChange(Option<HoodieInstant> changeActionInstant,
                               Option<HoodieInstant> lastCompletedActionInstant) {
    if (isLockRequired) {
      LOG.info("State change starting for {} with latest completed action instant {}",
          changeActionInstant, lastCompletedActionInstant);
      lockManager.lock();
      reset(this.changeActionInstant, changeActionInstant, lastCompletedActionInstant);
      LOG.info("State change started for {} with latest completed action instant {}",
          changeActionInstant, lastCompletedActionInstant);
    }
  }

  public void endStateChange(Option<HoodieInstant> changeActionInstant) {
    if (isLockRequired) {
      LOG.info("State change ending for action instant {}", changeActionInstant);
      if (reset(changeActionInstant, Option.empty(), Option.empty())) {
        lockManager.unlock();
        LOG.info("State change ended for action instant {}", changeActionInstant);
      }
    }
  }

  protected synchronized boolean reset(Option<HoodieInstant> callerInstant,
                                       Option<HoodieInstant> changeActionInstant,
                                       Option<HoodieInstant> lastCompletedActionInstant) {
    if (!this.changeActionInstant.isPresent() || this.changeActionInstant.get().equals(callerInstant.get())) {
      this.changeActionInstant = changeActionInstant;
      this.lastCompletedActionInstant = lastCompletedActionInstant;
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    if (isLockRequired) {
      lockManager.close();
      LOG.debug("Transaction manager closed");
    }
  }

  public Option<HoodieInstant> getLastCompletedTransactionOwner() {
    return lastCompletedActionInstant;
  }

  public Option<HoodieInstant> getCurrentTransactionOwner() {
    return changeActionInstant;
  }
}
