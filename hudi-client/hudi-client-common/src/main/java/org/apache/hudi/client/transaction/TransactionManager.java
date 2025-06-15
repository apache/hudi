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
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Function;

/**
 * This class allows clients to start and end transactions. Anything done between a start and end transaction is
 * guaranteed to be atomic.
 */
public class TransactionManager implements Serializable, AutoCloseable {

  protected static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);
  protected final LockManager lockManager;
  protected final boolean isLockRequired;
  private final transient TimeGenerator timeGenerator;
  protected Option<HoodieInstant> changeActionInstant = Option.empty();
  private Option<HoodieInstant> lastCompletedActionInstant = Option.empty();

  public TransactionManager(HoodieWriteConfig config, HoodieStorage storage) {
    this(new LockManager(config, storage), config, storage.getConf());
  }

  protected TransactionManager(LockManager lockManager, HoodieWriteConfig writeConfig, StorageConfiguration<?> storageConf) {
    this(lockManager, writeConfig.isLockRequired(), TimeGenerators.getTimeGenerator(writeConfig.getTimeGeneratorConfig(), storageConf));
  }

  public TransactionManager(LockManager lockManager, boolean isLockRequired, TimeGenerator timeGenerator) {
    this.lockManager = lockManager;
    this.isLockRequired = isLockRequired;
    this.timeGenerator = timeGenerator;
  }

  /**
   * Generates an instant time and executes an action that requires that instant time within a lock.
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   */
  public <T> T executeStateChangeWithInstant(Function<String, T> instantTimeConsumingAction) {
    return executeStateChangeWithInstant(Option.empty(), Option.empty(), instantTimeConsumingAction);
  }

  /**
   * Uses the provided instant if present or else generates an instant time and executes an action that requires that instant time within a lock.
   * @param providedInstantTime an optional instant time provided by the caller. If not provided, a new instant time will be generated.
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   */
  public <T> T executeStateChangeWithInstant(Option<String> providedInstantTime, Function<String, T> instantTimeConsumingAction) {
    return executeStateChangeWithInstant(providedInstantTime, Option.empty(), instantTimeConsumingAction);
  }

  /**
   * Uses the provided instant if present or else generates an instant time and executes an action that requires that instant time within a lock.
   * @param providedInstantTime an optional instant time provided by the caller. If not provided, a new instant time will be generated.
   * @param lastCompletedActionInstant optional input representing the last completed instant, used for logging purposes.
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   */
  public <T> T executeStateChangeWithInstant(Option<String> providedInstantTime, Option<HoodieInstant> lastCompletedActionInstant, Function<String, T> instantTimeConsumingAction) {
    return executeStateChangeWithInstant(true, providedInstantTime, lastCompletedActionInstant, instantTimeConsumingAction);
  }

  /**
   * Uses the provided instant if present or else generates an instant time and executes an action that requires that instant time within a lock.
   * @param requiresLock allows the caller to skip the locking if the action is already within another transaction block.
   * @param providedInstantTime an optional instant time provided by the caller. If not provided, a new instant time will be generated.
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   */
  public <T> T executeStateChangeWithInstant(boolean requiresLock, Option<String> providedInstantTime, Function<String, T> instantTimeConsumingAction) {
    return executeStateChangeWithInstant(requiresLock, providedInstantTime, Option.empty(), instantTimeConsumingAction);
  }

  private  <T> T executeStateChangeWithInstant(boolean requiresLock, Option<String> providedInstantTime,
                                               Option<HoodieInstant> lastCompletedActionInstant, Function<String, T> instantTimeConsumingAction) {
    if (requiresLock && isLockRequired()) {
      lockManager.lock();
    }
    String requestedInstant = providedInstantTime.orElseGet(() -> TimelineUtils.generateInstantTime(false, timeGenerator));
    try {
      if (lastCompletedActionInstant.isEmpty()) {
        LOG.info("State change starting for {}", changeActionInstant);
      } else {
        LOG.info("State change starting for {} with latest completed action instant {}", changeActionInstant, lastCompletedActionInstant.get());
      }
      return instantTimeConsumingAction.apply(requestedInstant);
    } finally {
      if (requiresLock && isLockRequired()) {
        lockManager.unlock();
        LOG.info("State change ended for {}", requestedInstant);
      }
    }
  }

  public void beginStateChange() {
    beginStateChange(Option.empty(), Option.empty());
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

  public void endStateChange() {
    endStateChange(Option.empty());
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
      LOG.info("Transaction manager closed");
    }
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  public Option<HoodieInstant> getLastCompletedTransactionOwner() {
    return lastCompletedActionInstant;
  }

  public Option<HoodieInstant> getCurrentTransactionOwner() {
    return changeActionInstant;
  }

  public boolean isLockRequired() {
    return isLockRequired;
  }
}
