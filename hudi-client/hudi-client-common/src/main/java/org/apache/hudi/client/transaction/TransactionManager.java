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
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.storage.HoodieStorage;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Coordinates state changes that must be serialized by the configured lock provider.
 *
 * <p>A {@code TransactionManager} is deliberately non-reentrant when locking is required. After a thread enters a
 * state change through {@link #beginStateChange(Option, Option)} or {@link #executeStateChangeWithInstant(Option,
 * Option, Function)}, that same thread must not enter another state change through this manager until the first one
 * finishes. Re-entry fails immediately with a {@link HoodieLockException}; callers already inside a state change can
 * use {@link #generateInstantTime()} when they only need another instant.</p>
 *
 * <p>Calls from other threads still contend through the configured lock provider. A state change started with
 * {@code beginStateChange} must be ended by the same thread and with the same action instant (including an empty
 * instant). Violations fail without changing the active transaction. The callback-based API is preferred because it
 * releases the lock in a {@code finally} block.</p>
 *
 * <p>When locking is not required, the begin and end methods are no-ops and the ownership checks described above do
 * not apply.</p>
 */
public class TransactionManager implements Serializable, AutoCloseable {

  protected static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);
  @Getter
  protected final LockManager lockManager;
  @Getter
  protected final boolean isLockRequired;
  private final transient TimeGenerator timeGenerator;
  private transient volatile Thread lockHolder;
  private transient volatile boolean explicitlyStartedStateChange;
  protected Option<HoodieInstant> changeActionInstant = Option.empty();
  private Option<HoodieInstant> lastCompletedActionInstant = Option.empty();

  public TransactionManager(HoodieWriteConfig config, HoodieStorage storage) {
    this(config, new LockManager(config, storage));
  }

  protected TransactionManager(HoodieWriteConfig writeConfig, LockManager lockManager) {
    this(lockManager, writeConfig.isLockRequired(), TimeGenerators.getTimeGenerator(writeConfig.getTimeGeneratorConfig()));
  }

  public TransactionManager(LockManager lockManager, boolean isLockRequired, TimeGenerator timeGenerator) {
    this.lockManager = lockManager;
    this.isLockRequired = isLockRequired;
    this.timeGenerator = timeGenerator;
    this.lockHolder = null;
    this.explicitlyStartedStateChange = false;
  }

  /**
   * Generates an instant while the current thread owns this manager's lock.
   *
   * <p>This method is intended for code that is already executing inside a state change and must not attempt to
   * enter another one solely to generate an instant.</p>
   *
   * @throws HoodieLockException if locking is required and the current thread does not own the lock
   */
  public String generateInstantTime() {
    if (isLockRequired && !isLockHeldByCurrentThread()) {
      throw new HoodieLockException("Cannot create instant without acquiring a lock first.");
    }
    return HoodieInstantTimeGenerator.createNewInstantTime(timeGenerator, 0L);
  }

  /**
   * Generates an instant time and executes an action that requires that instant time within a lock.
   * This method is non-reentrant when locking is required.
   *
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   * @throws HoodieLockException if the current thread is already executing a state change through this manager
   */
  public <T> T executeStateChangeWithInstant(Function<String, T> instantTimeConsumingAction) {
    return executeStateChangeWithInstant(Option.empty(), Option.empty(), instantTimeConsumingAction);
  }

  /**
   * Uses the provided instant if present or else generates an instant time and executes an action that requires that instant time within a lock.
   * This method is non-reentrant when locking is required.
   *
   * @param providedInstantTime an optional instant time provided by the caller. If not provided, a new instant time will be generated.
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   * @throws HoodieLockException if the current thread is already executing a state change through this manager
   */
  public <T> T executeStateChangeWithInstant(Option<String> providedInstantTime, Function<String, T> instantTimeConsumingAction) {
    return executeStateChangeWithInstant(providedInstantTime, Option.empty(), instantTimeConsumingAction);
  }

  /**
   * Uses the provided instant if present or else generates an instant time and executes an action that requires that instant time within a lock.
   * This method is non-reentrant when locking is required. Calls from other threads contend through the configured
   * lock provider.
   *
   * @param providedInstantTime an optional instant time provided by the caller. If not provided, a new instant time will be generated.
   * @param lastCompletedActionInstant optional input representing the last completed instant, used for logging purposes.
   * @param instantTimeConsumingAction a function that takes the generated instant time and performs some action
   * @return the result of the action
   * @param <T> type of the result
   * @throws HoodieLockException if the current thread is already executing a state change through this manager
   */
  public <T> T executeStateChangeWithInstant(Option<String> providedInstantTime, Option<HoodieInstant> lastCompletedActionInstant, Function<String, T> instantTimeConsumingAction) {
    if (isLockRequired()) {
      acquireLock();
    }
    String requestedInstant = null;
    try {
      requestedInstant = providedInstantTime.orElseGet(() -> HoodieInstantTimeGenerator.createNewInstantTime(timeGenerator, 0L));
      if (lastCompletedActionInstant.isEmpty()) {
        LOG.info("State change starting for {}", changeActionInstant);
      } else {
        LOG.info("State change starting for {} with latest completed action instant {}", changeActionInstant, lastCompletedActionInstant.get());
      }
      return instantTimeConsumingAction.apply(requestedInstant);
    } finally {
      if (isLockRequired()) {
        releaseLock();
        if (requestedInstant != null) {
          LOG.info("State change ended for {}", requestedInstant);
        }
      }
    }
  }

  public void beginStateChange() {
    beginStateChange(Option.empty(), Option.empty());
  }

  /**
   * Starts a non-reentrant state change.
   *
   * <p>When locking is required, the caller must later invoke {@link #endStateChange(Option)} from the same thread and
   * pass an action instant equal to {@code changeActionInstant}. Calls from other threads contend through the
   * configured lock provider.</p>
   *
   * @throws HoodieLockException if the current thread is already executing a state change through this manager
   */
  public void beginStateChange(Option<HoodieInstant> changeActionInstant,
                               Option<HoodieInstant> lastCompletedActionInstant) {
    if (isLockRequired) {
      LOG.info("State change starting for {} with latest completed action instant {}",
          changeActionInstant, lastCompletedActionInstant);
      acquireLock();
      explicitlyStartedStateChange = true;
      reset(changeActionInstant, lastCompletedActionInstant);
      LOG.info("State change started for {} with latest completed action instant {}",
          changeActionInstant, lastCompletedActionInstant);
    }
  }

  public void endStateChange() {
    endStateChange(Option.empty());
  }

  /**
   * Ends a state change started by {@link #beginStateChange(Option, Option)}.
   *
   * @throws HoodieLockException if the current thread does not own the lock or {@code changeActionInstant} does not
   *                              match the instant supplied to {@code beginStateChange}
   */
  public void endStateChange(Option<HoodieInstant> changeActionInstant) {
    if (isLockRequired) {
      LOG.info("State change ending for action instant {}", changeActionInstant);
      if (!isLockHeldByCurrentThread()) {
        throw new HoodieLockException("Cannot end a state change from a thread that does not own the lock");
      }
      if (!explicitlyStartedStateChange) {
        throw new HoodieLockException("Cannot call endStateChange for a callback-based state change");
      }
      if (!this.changeActionInstant.equals(changeActionInstant)) {
        throw new HoodieLockException(String.format(
            "Cannot end state change for action instant %s because the active action instant is %s",
            changeActionInstant, this.changeActionInstant));
      }
      releaseLock();
      LOG.info("State change ended for action instant {}", changeActionInstant);
    }
  }

  /**
   * Acquires the configured lock, failing immediately if the current thread already owns it.
   */
  private void acquireLock() {
    if (isLockHeldByCurrentThread()) {
      throw new HoodieLockException("TransactionManager is non-reentrant: the current thread already owns the lock");
    }
    lockManager.lock();
    // The previous owner may still be clearing its local state after releasing the underlying lock. Publish the new
    // state before returning so that its cleanup cannot erase this ownership.
    this.explicitlyStartedStateChange = false;
    reset(Option.empty(), Option.empty());
    this.lockHolder = Thread.currentThread();
    LOG.info("{}: Lock acquired for action instant {}", this, changeActionInstant);
  }

  private void releaseLock() {
    if (!isLockHeldByCurrentThread()) {
      throw new HoodieLockException("Cannot release a lock that is not owned by the current thread");
    }
    Thread releasingThread = Thread.currentThread();
    lockManager.unlock();
    clearLocalStateAfterUnlock(releasingThread);
    LOG.info("{}: Lock released for action instant {}", this, changeActionInstant);
  }

  private synchronized void clearLocalStateAfterUnlock(Thread releasingThread) {
    // Another thread may acquire the underlying lock and publish itself before the releasing thread reaches this
    // method. Only clear state if it still belongs to the releasing thread.
    if (lockHolder == releasingThread) {
      explicitlyStartedStateChange = false;
      lockHolder = null;
      reset(Option.empty(), Option.empty());
    }
  }

  boolean isLockHeldByCurrentThread() {
    return Thread.currentThread() == lockHolder;
  }

  boolean isLockHeld() {
    return lockHolder != null;
  }

  private void reset(Option<HoodieInstant> changeActionInstant,
                     Option<HoodieInstant> lastCompletedActionInstant) {
    this.changeActionInstant = changeActionInstant;
    this.lastCompletedActionInstant = lastCompletedActionInstant;
  }

  @Override
  public void close() {
    if (lockManager != null) {
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
