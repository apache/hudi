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

package org.apache.hudi.hbase;

import com.google.errorprone.annotations.RestrictedApi;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ChoreService is a service that can be used to schedule instances of {@link ScheduledChore} to run
 * periodically while sharing threads. The ChoreService is backed by a
 * {@link ScheduledThreadPoolExecutor} whose core pool size changes dynamically depending on the
 * number of {@link ScheduledChore} scheduled. All of the threads in the core thread pool of the
 * underlying {@link ScheduledThreadPoolExecutor} are set to be daemon threads.
 * <p>
 * The ChoreService provides the ability to schedule, cancel, and trigger instances of
 * {@link ScheduledChore}. The ChoreService also provides the ability to check on the status of
 * scheduled chores. The number of threads used by the ChoreService changes based on the scheduling
 * load and whether or not the scheduled chores are executing on time. As more chores are scheduled,
 * there may be a need to increase the number of threads if it is noticed that chores are no longer
 * meeting their scheduled start times. On the other hand, as chores are cancelled, an attempt is
 * made to reduce the number of running threads to see if chores can still meet their start times
 * with a smaller thread pool.
 * <p>
 * When finished with a ChoreService it is good practice to call {@link ChoreService#shutdown()}.
 * Calling this method ensures that all scheduled chores are cancelled and cleaned up properly.
 */
@InterfaceAudience.Private
public class ChoreService {
  private static final Logger LOG = LoggerFactory.getLogger(ChoreService.class);

  /**
   * The minimum number of threads in the core pool of the underlying ScheduledThreadPoolExecutor
   */
  @InterfaceAudience.Private
  public final static int MIN_CORE_POOL_SIZE = 1;

  /**
   * This thread pool is used to schedule all of the Chores
   */
  private final ScheduledThreadPoolExecutor scheduler;

  /**
   * Maps chores to their futures. Futures are used to control a chore's schedule
   */
  private final HashMap<ScheduledChore, ScheduledFuture<?>> scheduledChores;

  /**
   * Maps chores to Booleans which indicate whether or not a chore has caused an increase in the
   * core pool size of the ScheduledThreadPoolExecutor. Each chore should only be allowed to
   * increase the core pool size by 1 (otherwise a single long running chore whose execution is
   * longer than its period would be able to spawn too many threads).
   */
  private final HashMap<ScheduledChore, Boolean> choresMissingStartTime;

  /**
   * The coreThreadPoolPrefix is the prefix that will be applied to all threads within the
   * ScheduledThreadPoolExecutor. The prefix is typically related to the Server that the service is
   * running on. The prefix is useful because it allows us to monitor how the thread pool of a
   * particular service changes over time VIA thread dumps.
   */
  private final String coreThreadPoolPrefix;

  /**
   *
   * @param coreThreadPoolPrefix Prefix that will be applied to the Thread name of all threads
   *          spawned by this service
   */
  @InterfaceAudience.Private
  public ChoreService(final String coreThreadPoolPrefix) {
    this(coreThreadPoolPrefix, MIN_CORE_POOL_SIZE, false);
  }

  /**
   * @param coreThreadPoolPrefix Prefix that will be applied to the Thread name of all threads
   *          spawned by this service
   * @param jitter Should chore service add some jitter for all of the scheduled chores. When set
   *               to true this will add -10% to 10% jitter.
   */
  public ChoreService(final String coreThreadPoolPrefix, final boolean jitter) {
    this(coreThreadPoolPrefix, MIN_CORE_POOL_SIZE, jitter);
  }

  /**
   * @param coreThreadPoolPrefix Prefix that will be applied to the Thread name of all threads
   *          spawned by this service
   * @param corePoolSize The initial size to set the core pool of the ScheduledThreadPoolExecutor
   *          to during initialization. The default size is 1, but specifying a larger size may be
   *          beneficial if you know that 1 thread will not be enough.
   * @param jitter Should chore service add some jitter for all of the scheduled chores. When set
   *               to true this will add -10% to 10% jitter.
   */
  public ChoreService(final String coreThreadPoolPrefix, int corePoolSize, boolean jitter) {
    this.coreThreadPoolPrefix = coreThreadPoolPrefix;
    if (corePoolSize < MIN_CORE_POOL_SIZE)  {
      corePoolSize = MIN_CORE_POOL_SIZE;
    }

    final ThreadFactory threadFactory = new ChoreServiceThreadFactory(coreThreadPoolPrefix);
    if (jitter) {
      scheduler = new JitterScheduledThreadPoolExecutorImpl(corePoolSize, threadFactory, 0.1);
    } else {
      scheduler = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
    }

    scheduler.setRemoveOnCancelPolicy(true);
    scheduledChores = new HashMap<>();
    choresMissingStartTime = new HashMap<>();
  }

  /**
   * @param chore Chore to be scheduled. If the chore is already scheduled with another ChoreService
   *          instance, that schedule will be cancelled (i.e. a Chore can only ever be scheduled
   *          with a single ChoreService instance).
   * @return true when the chore was successfully scheduled. false when the scheduling failed
   *         (typically occurs when a chore is scheduled during shutdown of service)
   */
  public boolean scheduleChore(ScheduledChore chore) {
    if (chore == null) {
      return false;
    }
    // always lock chore first to prevent dead lock
    synchronized (chore) {
      synchronized (this) {
        try {
          // Chores should only ever be scheduled with a single ChoreService. If the choreService
          // is changing, cancel any existing schedules of this chore.
          if (chore.getChoreService() == this) {
            LOG.warn("Chore {} has already been scheduled with us", chore);
            return false;
          }
          if (chore.getPeriod() <= 0) {
            LOG.info("Chore {} is disabled because its period is not positive.", chore);
            return false;
          }
          LOG.info("Chore {} is enabled.", chore);
          if (chore.getChoreService() != null) {
            LOG.info("Cancel chore {} from its previous service", chore);
            chore.getChoreService().cancelChore(chore);
          }
          chore.setChoreService(this);
          ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(chore, chore.getInitialDelay(),
              chore.getPeriod(), chore.getTimeUnit());
          scheduledChores.put(chore, future);
          return true;
        } catch (Exception e) {
          LOG.error("Could not successfully schedule chore: {}", chore.getName(), e);
          return false;
        }
      }
    }
  }

  /**
   * @param chore The Chore to be rescheduled. If the chore is not scheduled with this ChoreService
   *          yet then this call is equivalent to a call to scheduleChore.
   */
  private void rescheduleChore(ScheduledChore chore) {
    if (scheduledChores.containsKey(chore)) {
      ScheduledFuture<?> future = scheduledChores.get(chore);
      future.cancel(false);
    }
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(chore, chore.getInitialDelay(),
        chore.getPeriod(), chore.getTimeUnit());
    scheduledChores.put(chore, future);
  }

  /**
   * Cancel any ongoing schedules that this chore has with the implementer of this interface.
   * <p/>
   * Call {@link ScheduledChore#cancel()} to cancel a {@link ScheduledChore}, in
   * {@link ScheduledChore#cancel()} method we will call this method to remove the
   * {@link ScheduledChore} from this {@link ChoreService}.
   */
  @RestrictedApi(explanation = "Should only be called in ScheduledChore", link = "",
      allowedOnPath = ".*/org/apache/hudi/hbase/(ScheduledChore|ChoreService).java")
  synchronized void cancelChore(ScheduledChore chore) {
    cancelChore(chore, true);
  }

  /**
   * Cancel any ongoing schedules that this chore has with the implementer of this interface.
   * <p/>
   * Call {@link ScheduledChore#cancel(boolean)} to cancel a {@link ScheduledChore}, in
   * {@link ScheduledChore#cancel(boolean)} method we will call this method to remove the
   * {@link ScheduledChore} from this {@link ChoreService}.
   */
  @RestrictedApi(explanation = "Should only be called in ScheduledChore", link = "",
      allowedOnPath = ".*/org/apache/hudi/hbase/(ScheduledChore|ChoreService).java")
  synchronized void cancelChore(ScheduledChore chore, boolean mayInterruptIfRunning) {
    if (scheduledChores.containsKey(chore)) {
      ScheduledFuture<?> future = scheduledChores.get(chore);
      future.cancel(mayInterruptIfRunning);
      scheduledChores.remove(chore);

      // Removing a chore that was missing its start time means it may be possible
      // to reduce the number of threads
      if (choresMissingStartTime.containsKey(chore)) {
        choresMissingStartTime.remove(chore);
        requestCorePoolDecrease();
      }
    }
  }

  /**
   * @return true when the chore is scheduled with the implementer of this interface
   */
  @InterfaceAudience.Private
  public synchronized boolean isChoreScheduled(ScheduledChore chore) {
    return chore != null && scheduledChores.containsKey(chore)
        && !scheduledChores.get(chore).isDone();
  }

  /**
   * This method tries to execute the chore immediately. If the chore is executing at the time of
   * this call, the chore will begin another execution as soon as the current execution finishes
   */
  @RestrictedApi(explanation = "Should only be called in ScheduledChore", link = "",
      allowedOnPath = ".*/org/apache/hudi/hbase/ScheduledChore.java")
  synchronized void triggerNow(ScheduledChore chore) {
    assert chore.getChoreService() == this;
    rescheduleChore(chore);
  }

  /**
   * @return number of chores that this service currently has scheduled
   */
  int getNumberOfScheduledChores() {
    return scheduledChores.size();
  }

  /**
   * @return number of chores that this service currently has scheduled that are missing their
   *         scheduled start time
   */
  int getNumberOfChoresMissingStartTime() {
    return choresMissingStartTime.size();
  }

  /**
   * @return number of threads in the core pool of the underlying ScheduledThreadPoolExecutor
   */
  int getCorePoolSize() {
    return scheduler.getCorePoolSize();
  }

  /**
   * Custom ThreadFactory used with the ScheduledThreadPoolExecutor so that all the threads are
   * daemon threads, and thus, don't prevent the JVM from shutting down
   */
  static class ChoreServiceThreadFactory implements ThreadFactory {
    private final String threadPrefix;
    private final static String THREAD_NAME_SUFFIX = ".Chore.";
    private AtomicInteger threadNumber = new AtomicInteger(1);

    /**
     * @param threadPrefix The prefix given to all threads created by this factory
     */
    public ChoreServiceThreadFactory(final String threadPrefix) {
      this.threadPrefix = threadPrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread =
          new Thread(r, threadPrefix + THREAD_NAME_SUFFIX + threadNumber.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }

  /**
   * Represents a request to increase the number of core pool threads. Typically a request
   * originates from the fact that the current core pool size is not sufficient to service all of
   * the currently running Chores
   * @return true when the request to increase the core pool size succeeds
   */
  private synchronized boolean requestCorePoolIncrease() {
    // There is no point in creating more threads than scheduledChores.size since scheduled runs
    // of the same chore cannot run concurrently (i.e. happen-before behavior is enforced
    // amongst occurrences of the same chore).
    if (scheduler.getCorePoolSize() < scheduledChores.size()) {
      scheduler.setCorePoolSize(scheduler.getCorePoolSize() + 1);
      printChoreServiceDetails("requestCorePoolIncrease");
      return true;
    }
    return false;
  }

  /**
   * Represents a request to decrease the number of core pool threads. Typically a request
   * originates from the fact that the current core pool size is more than sufficient to service the
   * running Chores.
   */
  private synchronized void requestCorePoolDecrease() {
    if (scheduler.getCorePoolSize() > MIN_CORE_POOL_SIZE) {
      scheduler.setCorePoolSize(scheduler.getCorePoolSize() - 1);
      printChoreServiceDetails("requestCorePoolDecrease");
    }
  }

  /**
   * A callback that tells the implementer of this interface that one of the scheduled chores is
   * missing its start time. The implication of a chore missing its start time is that the service's
   * current means of scheduling may not be sufficient to handle the number of ongoing chores (the
   * other explanation is that the chore's execution time is greater than its scheduled period). The
   * service should try to increase its concurrency when this callback is received.
   * @param chore The chore that missed its start time
   */
  @RestrictedApi(explanation = "Should only be called in ScheduledChore", link = "",
      allowedOnPath = ".*/org/apache/hudi/hbase/ScheduledChore.java")
  synchronized void onChoreMissedStartTime(ScheduledChore chore) {
    if (!scheduledChores.containsKey(chore)) {
      return;
    }

    // If the chore has not caused an increase in the size of the core thread pool then request an
    // increase. This allows each chore missing its start time to increase the core pool size by
    // at most 1.
    if (!choresMissingStartTime.containsKey(chore) || !choresMissingStartTime.get(chore)) {
      choresMissingStartTime.put(chore, requestCorePoolIncrease());
    }

    // Must reschedule the chore to prevent unnecessary delays of chores in the scheduler. If
    // the chore is NOT rescheduled, future executions of this chore will be delayed more and
    // more on each iteration. This hurts us because the ScheduledThreadPoolExecutor allocates
    // idle threads to chores based on how delayed they are.
    rescheduleChore(chore);
    printChoreDetails("onChoreMissedStartTime", chore);
  }

  /**
   * shutdown the service. Any chores that are scheduled for execution will be cancelled. Any chores
   * in the middle of execution will be interrupted and shutdown. This service will be unusable
   * after this method has been called (i.e. future scheduling attempts will fail).
   * <p/>
   * Notice that, this will only clean the chore from this ChoreService but you could still schedule
   * the chore with other ChoreService.
   */
  public synchronized void shutdown() {
    if (isShutdown()) {
      return;
    }
    scheduler.shutdownNow();
    LOG.info("Chore service for: {} had {} on shutdown", coreThreadPoolPrefix,
        scheduledChores.keySet());
    cancelAllChores(true);
    scheduledChores.clear();
    choresMissingStartTime.clear();
  }

  /**
   * @return true when the service is shutdown and thus cannot be used anymore
   */
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  /**
   * @return true when the service is shutdown and all threads have terminated
   */
  public boolean isTerminated() {
    return scheduler.isTerminated();
  }

  private void cancelAllChores(final boolean mayInterruptIfRunning) {
    // Build list of chores to cancel so we can iterate through a set that won't change
    // as chores are cancelled. If we tried to cancel each chore while iterating through
    // keySet the results would be undefined because the keySet would be changing
    ArrayList<ScheduledChore> choresToCancel = new ArrayList<>(scheduledChores.keySet());

    for (ScheduledChore chore : choresToCancel) {
      cancelChore(chore, mayInterruptIfRunning);
    }
  }

  /**
   * Prints a summary of important details about the chore. Used for debugging purposes
   */
  private void printChoreDetails(final String header, ScheduledChore chore) {
    if (!LOG.isTraceEnabled()) {
      return;
    }
    LinkedHashMap<String, String> output = new LinkedHashMap<>();
    output.put(header, "");
    output.put("Chore name: ", chore.getName());
    output.put("Chore period: ", Integer.toString(chore.getPeriod()));
    output.put("Chore timeBetweenRuns: ", Long.toString(chore.getTimeBetweenRuns()));

    for (Entry<String, String> entry : output.entrySet()) {
      LOG.trace(entry.getKey() + entry.getValue());
    }
  }

  /**
   * Prints a summary of important details about the service. Used for debugging purposes
   */
  private void printChoreServiceDetails(final String header) {
    if (!LOG.isTraceEnabled()) {
      return;
    }
    LinkedHashMap<String, String> output = new LinkedHashMap<>();
    output.put(header, "");
    output.put("ChoreService corePoolSize: ", Integer.toString(getCorePoolSize()));
    output.put("ChoreService scheduledChores: ", Integer.toString(getNumberOfScheduledChores()));
    output.put("ChoreService missingStartTimeCount: ",
        Integer.toString(getNumberOfChoresMissingStartTime()));

    for (Entry<String, String> entry : output.entrySet()) {
      LOG.trace(entry.getKey() + entry.getValue());
    }
  }
}
