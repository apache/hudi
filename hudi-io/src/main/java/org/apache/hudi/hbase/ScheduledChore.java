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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduledChore is a task performed on a period in hbase. ScheduledChores become active once
 * scheduled with a {@link ChoreService} via {@link ChoreService#scheduleChore(ScheduledChore)}. The
 * chore is run in a {@link ScheduledThreadPoolExecutor} and competes with other ScheduledChores for
 * access to the threads in the core thread pool. If an unhandled exception occurs, the chore
 * cancellation is logged. Implementers should consider whether or not the Chore will be able to
 * execute within the defined period. It is bad practice to define a ScheduledChore whose execution
 * time exceeds its period since it will try to hog one of the threads in the {@link ChoreService}'s
 * thread pool.
 * <p/>
 * Don't subclass ScheduledChore if the task relies on being woken up for something to do, such as
 * an entry being added to a queue, etc.
 */
@InterfaceAudience.Private
public abstract class ScheduledChore implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledChore.class);

  private final String name;

  /**
   * Default values for scheduling parameters should they be excluded during construction
   */
  private final static TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
  private final static long DEFAULT_INITIAL_DELAY = 0;

  /**
   * Scheduling parameters. Used by ChoreService when scheduling the chore to run periodically
   */
  private final int period; // in TimeUnit units
  private final TimeUnit timeUnit;
  private final long initialDelay; // in TimeUnit units

  /**
   * Interface to the ChoreService that this ScheduledChore is scheduled with. null if the chore is
   * not scheduled.
   */
  private ChoreService choreService;

  /**
   * Variables that encapsulate the meaningful state information
   */
  private long timeOfLastRun = -1; // system time millis
  private long timeOfThisRun = -1; // system time millis
  private boolean initialChoreComplete = false;

  /**
   * A means by which a ScheduledChore can be stopped. Once a chore recognizes that it has been
   * stopped, it will cancel itself. This is particularly useful in the case where a single stopper
   * instance is given to multiple chores. In such a case, a single {@link Stoppable#stop(String)}
   * command can cause many chores to stop together.
   */
  private final Stoppable stopper;

  /**
   * This constructor is for test only. It allows us to create an object and to call chore() on it.
   */
  @InterfaceAudience.Private
  protected ScheduledChore() {
    this("TestChore", null, 0, DEFAULT_INITIAL_DELAY, DEFAULT_TIME_UNIT);
  }

  /**
   * @param name Name assigned to Chore. Useful for identification amongst chores of the same type
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param period Period in millis with which this Chore repeats execution when scheduled.
   */
  public ScheduledChore(final String name, Stoppable stopper, final int period) {
    this(name, stopper, period, DEFAULT_INITIAL_DELAY);
  }

  /**
   * @param name Name assigned to Chore. Useful for identification amongst chores of the same type
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param period Period in millis with which this Chore repeats execution when scheduled.
   * @param initialDelay Delay before this Chore begins to execute once it has been scheduled. A
   *          value of 0 means the chore will begin to execute immediately. Negative delays are
   *          invalid and will be corrected to a value of 0.
   */
  public ScheduledChore(final String name, Stoppable stopper, final int period,
                        final long initialDelay) {
    this(name, stopper, period, initialDelay, DEFAULT_TIME_UNIT);
  }

  /**
   * @param name Name assigned to Chore. Useful for identification amongst chores of the same type
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param period Period in Timeunit unit with which this Chore repeats execution when scheduled.
   * @param initialDelay Delay in Timeunit unit before this Chore begins to execute once it has been
   *          scheduled. A value of 0 means the chore will begin to execute immediately. Negative
   *          delays are invalid and will be corrected to a value of 0.
   * @param unit The unit that is used to measure period and initialDelay
   */
  public ScheduledChore(final String name, Stoppable stopper, final int period,
                        final long initialDelay, final TimeUnit unit) {
    this.name = name;
    this.stopper = stopper;
    this.period = period;
    this.initialDelay = initialDelay < 0 ? 0 : initialDelay;
    this.timeUnit = unit;
  }

  /**
   * @see java.lang.Runnable#run()
   */
  @Override
  public void run() {
    updateTimeTrackingBeforeRun();
    if (missedStartTime() && isScheduled()) {
      onChoreMissedStartTime();
      LOG.info("Chore: {} missed its start time", getName());
    } else if (stopper.isStopped() || !isScheduled()) {
      // call shutdown here to cleanup the ScheduledChore.
      shutdown(false);
      LOG.info("Chore: {} was stopped", getName());
    } else {
      try {
        // TODO: Histogram metrics per chore name.
        // For now, just measure and log if DEBUG level logging is enabled.
        long start = 0;
        if (LOG.isDebugEnabled()) {
          start = System.nanoTime();
        }
        if (!initialChoreComplete) {
          initialChoreComplete = initialChore();
        } else {
          chore();
        }
        if (LOG.isDebugEnabled() && start > 0) {
          long end = System.nanoTime();
          LOG.debug("{} execution time: {} ms.", getName(),
              TimeUnit.NANOSECONDS.toMillis(end - start));
        }
      } catch (Throwable t) {
        LOG.error("Caught error", t);
        if (this.stopper.isStopped()) {
          cancel(false);
        }
      }
    }
  }

  /**
   * Update our time tracking members. Called at the start of an execution of this chore's run()
   * method so that a correct decision can be made as to whether or not we missed the start time
   */
  private synchronized void updateTimeTrackingBeforeRun() {
    timeOfLastRun = timeOfThisRun;
    timeOfThisRun = System.currentTimeMillis();
  }

  /**
   * Notify the ChoreService that this chore has missed its start time. Allows the ChoreService to
   * make the decision as to whether or not it would be worthwhile to increase the number of core
   * pool threads
   */
  private synchronized void onChoreMissedStartTime() {
    if (choreService != null) {
      choreService.onChoreMissedStartTime(this);
    }
  }

  /**
   * @return How long in millis has it been since this chore last run. Useful for checking if the
   *         chore has missed its scheduled start time by too large of a margin
   */
  synchronized long getTimeBetweenRuns() {
    return timeOfThisRun - timeOfLastRun;
  }

  /**
   * @return true when the time between runs exceeds the acceptable threshold
   */
  private synchronized boolean missedStartTime() {
    return isValidTime(timeOfLastRun) && isValidTime(timeOfThisRun)
        && getTimeBetweenRuns() > getMaximumAllowedTimeBetweenRuns();
  }

  /**
   * @return max allowed time in millis between runs.
   */
  private double getMaximumAllowedTimeBetweenRuns() {
    // Threshold used to determine if the Chore's current run started too late
    return 1.5 * timeUnit.toMillis(period);
  }

  /**
   * @param time in system millis
   * @return true if time is earlier or equal to current milli time
   */
  private synchronized boolean isValidTime(final long time) {
    return time > 0 && time <= System.currentTimeMillis();
  }

  /**
   * @return false when the Chore is not currently scheduled with a ChoreService
   */
  public synchronized boolean triggerNow() {
    if (choreService == null) {
      return false;
    }
    choreService.triggerNow(this);
    return true;
  }

  @RestrictedApi(explanation = "Should only be called in ChoreService", link = "",
      allowedOnPath = ".*/org/apache/hadoop/hbase/ChoreService.java")
  synchronized void setChoreService(ChoreService service) {
    choreService = service;
    timeOfThisRun = -1;
  }

  public synchronized void cancel() {
    cancel(true);
  }

  public synchronized void cancel(boolean mayInterruptIfRunning) {
    if (isScheduled()) {
      choreService.cancelChore(this, mayInterruptIfRunning);
    }
    choreService = null;
  }

  public String getName() {
    return name;
  }

  public Stoppable getStopper() {
    return stopper;
  }

  /**
   * @return period to execute chore in getTimeUnit() units
   */
  public int getPeriod() {
    return period;
  }

  /**
   * @return initial delay before executing chore in getTimeUnit() units
   */
  public long getInitialDelay() {
    return initialDelay;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public synchronized boolean isInitialChoreComplete() {
    return initialChoreComplete;
  }

  synchronized ChoreService getChoreService() {
    return choreService;
  }

  synchronized long getTimeOfLastRun() {
    return timeOfLastRun;
  }

  synchronized long getTimeOfThisRun() {
    return timeOfThisRun;
  }

  /**
   * @return true when this Chore is scheduled with a ChoreService
   */
  public synchronized boolean isScheduled() {
    return choreService != null && choreService.isChoreScheduled(this);
  }

  @InterfaceAudience.Private
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public synchronized void choreForTesting() {
    chore();
  }

  /**
   * The task to execute on each scheduled execution of the Chore
   */
  protected abstract void chore();

  /**
   * Override to run a task before we start looping.
   * @return true if initial chore was successful
   */
  protected boolean initialChore() {
    // Default does nothing
    return true;
  }

  /**
   * Override to run cleanup tasks when the Chore encounters an error and must stop running
   */
  protected void cleanup() {
  }

  /**
   * Call {@link #shutdown(boolean)} with {@code true}.
   * @see ScheduledChore#shutdown(boolean)
   */
  public synchronized void shutdown() {
    shutdown(true);
  }

  /**
   * Completely shutdown the ScheduleChore, which means we will call cleanup and you should not
   * schedule it again.
   * <p/>
   * This is another path to cleanup the chore, comparing to stop the stopper instance passed in.
   */
  public synchronized void shutdown(boolean mayInterruptIfRunning) {
    cancel(mayInterruptIfRunning);
    cleanup();
  }

  /**
   * A summation of this chore in human readable format. Downstream users should not presume
   * parsing of this string can relaibly be done between versions. Instead, they should rely
   * on the public accessor methods to get the information they desire.
   */
  @InterfaceAudience.Private
  @Override
  public String toString() {
    return "ScheduledChore name=" + getName() + ", period=" + getPeriod() +
        ", unit=" + getTimeUnit();
  }
}
