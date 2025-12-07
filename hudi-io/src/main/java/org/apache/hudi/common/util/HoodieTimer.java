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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;

import lombok.Getter;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Timing utility to help keep track of execution times of code blocks. This class helps to allow multiple timers
 * started at the same time and automatically returns the execution time in the order in which the timers are stopped.
 */
public class HoodieTimer {

  // Ordered stack of TimeInfo's to make sure stopping the timer returns the correct elapsed time
  private final Deque<TimeInfo> timeInfoDeque = new ArrayDeque<>(1);

  /**
   * @deprecated please use either {@link HoodieTimer#start} or {@link HoodieTimer#create} APIs
   */
  @Deprecated
  public HoodieTimer() {
    this(false);
  }

  private HoodieTimer(boolean shouldStart) {
    if (shouldStart) {
      startTimer();
    }
  }

  static class TimeInfo {
    // captures the startTime of the code block
    @Getter
    long startTime;
    // is the timing still running for the last started timer
    @Getter
    boolean isRunning;

    public TimeInfo(long startTime) {
      this.startTime = startTime;
      this.isRunning = true;
    }

    public long stop() {
      this.isRunning = false;
      return System.currentTimeMillis() - startTime;
    }
  }

  public HoodieTimer startTimer() {
    timeInfoDeque.push(new TimeInfo(System.currentTimeMillis()));
    return this;
  }

  public long endTimer() {
    if (timeInfoDeque.isEmpty()) {
      throw new HoodieException("Timer was not started");
    }
    return timeInfoDeque.pop().stop();
  }

  /**
   * tries to end the timer but will not cause a runtime failure like endTimer will if the queue is empty
   * @return option of the timer duration, empty option if the queue is empty
   */
  public Option<Long> tryEndTimer() {
    if (timeInfoDeque.isEmpty()) {
      return Option.empty();
    }
    return Option.of(timeInfoDeque.pop().stop());
  }

  /**
   * Creates an instance of {@link HoodieTimer} already started
   */
  public static HoodieTimer start() {
    return new HoodieTimer(true);
  }

  /**
   * Creates an instance of {@link HoodieTimer} that is NOT started
   */
  public static HoodieTimer create() {
    return new HoodieTimer(false);
  }
}
