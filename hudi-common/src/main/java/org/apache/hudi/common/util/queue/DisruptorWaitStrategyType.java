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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Enum for the type of waiting strategy in Disruptor Queue.
 */
@EnumDescription("Strategy employed for making Disruptor Executor wait on a cursor.")
public enum DisruptorWaitStrategyType {

  /**
   * The BlockingWaitStrategy is the slowest of the available wait strategies, but is the most conservative with the respect to CPU usage
   * and will give the most consistent behaviour across the widest variety of deployment options.
   */
  @EnumFieldDescription("The slowest of the available wait strategies. However, it is the most conservative with the respect to CPU usage and "
      + "will give the most consistent behaviour across the widest variety of deployment options.")
  BLOCKING_WAIT,

  /**
   * Like the `BlockingWaitStrategy` the `SleepingWaitStrategy` it attempts to be conservative with CPU usage by using a simple busy wait loop.
   * The difference is that the `SleepingWaitStrategy` uses a call to `LockSupport.parkNanos(1)` in the middle of the loop.
   * On a typical Linux system this will pause the thread for around 60µs.
   */
  @EnumFieldDescription("Like the `BLOCKING_WAIT` strategy, it attempts to be conservative with CPU usage by using a simple busy wait loop. "
   + "The difference is that the `SLEEPING_WAIT` strategy uses a call to `LockSupport.parkNanos(1)` in the middle of the loop. "
   + "On a typical Linux system this will pause the thread for around 60µs.")
  SLEEPING_WAIT,

  /**
   * The `YieldingWaitStrategy` is one of two WaitStrategies that can be used in low-latency systems.
   * It is designed for cases where there is the option to burn CPU cycles with the goal of improving latency.
   * The `YieldingWaitStrategy` will busy spin, waiting for the sequence to increment to the appropriate value.
   * Inside the body of the loop `Thread#yield()` will be called allowing other queued threads to run.
   * This is the recommended wait strategy when you need very high performance, and the number of `EventHandler` threads is lower than the total number of logical cores,
   * e.g. you have hyper-threading enabled.
   */
  @EnumFieldDescription("The `YIELDING_WAIT` strategy is one of two wait strategy that can be used in low-latency systems. "
    + "It is designed for cases where there is an opportunity to burn CPU cycles with the goal of improving latency. "
    + "The `YIELDING_WAIT` strategy will busy spin, waiting for the sequence to increment to the appropriate value. "
    + "Inside the body of the loop `Thread#yield()` will be called allowing other queued threads to run. "
    + "This is the recommended wait strategy when you need very high performance, and the number of `EventHandler` threads is lower than the total number of logical cores, "
    + "such as when hyper-threading is enabled.")
  YIELDING_WAIT,

  /**
   * The `BusySpinWaitStrategy` is the highest performing WaitStrategy.
   * Like the `YieldingWaitStrategy`, it can be used in low-latency systems, but puts the highest constraints on the deployment environment.
   */
  @EnumFieldDescription("The `BUSY_SPIN_WAIT` strategy is the highest performing wait strategy. "
    + "Like the `YIELDING_WAIT` strategy, it can be used in low-latency systems, but puts the highest constraints on the deployment environment.")
  BUSY_SPIN_WAIT;

  public static List<String> getNames() {
    List<String> names = new ArrayList<>(KeyGeneratorType.values().length);
    Arrays.stream(KeyGeneratorType.values())
        .forEach(x -> names.add(x.name()));
    return names;
  }
}
