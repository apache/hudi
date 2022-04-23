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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.apache.hudi.exception.HoodieException;

public class WaitStrategyFactory {

  public static final String DEFAULT_STRATEGY = "BlockingWaitStrategy";

  /**
   * Build WaitStrategy for disruptor
   */
  public static WaitStrategy build(String name) {
    WaitStrategy waitStrategy = null;
    if ("BlockingWaitStrategy".equals(name)) {
      waitStrategy = new BlockingWaitStrategy();
    } else if ("BusySpinWaitStrategy".equals(name)) {
      waitStrategy = new BusySpinWaitStrategy();
    } else if ("SleepingWaitStrategy".equals(name)) {
      waitStrategy = new SleepingWaitStrategy();
    } else if ("YieldingWaitStrategy".equals(name)) {
      waitStrategy = new YieldingWaitStrategy();
    } else {
      throw new HoodieException("Invalid wait strategy: " + name);
    }
    return waitStrategy;
  }
}
