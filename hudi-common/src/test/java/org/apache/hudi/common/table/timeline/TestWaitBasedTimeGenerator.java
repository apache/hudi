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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestWaitBasedTimeGenerator {

  public static class MockInProcessLockProvider extends InProcessLockProvider {

    private static final ThreadLocal<Boolean> NEED_TO_LOCK_LATER = ThreadLocal.withInitial(() -> false);
    private static CountDownLatch SIGNAL;

    public static void initialize() {
      SIGNAL = new CountDownLatch(1);
    }

    public static void needToLockLater(Boolean lockLater) {
      NEED_TO_LOCK_LATER.set(lockLater);
    }

    public MockInProcessLockProvider(LockConfiguration lockConfiguration, Configuration conf) {
      super(lockConfiguration, conf);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      if (NEED_TO_LOCK_LATER.get()) {
        // Wait until another thread acquired lock firstly
        try {
          SIGNAL.await();
        } catch (InterruptedException e) {
          throw new HoodieLockException(e);
        }
      }
      boolean locked = super.tryLock(time, unit);
      if (locked) {
        SIGNAL.countDown();
      }
      return locked;
    }
  }

  // Clock skew time
  private final long clockSkewTime = 20L;

  private final Configuration hadoopConf = new Configuration();

  private HoodieTimeGeneratorConfig timeGeneratorConfig;

  @BeforeEach
  public void initialize() {
    timeGeneratorConfig = HoodieTimeGeneratorConfig.newBuilder()
        .withPath("test_wait_based")
        .withMaxExpectedClockSkewMs(25L)
        .withTimeGeneratorType(TimeGeneratorType.WAIT_TO_ADJUST_SKEW)
        .build();
    timeGeneratorConfig.setValue(HoodieTimeGeneratorConfig.LOCK_PROVIDER_KEY, MockInProcessLockProvider.class.getName());
    MockInProcessLockProvider.initialize();
  }

  /**
   * Two threads concurrently try to get current time, manually let's t2 is always slower than
   * t1, TimeGenerator needs to ensure monotonically increasing time generated no matter which
   * thread firstly acquired lock.
   *
   * To be more specific, pretend t2 is slower than t1 20ms, the MaxExpectedClockSkewMs is 25ms
   * 1. t1 acquired lock first,
   *    1> t1(before lock: 20ms) acquired lock at 20ms, then t1 holds lock for 25ms and 20 is returned.
   *    2> t2(before lock: 0ms) wait for about 25ms until lock is free, then holds lock for 25ms and 25 is returned.
   *    3> whereas t1's timestamp < t2's timestamp
   * 2. t2 acquired lock first,
   *    1> t2(before lock: 20ms) acquired lock at 20ms, then t2 holds lock for 25ms and 20 is returned.
   *    2> t1(before lock: 40ms) wait for about 25ms until lock is free, then holds lock for 25ms and 65 (40 + 25) is returned.
   *    3> whereas t1's timestamp > t2's timestamp
   * So no matter which thread firstly acquires lock, the first acquired thread's timestamp should be earlier.
   */
  @Disabled("This test is flaky, disable it for now. Fix in review -> https://github.com/apache/hudi/pull/9972")
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSlowerThreadLaterAcquiredLock(boolean slowerThreadAcquiredLockLater) throws InterruptedException {
    AtomicLong t1Timestamp = new AtomicLong(0L);
    Thread t1 = new Thread(() -> {
      try {
        MockInProcessLockProvider.needToLockLater(!slowerThreadAcquiredLockLater);
        TimeGenerator timeGenerator = TimeGenerators.getTimeGenerator(timeGeneratorConfig, hadoopConf);
        t1Timestamp.set(timeGenerator.currentTimeMillis(false));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    AtomicLong t2Timestamp = new AtomicLong(0L);
    Thread t2 = new Thread(() -> {
      try {
        MockInProcessLockProvider.needToLockLater(slowerThreadAcquiredLockLater);
        TimeGenerator timeGenerator = TimeGenerators.getTimeGenerator(timeGeneratorConfig, hadoopConf);
        // Pretend t2 is slower 20ms than t1
        t2Timestamp.set(timeGenerator.currentTimeMillis(false) - clockSkewTime);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    t1.start();
    t2.start();

    t1.join(60000);
    t2.join(60000);

    Assertions.assertTrue(t2Timestamp.get() != 0L);
    Assertions.assertTrue(t1Timestamp.get() != 0L);

    if (slowerThreadAcquiredLockLater) {
      Assertions.assertTrue(t2Timestamp.get() > t1Timestamp.get());
    } else {
      Assertions.assertTrue(t2Timestamp.get() < t1Timestamp.get());
    }
  }
}
