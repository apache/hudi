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

package org.apache.hudi.client.transaction;

import org.apache.hudi.client.transaction.lock.InMemoryStorageBasedLockProvider;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieLockException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two threads sharing one {@link TransactionManager}, as the ingest thread and the in-process async
 * compactor or clusterer do through HoodieStreamer's shared write client. Only one of them may be
 * inside a state change at a time, and the second must not release the first one's lock.
 */
public class TestTransactionManagerSharedAcrossThreads extends HoodieCommonTestHarness {

  @BeforeEach
  void setUp() throws IOException {
    initPath();
    initMetaClient();
    InMemoryStorageBasedLockProvider.reset();
  }

  @AfterEach
  void tearDown() {
    InMemoryStorageBasedLockProvider.reset();
  }

  // InProcessLockProvider is the control: it is owned per thread, so the second thread waits.
  @ParameterizedTest
  @ValueSource(classes = {InProcessLockProvider.class, InMemoryStorageBasedLockProvider.class})
  void secondThreadWaitsAndCannotReleaseHeldLock(Class<?> lockProviderClass) throws Exception {
    TransactionManager shared = new TransactionManager(writeConfig(lockProviderClass), metaClient.getStorage());
    TransactionManager otherWriter = new TransactionManager(writeConfig(lockProviderClass), metaClient.getStorage());
    Option<HoodieInstant> compaction = Option.of(INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, "compaction", "0000001"));
    CountDownLatch holderInside = new CountDownLatch(1);
    CountDownLatch releaseHolder = new CountDownLatch(1);

    // The compactor thread commits a compaction while holding the lock.
    Thread holder = new Thread(() -> {
      shared.beginStateChange(compaction, Option.empty());
      holderInside.countDown();
      try {
        releaseHolder.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        shared.endStateChange(compaction);
      }
    }, "compactor");
    try {
      holder.start();
      assertTrue(holderInside.await(30, TimeUnit.SECONDS), "the holder did not acquire the lock");

      // Meanwhile the ingest thread schedules a table service through the same transaction manager.
      AtomicBoolean secondEntered = new AtomicBoolean(false);
      Thread second = new Thread(() -> {
        try {
          shared.beginStateChange(Option.empty(), Option.empty());
          secondEntered.set(true);
          shared.endStateChange(Option.empty());
        } catch (HoodieLockException e) {
          // Waited for the holder and timed out: the expected outcome.
        }
      }, "ingest");
      second.start();
      second.join(TimeUnit.SECONDS.toMillis(30));

      boolean otherWriterAcquired = tryAcquire(otherWriter);
      assertAll(
          () -> assertFalse(secondEntered.get(),
              "a second thread entered the state change while another thread held the lock"),
          () -> assertFalse(otherWriterAcquired,
              "another writer acquired the lock while the first thread was still inside its state change"));
    } finally {
      releaseHolder.countDown();
      holder.join(TimeUnit.SECONDS.toMillis(30));
      shared.close();
      otherWriter.close();
    }
  }

  private static boolean tryAcquire(TransactionManager txnManager) {
    try {
      txnManager.beginStateChange(Option.empty(), Option.empty());
    } catch (HoodieLockException e) {
      return false;
    }
    txnManager.endStateChange(Option.empty());
    return true;
  }

  @SuppressWarnings("unchecked")
  private HoodieWriteConfig writeConfig(Class<?> lockProviderClass) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider((Class<? extends LockProvider>) lockProviderClass)
            .withClientNumRetries(0)
            .withLockWaitTimeInMillis(500L)
            .build())
        .build();
  }
}
