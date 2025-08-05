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

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.metrics.MetricsReporterType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTransactionManager extends HoodieCommonTestHarness {
  HoodieWriteConfig writeConfig;
  TransactionManager transactionManager;

  @BeforeEach
  public void init(TestInfo testInfo) throws IOException {
    initPath();
    initMetaClient();
    this.writeConfig = getWriteConfig(testInfo.getTags().contains("useLockProviderWithRuntimeError"));
    this.transactionManager = new TransactionManager(this.writeConfig, this.metaClient.getStorage());
  }

  private HoodieWriteConfig getWriteConfig(boolean useLockProviderWithRuntimeError) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withCleanConfig(HoodieCleanConfig.newBuilder()
        .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
        .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(useLockProviderWithRuntimeError ? InProcessLockProviderWithRuntimeError.class : InProcessLockProvider.class)
            .withLockWaitTimeInMillis(1000L)
            .withRetryWaitTimeInMillis(1000L)
            .withClientRetryWaitTimeInMillis(1000L)
            .build())
        .forTable("testtable")
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().withReporterType(MetricsReporterType.INMEMORY.toString()).withLockingMetrics(true).on(true).build())
        .build();
  }

  @Test
  public void testSingleWriterTransaction() {
    Option<HoodieInstant> lastCompletedInstant = getInstant("0000001");
    Option<HoodieInstant> newTxnOwnerInstant = getInstant("0000002");
    transactionManager.beginStateChange(newTxnOwnerInstant, lastCompletedInstant);
    transactionManager.endStateChange(newTxnOwnerInstant);
  }

  @Test
  public void testSingleWriterNestedTransaction() {
    Option<HoodieInstant> lastCompletedInstant = getInstant("0000001");
    Option<HoodieInstant> newTxnOwnerInstant = getInstant("0000002");
    transactionManager.beginStateChange(newTxnOwnerInstant, lastCompletedInstant);

    Option<HoodieInstant> lastCompletedInstant1 = getInstant("0000003");
    Option<HoodieInstant> newTxnOwnerInstant1 = getInstant("0000004");

    assertDoesNotThrow(() -> transactionManager.beginStateChange(newTxnOwnerInstant1, lastCompletedInstant1));

    transactionManager.endStateChange(newTxnOwnerInstant);
    assertDoesNotThrow(() -> {
      transactionManager.endStateChange(newTxnOwnerInstant1);
    });
  }

  @Test
  public void testMultiWriterTransactions() throws InterruptedException {
    final long awaitMaxTimeoutMs = 2000L;
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger executionCounter = new AtomicInteger(0);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Function to handle CountDownLatch.await with InterruptedException handling
    Function<CountDownLatch, Void> awaitWithTimeout = l -> {
      try {
        l.await(awaitMaxTimeoutMs, TimeUnit.MILLISECONDS);
        return null;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };

    Option<HoodieInstant> lastCompletedInstant1 = getInstant("0000001");
    Option<HoodieInstant> newTxnOwnerInstant1 = getInstant("0000002");
    Option<HoodieInstant> lastCompletedInstant2 = getInstant("0000003");
    Option<HoodieInstant> newTxnOwnerInstant2 = getInstant("0000004");

    // When: Let writer1 get the lock first, then wait for the second writer to also grab the lock
    // to join the sync up point.
    Thread writer1 = new Thread(() -> {
      assertDoesNotThrow(() -> transactionManager.beginStateChange(newTxnOwnerInstant1, lastCompletedInstant1));
      // grab lock and allow writer 2 to be started; ensuring determinism for lock acquisition.
      latch.countDown();
      assertEquals(0, executionCounter.getAndIncrement());
      assertDoesNotThrow(() -> transactionManager.endStateChange(newTxnOwnerInstant1));
      writer1Completed.set(true);
    });
    writer1.start();

    // Writer2 will block on trying to acquire the lock
    // and will eventually get the lock before the timeout.
    Thread writer2 = new Thread(() -> {
      awaitWithTimeout.apply(latch); // wait till writer 1 grabs lock.
      assertDoesNotThrow(() -> transactionManager.beginStateChange(newTxnOwnerInstant2, lastCompletedInstant2));
      // should see the increment above if endStateChange(1) precedes beginStateChange(2)
      assertEquals(1, executionCounter.getAndIncrement());
      assertDoesNotThrow(() -> transactionManager.endStateChange(newTxnOwnerInstant2));
      writer2Completed.set(true);
    });
    writer2.start();

    // Let writer1 and writer2 wait at sync point
    writer1.join();
    writer2.join();

    //Then: Make sure both writers actually completed good
    Assertions.assertTrue(writer1Completed.get());
    Assertions.assertTrue(writer2Completed.get());
  }

  @Test
  public void testEndStateChangeByDiffOwner() throws InterruptedException {
    // 1. Begin and end by the same transaction owner
    Option<HoodieInstant> lastCompletedInstant = getInstant("0000001");
    Option<HoodieInstant> newTxnOwnerInstant = getInstant("0000002");
    transactionManager.beginStateChange(newTxnOwnerInstant, lastCompletedInstant);

    CountDownLatch countDownLatch = new CountDownLatch(1);
    // Another writer thread
    Thread writer2 = new Thread(() -> {
      Option<HoodieInstant> newTxnOwnerInstant1 = getInstant("0000003");
      transactionManager.endStateChange(newTxnOwnerInstant1);
      countDownLatch.countDown();
    });

    writer2.start();
    countDownLatch.await(30, TimeUnit.SECONDS);
    // should not have reset the state within transaction manager since the owner is different.
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner().isPresent());

    transactionManager.endStateChange(newTxnOwnerInstant);
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
  }

  @Test
  public void testTransactionsWithInstantTime() {
    // 1. Begin and end by the same transaction owner
    Option<HoodieInstant> lastCompletedInstant = getInstant("0000001");
    Option<HoodieInstant> newTxnOwnerInstant = getInstant("0000002");
    transactionManager.beginStateChange(newTxnOwnerInstant, lastCompletedInstant);
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner() == newTxnOwnerInstant);
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner() == lastCompletedInstant);
    transactionManager.endStateChange(newTxnOwnerInstant);
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 2. Begin transaction with a new txn owner, but end transaction with wrong owner
    lastCompletedInstant = getInstant("0000002");
    newTxnOwnerInstant = getInstant("0000003");
    transactionManager.beginStateChange(newTxnOwnerInstant, lastCompletedInstant);
    transactionManager.endStateChange(getInstant("0000004"));
    // Owner reset would not happen as the end txn was invoked with an incorrect current txn owner
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner() == newTxnOwnerInstant);
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner() == lastCompletedInstant);
    transactionManager.endStateChange(newTxnOwnerInstant);

    // 3. But, we should be able to begin a new transaction for a new owner
    lastCompletedInstant = getInstant("0000003");
    newTxnOwnerInstant = getInstant("0000004");
    transactionManager.beginStateChange(newTxnOwnerInstant, lastCompletedInstant);
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner() == newTxnOwnerInstant);
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner() == lastCompletedInstant);
    transactionManager.endStateChange(newTxnOwnerInstant);
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 4. Transactions with new instants but with same timestamps should properly reset owners
    transactionManager.beginStateChange(getInstant("0000005"), Option.empty());
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
    transactionManager.endStateChange(getInstant("0000005"));
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 6. Transactions with no owners should also go through
    transactionManager.beginStateChange();
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
    transactionManager.endStateChange();
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
  }

  @Test
  public void testGenerateInstantTimeFailsWithoutLock() {
    assertThrows(HoodieLockException.class, () -> transactionManager.generateInstantTime());
  }

  @Test
  @Tag("useLockProviderWithRuntimeError")
  public void testTransactionsWithUncheckedLockProviderRuntimeException() {
    assertThrows(RuntimeException.class, () -> {
      try {
        transactionManager.beginStateChange(Option.empty(), Option.empty());
      } finally {
        transactionManager.endStateChange(Option.empty());
      }
    });
  }

  private Option<HoodieInstant> getInstant(String timestamp) {
    return Option.of(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, timestamp));
  }
}
