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
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieLockException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestTransactionManager extends HoodieCommonTestHarness {
  HoodieWriteConfig writeConfig;
  TransactionManager transactionManager;

  @BeforeEach
  private void init() throws IOException {
    initPath();
    initMetaClient();
    this.writeConfig = getWriteConfig();
    this.transactionManager = new TransactionManager(this.writeConfig, this.metaClient.getFs());
  }

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .build())
        .build();
  }

  @Test
  public void testSingleWriterTransaction() {
    transactionManager.beginTransaction();
    transactionManager.endTransaction();
  }

  @Test
  public void testSingleWriterNestedTransaction() {
    transactionManager.beginTransaction();
    assertThrows(HoodieLockException.class, () -> {
      transactionManager.beginTransaction();
    });

    transactionManager.endTransaction();
    assertThrows(HoodieLockException.class, () -> {
      transactionManager.endTransaction();
    });
  }

  @Test
  public void testMultiWriterTransactions() {
    final int threadCount = 3;
    final long awaitMaxTimeoutMs = 2000L;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    final AtomicBoolean writer1Completed = new AtomicBoolean(false);
    final AtomicBoolean writer2Completed = new AtomicBoolean(false);

    // Let writer1 get the lock first, then wait for others
    // to join the sync up point.
    Thread writer1 = new Thread(() -> {
      assertDoesNotThrow(() -> {
        transactionManager.beginTransaction();
      });
      latch.countDown();
      try {
        latch.await(awaitMaxTimeoutMs, TimeUnit.MILLISECONDS);
        // Following sleep is to make sure writer2 attempts
        // to try lock and to get blocked on the lock which
        // this thread is currently holding.
        Thread.sleep(50);
      } catch (InterruptedException e) {
        //
      }
      assertDoesNotThrow(() -> {
        transactionManager.endTransaction();
      });
      writer1Completed.set(true);
    });
    writer1.start();

    // Writer2 will block on trying to acquire the lock
    // and will eventually get the lock before the timeout.
    Thread writer2 = new Thread(() -> {
      latch.countDown();
      try {
        latch.await(awaitMaxTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        //
      }
      assertDoesNotThrow(() -> {
        transactionManager.beginTransaction();
      });
      assertDoesNotThrow(() -> {
        transactionManager.endTransaction();
      });
      writer2Completed.set(true);
    });
    writer2.start();

    // Let writer1 and writer2 wait at the sync up
    // point to make sure they run in parallel and
    // one get blocked by the other.
    latch.countDown();
    try {
      writer1.join();
      writer2.join();
    } catch (InterruptedException e) {
      //
    }

    // Make sure both writers actually completed good
    Assertions.assertTrue(writer1Completed.get());
    Assertions.assertTrue(writer2Completed.get());
  }

  @Test
  public void testTransactionsWithInstantTime() {
    // 1. Begin and end by the same transaction owner
    Option<HoodieInstant> lastCompletedInstant = getInstant("0000001");
    Option<HoodieInstant> newTxnOwnerInstant = getInstant("0000002");
    transactionManager.beginTransaction(newTxnOwnerInstant, lastCompletedInstant);
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner() == newTxnOwnerInstant);
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner() == lastCompletedInstant);
    transactionManager.endTransaction(newTxnOwnerInstant);
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 2. Begin transaction with a new txn owner, but end transaction with no/wrong owner
    lastCompletedInstant = getInstant("0000002");
    newTxnOwnerInstant = getInstant("0000003");
    transactionManager.beginTransaction(newTxnOwnerInstant, lastCompletedInstant);
    transactionManager.endTransaction();
    // Owner reset would not happen as the end txn was invoked with an incorrect current txn owner
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner() == newTxnOwnerInstant);
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner() == lastCompletedInstant);

    // 3. But, we should be able to begin a new transaction for a new owner
    lastCompletedInstant = getInstant("0000003");
    newTxnOwnerInstant = getInstant("0000004");
    transactionManager.beginTransaction(newTxnOwnerInstant, lastCompletedInstant);
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner() == newTxnOwnerInstant);
    Assertions.assertTrue(transactionManager.getLastCompletedTransactionOwner() == lastCompletedInstant);
    transactionManager.endTransaction(newTxnOwnerInstant);
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 4. Transactions with no owners should also go through
    transactionManager.beginTransaction();
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
    transactionManager.endTransaction();
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 5. Transactions with new instants but with same timestamps should properly reset owners
    transactionManager.beginTransaction(getInstant("0000005"), Option.empty());
    Assertions.assertTrue(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
    transactionManager.endTransaction(getInstant("0000005"));
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());

    // 6. Transactions with no owners should also go through
    transactionManager.beginTransaction(Option.empty(), Option.empty());
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
    transactionManager.endTransaction(Option.empty());
    Assertions.assertFalse(transactionManager.getCurrentTransactionOwner().isPresent());
    Assertions.assertFalse(transactionManager.getLastCompletedTransactionOwner().isPresent());
  }

  private Option<HoodieInstant> getInstant(String timestamp) {
    return Option.of(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, timestamp));
  }
}
