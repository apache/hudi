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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A parquet test source that proves continuous-mode multi-table syncs run in parallel.
 *
 * <p>Every table waits at a shared barrier before producing data, so a sequential implementation would block the first
 * table forever and time out. Only concurrent syncs let all tables pass the barrier.
 */
public class ContinuousTestSource extends ParquetDFSSource {

  // When set on a table's properties, that table fails right after passing the barrier, i.e. once all tables started.
  public static final String FAIL_AFTER_BARRIER = "hoodie.test.continuous.source.fail.after.barrier";

  // When set on a table's properties, that table blocks after the barrier until fail fast interrupts it.
  public static final String BLOCK_UNTIL_INTERRUPTED = "hoodie.test.continuous.source.block.until.interrupted";

  private static final long BARRIER_TIMEOUT_SECONDS = 60;

  private static volatile CyclicBarrier startBarrier = new CyclicBarrier(1);
  // Counted down by a blocking table once it observes the fail-fast interrupt, so a test can assert it was torn down.
  private static volatile CountDownLatch blockedTableInterrupted = new CountDownLatch(1);

  private final boolean failAfterBarrier;
  private final boolean blockUntilInterrupted;
  private final AtomicBoolean barrierPassed = new AtomicBoolean(false);

  public ContinuousTestSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.failAfterBarrier = props.getBoolean(FAIL_AFTER_BARRIER, false);
    this.blockUntilInterrupted = props.getBoolean(BLOCK_UNTIL_INTERRUPTED, false);
  }

  // Resets the shared barrier to expect one arrival per table, and the interrupt latch. Call before each sync.
  public static void resetBarrier(int numTables) {
    startBarrier = new CyclicBarrier(numTables);
    blockedTableInterrupted = new CountDownLatch(1);
  }

  // Whether a blocking table has already observed the fail-fast interrupt.
  public static boolean wasBlockedTableInterrupted() {
    return blockedTableInterrupted.getCount() == 0;
  }

  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    // Only rendezvous once, on the first fetch, so that later empty polls do not block termination.
    if (barrierPassed.compareAndSet(false, true)) {
      awaitBarrier();
      if (failAfterBarrier) {
        throw new HoodieException("Simulated table sync failure after all tables started");
      }
      if (blockUntilInterrupted) {
        blockUntilInterrupted();
      }
    }
    return super.fetchNextBatch(lastCheckpoint, sourceLimit);
  }

  // Blocks until fail fast interrupts this table, records that it observed the interrupt, then propagates it.
  private void blockUntilInterrupted() {
    // Nothing counts this down. Only an interrupt can end.
    CountDownLatch blockIndefinitely = new CountDownLatch(1);
    try {
      blockIndefinitely.await();
    } catch (InterruptedException e) {
      blockedTableInterrupted.countDown();
      Thread.currentThread().interrupt();
      throw new HoodieException("Table sync was interrupted by fail fast", e);
    }
  }

  private void awaitBarrier() {
    try {
      startBarrier.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new HoodieException("Timed out waiting for all tables to start syncing; continuous-mode syncs are not "
          + "running in parallel", e);
    } catch (BrokenBarrierException e) {
      throw new HoodieException("Barrier was broken while waiting for all tables to start syncing", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted while waiting for all tables to start syncing", e);
    }
  }
}
