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

package org.apache.hudi.hive.util;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;

/**
 * Pool of Hive {@link Driver} + {@link SessionState} pairs for parallel HiveQL DDL.
 *
 * <p>Hive's {@code SessionState.start(state)} binds state to the calling thread's
 * thread-local, and {@code Driver} reads from that thread-local during {@code run()}.
 * A Driver constructed on one thread cannot be safely used from another. This pool
 * solves that by giving each slot its own dedicated worker thread (a single-thread
 * executor) — the Driver and SessionState are built on that thread by a bootstrap
 * task, and all subsequent SQL for that slot runs on the same thread.
 *
 * <p><b>Usage contract:</b> use this pool only for partition-row DDL statements that
 * are independent of each other and freely shuffleable across workers. Table-level
 * statements (createTable, schema evolution, USE database) must continue to run on
 * the session {@code Driver} held by {@code HiveQueryDDLExecutor} on the sync driver
 * thread. The pool is gated behind {@code hoodie.datasource.hive_sync.batching.enabled}
 * and is constructed only for HiveQL sync mode.
 */
public class HiveDriverPool implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveDriverPool.class);

  private final List<Worker> workers;
  private final int size;
  private volatile boolean closed;

  public HiveDriverPool(HiveSyncConfig config, int size) {
    this(config, size, new DefaultDriverFactory(config));
  }

  // Package-private for tests: accepts a DriverFactory so unit tests can inject
  // mock Driver instances without standing up a real Hive instance.
  HiveDriverPool(HiveSyncConfig config, int size, DriverFactory factory) {
    if (size < 1) {
      throw new IllegalArgumentException("Pool size must be >= 1, got " + size);
    }
    this.size = size;
    this.workers = new ArrayList<>(size);
    String databaseName = config.getStringOrDefault(META_SYNC_DATABASE_NAME);
    PoolThreadFactory threadFactory = new PoolThreadFactory();
    List<Future<Void>> bootstrapFutures = new ArrayList<>(size);
    try {
      for (int i = 0; i < size; i++) {
        Worker worker = new Worker(threadFactory);
        workers.add(worker);
        bootstrapFutures.add(worker.executor.submit(() -> {
          worker.driver = factory.newDriver(databaseName);
          return null;
        }));
      }
      // Block until all bootstraps complete so we surface construction errors
      // before any caller hands us SQL.
      for (Future<Void> f : bootstrapFutures) {
        f.get();
      }
    } catch (Exception e) {
      tearDown();
      throw new HoodieException("Failed to construct HiveDriverPool of size " + size, e);
    }
    LOG.info("Initialized HiveDriverPool with {} workers", size);
  }

  /**
   * Runs each given SQL on <i>every</i> worker, in order. Used for setup statements
   * (e.g. {@code USE database}) that must establish per-thread session context
   * before any partition statement runs. Blocks until all workers have completed
   * the setup. Throws on first error.
   */
  public void runOnEachWorker(List<String> setupSqls) {
    if (closed) {
      throw new IllegalStateException("Cannot dispatch to a closed HiveDriverPool");
    }
    if (setupSqls.isEmpty()) {
      return;
    }
    List<Future<?>> futures = new ArrayList<>(workers.size());
    for (Worker worker : workers) {
      futures.add(worker.executor.submit(() -> {
        for (String sql : setupSqls) {
          worker.driver.run(sql);
        }
        return null;
      }));
    }
    awaitAll(futures);
  }

  /**
   * Dispatches each SQL string to a worker (round-robin) and returns the list of
   * futures. The caller is responsible for awaiting and collecting errors.
   */
  public List<Future<?>> runAll(List<String> sqls) {
    if (closed) {
      throw new IllegalStateException("Cannot dispatch to a closed HiveDriverPool");
    }
    List<Future<?>> futures = new ArrayList<>(sqls.size());
    for (int i = 0; i < sqls.size(); i++) {
      String sql = sqls.get(i);
      Worker worker = workers.get(i % workers.size());
      futures.add(worker.executor.submit(() -> {
        long start = System.currentTimeMillis();
        worker.driver.run(sql);
        LOG.info("Time taken to execute [{}]: {} ms", sql, System.currentTimeMillis() - start);
        return null;
      }));
    }
    return futures;
  }

  /**
   * Awaits all futures, throws the first exception encountered (logging the rest at
   * WARN), and returns the list of CommandProcessorResponse objects (currently
   * unused but matches the existing single-threaded contract that returned them).
   */
  public List<CommandProcessorResponse> awaitAll(List<Future<?>> futures) {
    Exception firstError = null;
    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        if (firstError == null) {
          firstError = ie;
        }
      } catch (ExecutionException ee) {
        Exception cause = unwrap(ee);
        if (firstError == null) {
          firstError = cause;
        } else {
          LOG.warn("Additional SQL batch failed (suppressed in favor of first error)", cause);
        }
      }
    }
    if (firstError != null) {
      throw new HoodieHiveSyncException("Failed in executing SQL", firstError);
    }
    return new ArrayList<>();
  }

  private static Exception unwrap(ExecutionException ee) {
    Throwable cause = ee.getCause();
    return (cause instanceof Exception) ? (Exception) cause : ee;
  }

  public int size() {
    return size;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    tearDown();
  }

  private void tearDown() {
    // Close each worker's Driver/SessionState on its own thread, then shut the
    // executor down. Running close() on the bound thread keeps SessionState's
    // thread-local cleanup correct.
    for (Worker worker : workers) {
      try {
        worker.executor.submit(() -> {
          if (worker.driver != null) {
            try {
              worker.driver.close();
            } catch (Exception e) {
              LOG.warn("Error closing pooled Driver", e);
            }
          }
          SessionState ss = SessionState.get();
          if (ss != null) {
            try {
              ss.close();
            } catch (Exception e) {
              LOG.warn("Error closing pooled SessionState", e);
            }
          }
          return null;
        }).get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.warn("Error during pool worker shutdown", e);
      }
      worker.executor.shutdown();
      try {
        if (!worker.executor.awaitTermination(10, TimeUnit.SECONDS)) {
          worker.executor.shutdownNow();
        }
      } catch (InterruptedException ie) {
        worker.executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    workers.clear();
  }

  /**
   * Per-slot state: a single-thread executor and the Driver bound to its thread.
   * Driver is volatile because it is written by the bootstrap task and read by
   * subsequent dispatch tasks on the same executor.
   */
  private static final class Worker {
    final ExecutorService executor;
    volatile Driver driver;

    Worker(ThreadFactory threadFactory) {
      this.executor = Executors.newSingleThreadExecutor(threadFactory);
    }
  }

  @FunctionalInterface
  interface DriverFactory {
    Driver newDriver(String databaseName) throws Exception;
  }

  /**
   * Builds a real Hive {@link Driver} on the calling thread. The SessionState is
   * constructed lazily (once, on the first worker thread that builds a Driver) and
   * shared across all worker threads — Hive uses ThreadLocal attachment, not
   * exclusive ownership, so multiple workers calling
   * {@code SessionState.start(sharedState)} all see the same config and scratch dir
   * without each spending the cost of building their own SessionState (and risking
   * resource-dir creation races during the constructor).
   */
  private static final class DefaultDriverFactory implements DriverFactory {
    private final HiveConf hiveConf;
    private volatile SessionState sharedSessionState;

    DefaultDriverFactory(HiveSyncConfig config) {
      this.hiveConf = config.getHiveConf();
    }

    @Override
    public synchronized Driver newDriver(String databaseName) throws Exception {
      if (sharedSessionState == null) {
        sharedSessionState = new SessionState(hiveConf,
            UserGroupInformation.getCurrentUser().getShortUserName());
      }
      SessionState.start(sharedSessionState);
      sharedSessionState.setCurrentDatabase(databaseName);
      return new Driver(hiveConf);
    }
  }

  private static final class PoolThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_ID = new AtomicInteger(0);
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String namePrefix = "hudi-hive-driver-pool-" + POOL_ID.incrementAndGet() + "-";

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, namePrefix + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    }
  }
}
