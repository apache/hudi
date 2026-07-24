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
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
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

  // Per-worker Driver construction has to be fast in practice (a few hundred ms
  // for the SessionState + Driver init). A 60s ceiling per worker leaves plenty of
  // headroom for a slow JVM warm-up but bounds the failure mode if the metastore
  // is unreachable or Hive hangs during init.
  private static final long BOOTSTRAP_TIMEOUT_SECONDS = 60;

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
    try {
      // Bootstrap workers one at a time (not concurrently): each worker builds its
      // own exclusively-owned SessionState, and constructing several SessionStates
      // in parallel risks racing on shared scratch-dir creation. This only affects
      // one-time pool startup cost, not per-statement dispatch latency.
      for (int i = 0; i < size; i++) {
        Worker worker = new Worker(threadFactory);
        workers.add(worker);
        worker.executor.submit(() -> {
          worker.driver = factory.newDriver(databaseName);
          worker.sessionState = SessionState.get();
          return null;
        }).get(BOOTSTRAP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
   * in-flight futures — this method does not block. The caller is responsible for
   * awaiting completion via {@link #awaitAll(List)} and collecting errors. SQL text
   * is intentionally not logged per-statement here: batched TOUCH/ADD statements can
   * be many kilobytes, and N parallel workers would multiply the log volume. See
   * {@link #awaitAll(List)} for the per-call summary log.
   */
  public List<Future<?>> dispatchAll(List<String> sqls) {
    if (closed) {
      throw new IllegalStateException("Cannot dispatch to a closed HiveDriverPool");
    }
    List<Future<?>> futures = new ArrayList<>(sqls.size());
    for (int i = 0; i < sqls.size(); i++) {
      String sql = sqls.get(i);
      Worker worker = workers.get(i % workers.size());
      futures.add(worker.executor.submit(() -> {
        worker.driver.run(sql);
        return null;
      }));
    }
    return futures;
  }

  /**
   * Awaits all futures and throws the first exception encountered. On first failure,
   * cancels the remaining (not yet started) futures so workers don't keep running
   * pointless work after a fatal error. Any errors that finished before cancellation
   * are logged at WARN. Callers do not need per-statement results (Hive's Driver.run
   * side-effects the metastore), so this method is void.
   */
  public void awaitAll(List<Future<?>> futures) {
    long start = System.currentTimeMillis();
    Exception firstError = null;
    int completed = 0;
    int cancelled = 0;
    for (int i = 0; i < futures.size(); i++) {
      Future<?> f = futures.get(i);
      try {
        f.get();
        completed++;
      } catch (CancellationException ce) {
        // We cancelled this future ourselves after a prior error. Don't treat it
        // as a new failure; just note it for the summary log.
        cancelled++;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        if (firstError == null) {
          firstError = ie;
          cancelled += cancelRemaining(futures, i + 1);
        }
      } catch (ExecutionException ee) {
        Exception cause = unwrap(ee);
        if (firstError == null) {
          firstError = cause;
          cancelled += cancelRemaining(futures, i + 1);
        } else {
          LOG.warn("Additional SQL batch failed (suppressed in favor of first error)", cause);
        }
      }
    }
    if (firstError != null) {
      throw new HoodieHiveSyncException("Failed in executing SQL", firstError);
    }
    LOG.info("Completed {} SQL statements ({} cancelled) in {} ms across {} workers",
        completed, cancelled, System.currentTimeMillis() - start, size);
  }

  private static int cancelRemaining(List<Future<?>> futures, int fromIndex) {
    int cancelled = 0;
    for (int j = fromIndex; j < futures.size(); j++) {
      // mayInterruptIfRunning=false: the worker thread is bound to a Hive Driver
      // whose state we don't want to corrupt mid-statement. Cancel only those that
      // haven't started yet; in-flight statements run to completion.
      if (futures.get(j).cancel(false)) {
        cancelled++;
      }
    }
    return cancelled;
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
    // Close each worker's own Driver and SessionState on its own thread, then shut
    // the executor down. Each worker owns an exclusive SessionState (see
    // DefaultDriverFactory), so there is no cross-worker close ordering to worry
    // about here — closing worker i never affects worker j.
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
          if (worker.sessionState != null) {
            try {
              worker.sessionState.close();
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
   * Per-slot state: a single-thread executor and the Driver + SessionState bound to
   * its thread. Both are volatile because they are written by the bootstrap task and
   * read by subsequent dispatch/teardown tasks on the same executor.
   */
  private static final class Worker {
    final ExecutorService executor;
    volatile Driver driver;
    volatile SessionState sessionState;

    Worker(ThreadFactory threadFactory) {
      this.executor = Executors.newSingleThreadExecutor(threadFactory);
    }
  }

  @FunctionalInterface
  interface DriverFactory {
    Driver newDriver(String databaseName) throws Exception;
  }

  /**
   * Builds a real Hive {@link Driver} on the calling thread, backed by a
   * {@link SessionState} that is exclusively owned by that thread (not shared with
   * any other worker). Hive's session-scoped state (current database, scratch
   * directories, and the transaction/lock manager under {@code DbTxnManager}) is
   * mutated by {@code Driver.run()} and is not safe for concurrent use from multiple
   * threads, so each worker must have its own instance. Bootstrap of all workers is
   * done sequentially by the pool constructor specifically so these constructions
   * don't race each other (e.g. on scratch-dir creation).
   */
  private static final class DefaultDriverFactory implements DriverFactory {
    private final HiveConf hiveConf;

    DefaultDriverFactory(HiveSyncConfig config) {
      this.hiveConf = config.getHiveConf();
    }

    @Override
    public Driver newDriver(String databaseName) throws Exception {
      SessionState sessionState = new SessionState(hiveConf,
          UserGroupInformation.getCurrentUser().getShortUserName());
      sessionState.setCurrentDatabase(databaseName);
      SessionState.start(sessionState);
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
