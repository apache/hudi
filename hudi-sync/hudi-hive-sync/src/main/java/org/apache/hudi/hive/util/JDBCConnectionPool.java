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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;

/**
 * Pool of JDBC {@link Connection} instances for parallel HiveQL DDL via JDBC mode.
 *
 * <p>JDBC {@code Connection} is not thread-safe but is cheap to construct (one TCP
 * socket to HiveServer2). Connections are opened eagerly at pool construction. The
 * pool exposes {@link #runOnEachConnection(java.util.List)} for setup statements
 * that must run on every connection before any partition fan-out (e.g.
 * {@code USE database}), which {@code JDBCExecutor} uses to handle the Hive 2.x
 * quirk where {@code ALTER PARTITION ... SET LOCATION} ignores {@code db.tbl}
 * qualifiers and silently uses the connection's current database.
 *
 * <p><b>Usage contract:</b> use this pool only for partition-row DDL statements
 * that are independent of each other and freely shuffleable across workers.
 * Table-level statements (createTable, schema evolution, schema reads) must
 * continue to run on the session connection held by {@link
 * org.apache.hudi.hive.ddl.JDBCExecutor} on the sync driver thread, and the
 * shared session connection is also what {@link
 * org.apache.hudi.hive.ddl.JDBCBasedMetadataOperator} uses for its read/write of
 * table parameters.
 *
 * <p>Gated behind {@code hoodie.datasource.hive_sync.batching.enabled} and
 * constructed only for JDBC sync mode.
 */
public class JDBCConnectionPool implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCConnectionPool.class);

  private final ArrayBlockingQueue<Connection> available;
  private final List<Connection> all;
  private final ExecutorService executor;
  private final int size;
  private volatile boolean closed;

  public JDBCConnectionPool(HiveSyncConfig config, int size) {
    this(buildConnections(config, size), size);
  }

  // Package-private for tests: accepts a pre-built list of connections so we can
  // exercise borrow/return/close semantics without a live HiveServer2.
  JDBCConnectionPool(List<Connection> connections, int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Pool size must be >= 1, got " + size);
    }
    if (connections.size() != size) {
      throw new IllegalArgumentException("Expected " + size + " connections, got " + connections.size());
    }
    this.size = size;
    this.available = new ArrayBlockingQueue<>(size);
    this.all = new ArrayList<>(connections);
    this.available.addAll(connections);
    this.executor = Executors.newFixedThreadPool(size, new PoolThreadFactory());
    LOG.info("Initialized JDBCConnectionPool with {} connections", size);
  }

  private static List<Connection> buildConnections(HiveSyncConfig config, int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Pool size must be >= 1, got " + size);
    }
    String jdbcUrl = config.getStringOrDefault(HIVE_URL);
    String user = config.getStringOrDefault(HIVE_USER);
    String pass = config.getStringOrDefault(HIVE_PASS);
    try {
      // Defensive: the Hive JDBC driver is normally already loaded by JDBCExecutor;
      // load it here too so the pool can be constructed in isolation.
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      throw new HoodieException("Hive JDBC driver class not found on classpath", e);
    }
    List<Connection> connections = new ArrayList<>(size);
    try {
      for (int i = 0; i < size; i++) {
        connections.add(DriverManager.getConnection(jdbcUrl, user, pass));
      }
      return connections;
    } catch (SQLException e) {
      // Construction failed mid-way; close any connections we already built before
      // surfacing the error so we don't leak sockets.
      for (Connection c : connections) {
        try {
          c.close();
        } catch (SQLException ignore) {
          // intentional: best-effort cleanup during failure
        }
      }
      throw new HoodieException("Failed to construct JDBCConnectionPool of size " + size, e);
    }
  }

  /**
   * Runs each given SQL on <i>every</i> pooled connection, in order. Used for
   * setup statements (e.g. {@code USE database}) that must establish per-
   * connection session context before any partition fan-out. Blocks until all
   * connections complete. Throws on first error.
   */
  public void runOnEachConnection(List<String> setupSqls) throws Exception {
    if (closed) {
      throw new IllegalStateException("Cannot dispatch to a closed JDBCConnectionPool");
    }
    if (setupSqls.isEmpty()) {
      return;
    }
    List<Future<?>> futures = new ArrayList<>(all.size());
    for (Connection conn : all) {
      futures.add(executor.submit(() -> {
        for (String sql : setupSqls) {
          try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
          }
        }
        return null;
      }));
    }
    Exception firstError = null;
    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        if (firstError == null) {
          firstError = ie;
        }
      } catch (java.util.concurrent.ExecutionException ee) {
        Throwable cause = ee.getCause();
        if (firstError == null) {
          firstError = (cause instanceof Exception) ? (Exception) cause : ee;
        } else {
          LOG.warn("Additional setup SQL failed (suppressed in favor of first error)", cause);
        }
      }
    }
    if (firstError != null) {
      throw firstError;
    }
  }

  /**
   * Borrows a connection, runs the action, and returns the connection to the pool.
   * Blocks if all connections are in use until one becomes available.
   */
  public <T> T run(ConnectionAction<T> action) throws Exception {
    if (closed) {
      throw new IllegalStateException("Cannot borrow from a closed JDBCConnectionPool");
    }
    Connection conn = available.take();
    try {
      return action.apply(conn);
    } finally {
      if (!closed) {
        available.offer(conn);
      }
    }
  }

  /**
   * Worker thread pool sized to match the connection pool. Use this to fan out
   * batches so the number of in-flight statements cannot exceed the number of
   * pooled connections.
   */
  public ExecutorService executor() {
    return executor;
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
    executor.shutdown();
    try {
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    closeQuietly();
  }

  private void closeQuietly() {
    for (Connection conn : all) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOG.warn("Error closing pooled JDBC Connection", e);
      }
    }
    available.clear();
    all.clear();
  }

  @FunctionalInterface
  public interface ConnectionAction<T> {
    T apply(Connection conn) throws Exception;
  }

  private static final class PoolThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_ID = new AtomicInteger(0);
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String namePrefix = "hudi-hive-jdbc-pool-" + POOL_ID.incrementAndGet() + "-";

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, namePrefix + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    }
  }
}
