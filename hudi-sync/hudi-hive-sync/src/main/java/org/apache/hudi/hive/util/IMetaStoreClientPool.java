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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool of {@link IMetaStoreClient} instances for parallel partition sync.
 *
 * <p>Each pooled client wraps an independent Thrift connection to the Hive Metastore.
 * Callers borrow a client via {@link #run(ClientAction)}, which blocks until a client
 * is available, executes the action, and returns the client to the pool. A worker
 * thread pool of the same size is exposed via {@link #executor()} so callers can fan
 * out their batches to match the number of available clients.
 *
 * <p><b>Usage contract:</b> pool clients must be used <i>only</i> for partition-row
 * operations — {@code add_partitions}, {@code alter_partitions}, {@code dropPartition},
 * {@code getPartition}. Table-row operations ({@code createTable}, {@code alter_table},
 * {@code getTable} used as the read half of a read-modify-write of table parameters)
 * must continue to go through the session client held by
 * {@code HoodieHiveSyncClient.client} on the sync driver thread. Mixing the two would
 * risk lost updates on table parameters such as the last-commit-time-synced marker.
 *
 * <p>The pool is gated behind {@code hoodie.datasource.hive_sync.batching.enabled} and
 * is constructed only for sync mode HMS.
 */
public class IMetaStoreClientPool implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(IMetaStoreClientPool.class);

  private final ArrayBlockingQueue<IMetaStoreClient> available;
  private final List<IMetaStoreClient> all;
  private final ExecutorService executor;
  private final int size;
  private volatile boolean closed;

  public IMetaStoreClientPool(HiveSyncConfig config, int size) {
    this(buildClients(config, size), size);
  }

  // Package-private for tests: accepts a pre-built list of clients so we can
  // exercise borrow/return/close semantics without a live metastore.
  IMetaStoreClientPool(List<IMetaStoreClient> clients, int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Pool size must be >= 1, got " + size);
    }
    if (clients.size() != size) {
      throw new IllegalArgumentException("Expected " + size + " clients, got " + clients.size());
    }
    this.size = size;
    this.available = new ArrayBlockingQueue<>(size);
    this.all = new ArrayList<>(clients);
    this.available.addAll(clients);
    this.executor = Executors.newFixedThreadPool(size, new PoolThreadFactory());
    LOG.info("Initialized IMetaStoreClient pool with {} clients", size);
  }

  private static List<IMetaStoreClient> buildClients(HiveSyncConfig config, int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Pool size must be >= 1, got " + size);
    }
    HiveConf hiveConf = config.getHiveConf();
    List<IMetaStoreClient> clients = new ArrayList<>(size);
    try {
      for (int i = 0; i < size; i++) {
        clients.add(newClient(hiveConf));
      }
      return clients;
    } catch (Exception e) {
      // Construction failed mid-way; close any clients we already built before
      // surfacing the error so we don't leak Thrift sockets.
      for (IMetaStoreClient c : clients) {
        try {
          c.close();
        } catch (Exception ignore) {
          // intentional: best-effort cleanup during failure
        }
      }
      throw new HoodieException("Failed to construct IMetaStoreClient pool of size " + size, e);
    }
  }

  private static IMetaStoreClient newClient(HiveConf hiveConf) {
    try {
      // RetryingMetaStoreClient.getProxy returns an independent IMetaStoreClient
      // (one Thrift socket per call), bypassing the Hive thread-local cache that
      // Hive.get(conf) would use. This is what gives us N truly independent clients.
      return RetryingMetaStoreClient.getProxy(hiveConf, true);
    } catch (Exception e) {
      throw new HoodieException("Failed to create IMetaStoreClient for pool", e);
    }
  }

  /**
   * Borrows a client, runs the action, and returns the client to the pool.
   * Blocks if all clients are in use until one becomes available.
   */
  public <T> T run(ClientAction<T> action) throws Exception {
    if (closed) {
      throw new IllegalStateException("Cannot borrow from a closed IMetaStoreClient pool");
    }
    IMetaStoreClient client = available.take();
    try {
      return action.apply(client);
    } finally {
      // Always return the client to the pool, even on failure. Thrift clients
      // recover transparently from transactional errors at the HMS layer;
      // RetryingMetaStoreClient handles transient socket failures internally.
      if (!closed) {
        available.offer(client);
      }
    }
  }

  /**
   * Worker thread pool sized to match the client pool. Use this to fan out
   * batches so the number of in-flight Thrift calls cannot exceed the
   * number of pooled clients.
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
    for (IMetaStoreClient client : all) {
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn("Error closing pooled IMetaStoreClient", e);
      }
    }
    available.clear();
    all.clear();
  }

  @FunctionalInterface
  public interface ClientAction<T> {
    T apply(IMetaStoreClient client) throws Exception;
  }

  private static final class PoolThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_ID = new AtomicInteger(0);
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String namePrefix = "hudi-hive-sync-pool-" + POOL_ID.incrementAndGet() + "-";

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, namePrefix + threadId.incrementAndGet());
      t.setDaemon(true);
      return t;
    }
  }
}
