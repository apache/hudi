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

package org.apache.hudi.client.heartbeat;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieHeartbeatClient extends HoodieCommonTestHarness {

  private static String instantTime1 = "100";
  private static String instantTime2 = "101";
  private static Long heartBeatInterval = 1000L;
  private static int numTolerableMisses = 1;

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testStartHeartbeat() throws IOException {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getStorage(), metaClient.getBasePath().toString(),
            heartBeatInterval,
            numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    List<StoragePathInfo> listFiles = metaClient.getStorage().listDirectEntries(
        new StoragePath(hoodieHeartbeatClient.getHeartbeatFolderPath()));
    assertTrue(listFiles.size() == 1);
    assertTrue(listFiles.get(0).getPath().toString().contains(instantTime1));
  }

  @Test
  public void testStopHeartbeat() {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getStorage(), metaClient.getBasePath().toString(),
            heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    HoodieHeartbeatClient.Heartbeat heartbeat = hoodieHeartbeatClient.stop(instantTime1);
    await().atMost(5, SECONDS).until(() -> heartbeat.getNumHeartbeats() > 0);
    assertEquals(1, (int) heartbeat.getNumHeartbeats());
    assertNull(hoodieHeartbeatClient.getHeartbeat(instantTime1), "Heartbeat should be removed from client cache after explicit stop");
  }

  @Test
  public void testIsHeartbeatExpired() throws IOException {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getStorage(), metaClient.getBasePath().toString(),
            heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    hoodieHeartbeatClient.stop(instantTime1);
    assertTrue(hoodieHeartbeatClient.isHeartbeatExpired(instantTime1), "The explicit stopped instant is deemed expiry for heartbeats");
  }

  @Test
  public void testNumHeartbeatsGenerated() {
    Long heartBeatInterval = 5000L;
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getStorage(), metaClient.getBasePath().toString(),
            heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start("100");
    await().atMost(5, SECONDS).until(() -> hoodieHeartbeatClient.getHeartbeat(instantTime1).getNumHeartbeats() >= 1);
  }

  @Test
  public void testDeleteWrongHeartbeat() {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getStorage(), metaClient.getBasePath().toString(),
            heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    hoodieHeartbeatClient.stop(instantTime1);
    assertFalse(
        WriterHeartbeatUtils.deleteHeartbeatFile(metaClient.getStorage(), basePath, instantTime2));
  }

  @Test
  public void testStopHeartbeatTimers() throws IOException {
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(metaClient.getStorage(), metaClient.getBasePath().toString(),
            heartBeatInterval, numTolerableMisses);
    hoodieHeartbeatClient.start(instantTime1);
    hoodieHeartbeatClient.stopHeartbeatTimers();
    assertFalse(hoodieHeartbeatClient.isHeartbeatExpired(instantTime1));
    assertTrue(hoodieHeartbeatClient.getHeartbeat(instantTime1).isHeartbeatStopped());
  }

  /**
   * Regression test for the heartbeat-expiry incident: a single slow/hung storage write must not
   * block (freeze) the heartbeat scheduler thread. The first heartbeat write blocks (simulating a hung
   * cloud-storage call); we assert the scheduler keeps producing heartbeats on fresh threads once that
   * write times out, proving the scheduler thread was not blocked by the synchronous storage call (#1).
   * A high tolerable-misses is used so that recovery after the blocked write does not itself trip the
   * expiry path (which intentionally stops refresh on a genuine lapse).
   */
  @Test
  public void testSlowHeartbeatWriteDoesNotBlockScheduler() {
    CountDownLatch releaseFirstWrite = new CountDownLatch(1);
    SlowCreateStorage slowStorage =
        new SlowCreateStorage((FileSystem) metaClient.getStorage().getFileSystem(), releaseFirstWrite);
    // interval 1s, write timeout = 1s; high tolerable-misses so the ~1s recovery gap stays well within
    // the allowable window and the scheduler keeps beating rather than treating it as a lapse.
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(slowStorage, metaClient.getBasePath().toString(),
            heartBeatInterval, 10);
    try {
      hoodieHeartbeatClient.start(instantTime1);
      // Despite the first write hanging, the scheduler must keep generating heartbeats on fresh threads.
      await().atMost(15, SECONDS)
          .until(() -> hoodieHeartbeatClient.getHeartbeat(instantTime1).getNumHeartbeats() >= 2);
    } finally {
      releaseFirstWrite.countDown();
      hoodieHeartbeatClient.close();
    }
  }

  @Test
  public void testScheduledHeartbeatRetriesAfterWriteFailure() {
    FailOnceAfterInitialCreateStorage storage =
        new FailOnceAfterInitialCreateStorage((FileSystem) metaClient.getStorage().getFileSystem());
    HoodieHeartbeatClient hoodieHeartbeatClient =
        new HoodieHeartbeatClient(storage, metaClient.getBasePath().toString(), heartBeatInterval, 10);
    try {
      hoodieHeartbeatClient.start(instantTime1);
      await().atMost(10, SECONDS).until(storage::hasInjectedFailure);
      await().atMost(10, SECONDS)
          .until(() -> hoodieHeartbeatClient.getHeartbeat(instantTime1).getNumHeartbeats() >= 2);
    } finally {
      hoodieHeartbeatClient.close();
    }
  }

  /**
   * A storage wrapper whose first {@code create()} call blocks until released, simulating a hung
   * storage write. All subsequent calls delegate normally.
   */
  private static class SlowCreateStorage extends HoodieHadoopStorage {

    private final AtomicBoolean firstCall = new AtomicBoolean(true);
    private final CountDownLatch releaseFirstWrite;

    SlowCreateStorage(FileSystem fs, CountDownLatch releaseFirstWrite) {
      super(fs);
      this.releaseFirstWrite = releaseFirstWrite;
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
      if (firstCall.getAndSet(false)) {
        try {
          releaseFirstWrite.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while simulating a hung heartbeat write", e);
        }
      }
      return super.create(path, overwrite);
    }
  }

  private static class FailOnceAfterInitialCreateStorage extends HoodieHadoopStorage {

    private final AtomicInteger createCalls = new AtomicInteger(0);
    private final AtomicBoolean injectedFailure = new AtomicBoolean(false);

    FailOnceAfterInitialCreateStorage(FileSystem fs) {
      super(fs);
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
      int currentCall = createCalls.incrementAndGet();
      if (currentCall == 2 && injectedFailure.compareAndSet(false, true)) {
        throw new IOException("Injected scheduled heartbeat write failure");
      }
      return super.create(path, overwrite);
    }

    private boolean hasInjectedFailure() {
      return injectedFailure.get();
    }
  }
}
