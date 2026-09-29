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
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
   * stop() must not delete the heartbeat file while a scheduled refresh is still writing it. A
   * refresh landing after the delete recreates the file, and on storage that enforces preconditions
   * it changes the object generation so a generation-matched delete is rejected (e.g. GCS 412).
   */
  @Test
  public void testStopWaitsForInFlightHeartbeatRefresh() throws Exception {
    // A longer interval also widens the bounded-write timeout, so a slow CI box cannot let the write
    // time out before the test releases it, which would let the delete run first.
    long interval = 5000L;
    CountDownLatch refreshEntered = new CountDownLatch(1);
    CountDownLatch releaseRefresh = new CountDownLatch(1);
    OrderRecordingStorage storage = new OrderRecordingStorage(
        (FileSystem) metaClient.getStorage().getFileSystem(), refreshEntered, releaseRefresh);
    HoodieHeartbeatClient client = new HoodieHeartbeatClient(
        storage, metaClient.getBasePath().toString(), interval, numTolerableMisses);
    try {
      client.start(instantTime1);
      assertTrue(refreshEntered.await(20, SECONDS), "Scheduled heartbeat refresh never started");

      Thread stopper = new Thread(() -> client.stop(instantTime1));
      stopper.start();
      // Without awaiting termination, stop() runs straight through and never parks here.
      await().atMost(20, SECONDS).until(() -> stopper.getState() == Thread.State.TIMED_WAITING
          || stopper.getState() == Thread.State.WAITING
          || stopper.getState() == Thread.State.BLOCKED);

      releaseRefresh.countDown();
      stopper.join(SECONDS.toMillis(20));
      assertFalse(stopper.isAlive(), "stop() did not complete after the refresh was released");

      assertTrue(storage.events().contains("delete"), "stop() never deleted the heartbeat file");
      assertEquals("delete", storage.events().get(storage.events().size() - 1),
          "No heartbeat refresh may follow the delete; events were " + storage.events());
    } finally {
      releaseRefresh.countDown();
      client.close();
    }
  }

  /**
   * A rejected heartbeat delete must never propagate. HoodieStorage.deleteFile throws
   * HoodieIOException (unchecked) when the object still exists after the delete was refused, which is
   * what a generation-matched delete does on storage that enforces preconditions. Callers reach this
   * from postCommit, where the commit is already durable.
   */
  @Test
  public void testDeleteHeartbeatFileSwallowsHoodieIOException() {
    ThrowOnDeleteStorage storage =
        new ThrowOnDeleteStorage((FileSystem) metaClient.getStorage().getFileSystem());
    assertFalse(WriterHeartbeatUtils.deleteHeartbeatFile(storage, basePath, instantTime1),
        "A refused heartbeat delete must be reported via the return value, never thrown");
  }

  /** Interrupting a thread parked in stop() must re-assert the interrupt rather than swallow it. */
  @Test
  public void testStopReassertsInterruptWhileAwaiting() throws Exception {
    // A longer interval widens the bounded-write window, so the await is still parked when interrupted.
    long interval = 5000L;
    CountDownLatch refreshEntered = new CountDownLatch(1);
    CountDownLatch releaseRefresh = new CountDownLatch(1);
    OrderRecordingStorage storage = new OrderRecordingStorage(
        (FileSystem) metaClient.getStorage().getFileSystem(), refreshEntered, releaseRefresh);
    HoodieHeartbeatClient client = new HoodieHeartbeatClient(
        storage, metaClient.getBasePath().toString(), interval, numTolerableMisses);
    try {
      client.start(instantTime1);
      assertTrue(refreshEntered.await(20, SECONDS), "Scheduled heartbeat refresh never started");

      AtomicBoolean interruptPreserved = new AtomicBoolean();
      Thread stopper = new Thread(() -> {
        client.stop(instantTime1);
        interruptPreserved.set(Thread.currentThread().isInterrupted());
      });
      stopper.start();
      await().atMost(20, SECONDS).until(() -> stopper.getState() == Thread.State.TIMED_WAITING
          || stopper.getState() == Thread.State.WAITING
          || stopper.getState() == Thread.State.BLOCKED);
      stopper.interrupt();
      stopper.join(SECONDS.toMillis(20));

      assertFalse(stopper.isAlive(), "stop() did not return after being interrupted");
      assertTrue(interruptPreserved.get(), "stop() swallowed the interrupt");
    } finally {
      releaseRefresh.countDown();
      client.close();
    }
  }

  /** Refuses every delete with HoodieIOException, as a precondition-enforcing store does. */
  private static class ThrowOnDeleteStorage extends HoodieHadoopStorage {

    ThrowOnDeleteStorage(FileSystem fs) {
      super(fs);
    }

    @Override
    public boolean deleteFile(StoragePath path) {
      throw new HoodieIOException("Failed to delete invalid data file: " + path);
    }
  }

  /**
   * Records create/delete order and blocks the first scheduled refresh until released. The block
   * ignores interruption, as a storage write already in flight would.
   */
  private static class OrderRecordingStorage extends HoodieHadoopStorage {

    private final List<String> events = new CopyOnWriteArrayList<>();
    private final AtomicBoolean gated = new AtomicBoolean(false);
    private final CountDownLatch refreshEntered;
    private final CountDownLatch releaseRefresh;

    OrderRecordingStorage(FileSystem fs, CountDownLatch refreshEntered, CountDownLatch releaseRefresh) {
      super(fs);
      this.refreshEntered = refreshEntered;
      this.releaseRefresh = releaseRefresh;
    }

    List<String> events() {
      return events;
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
      // The first create is start()'s synchronous beat; gate only the first scheduled refresh.
      if (!events.isEmpty() && gated.compareAndSet(false, true)) {
        refreshEntered.countDown();
        boolean released = false;
        while (!released) {
          try {
            releaseRefresh.await();
            released = true;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      OutputStream stream = super.create(path, overwrite);
      events.add("create");
      return stream;
    }

    @Override
    public boolean deleteFile(StoragePath path) throws IOException {
      events.add("delete");
      return super.deleteFile(path);
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
