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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWritePool;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Payload files written in the background ({@link PayloadWriter#write} on an {@link OutOfLineWritePool}): the same files as a
 * write on the task's thread (one marker each, created before its file, the bytes, the calling task's write token in the name),
 * a failure thrown at the task's next submit and at its barrier, the bytes held for pending writes bounded, and the threads
 * stopped only after the writes they started end.
 */
class TestPayloadWriterInBackground {

  private static final String INSTANT = "20261110120000000";
  private static final String ROW = "source_path_sha=s modality=image chunk_id=";

  @TempDir
  Path temp;

  /** Marker calls, in order, as "directory/name". */
  private final List<String> marked = Collections.synchronizedList(new ArrayList<>());

  private OutOfLineWriter writer(HoodieStorage storage, String base, long markerMillis) {
    TimelineServerBasedWriteMarkers markers = mock(TimelineServerBasedWriteMarkers.class);
    when(markers.create(anyString(), anyString(), eq(IOType.CREATE))).thenAnswer(invocation -> {
      String path = invocation.getArgument(0) + "/" + invocation.getArgument(1);
      assertFalse(Files.exists(Paths.get(new StoragePath(base, path).toUri())), "the marker comes before its file: " + path);
      if (markerMillis > 0) {
        // by design: simulates a marker request waiting for the timeline server's next batch; not a wait for a condition
        Thread.sleep(markerMillis);
      }
      marked.add(path);
      return Option.of(new StoragePath(path));
    });
    return new OutOfLineWriter(storage, base, INSTANT, Option.of(markers));
  }

  private HoodieStorage storage(String base) {
    return spy(new HoodieHadoopStorage(new StoragePath(base), HadoopFSUtils.getStorageConf(new Configuration())));
  }

  private static byte[] payload(int size, long seed) {
    byte[] bytes = new byte[size];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  @Test
  void backgroundWritesLeaveTheFilesAWriteOnTheTaskThreadLeaves() throws IOException {
    String base = temp.toUri().toString();
    OutOfLineWriter writer = writer(storage(base), base, 0);
    List<String> paths = new ArrayList<>();
    List<byte[]> payloads = new ArrayList<>();
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      for (int i = 0; i < 40; i++) {
        byte[] bytes = payload(20_000 + i, i);
        payloads.add(bytes);
        paths.add(PayloadWriter.write(writer, pool, new PayloadWriter.Payload(ROW + i, "p", bytes.length), bytes));
      }
      pool.awaitAll();
    }
    String onTaskThread = writer.write("p", out -> out.write(1));
    String tokenOnTaskThread = FSUtils.getWriteTokenFromBaseFile(onTaskThread.substring(onTaskThread.lastIndexOf('/') + 1));
    assertEquals(40, paths.stream().distinct().count(), "a fresh file per payload");
    for (int i = 0; i < paths.size(); i++) {
      String relative = paths.get(i);
      assertTrue(relative.startsWith(".hoodie/.blob/p/"), relative);
      String name = relative.substring(relative.lastIndexOf('/') + 1);
      assertEquals(INSTANT, FSUtils.getCommitTime(name));
      assertEquals(tokenOnTaskThread, FSUtils.getWriteTokenFromBaseFile(name), "named on the calling thread, with its token");
      assertArrayEquals(payloads.get(i), Files.readAllBytes(temp.resolve(relative)));
    }
    List<String> expected = new ArrayList<>(paths);
    expected.add(onTaskThread);
    List<String> markedSorted = new ArrayList<>(marked);
    markedSorted.sort(null);
    expected.sort(null);
    assertEquals(expected, markedSorted, "one marker per file");
  }

  @Test
  void failedWriteIsThrownAtTheNextSubmitAndAtTheBarrier() throws IOException {
    String base = temp.toUri().toString();
    HoodieStorage storage = storage(base);
    OutOfLineWriter writer = writer(storage, base, 0);
    IOException refused = new IOException("PUT refused");
    doAnswer(invocation -> {
      throw refused;
    }).when(storage).create(org.mockito.ArgumentMatchers.any(StoragePath.class), eq(false));
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      byte[] bytes = payload(100, 1);
      PayloadWriter.write(writer, pool, new PayloadWriter.Payload(ROW + 0, "p", bytes.length), bytes);
      EmbeddingPayloadWriteException atBarrier = assertThrows(EmbeddingPayloadWriteException.class, pool::awaitAll);
      assertSame(refused, atBarrier.getCause(), "the storage failure, as a write on the task thread reports it");
      assertTrue(atBarrier.getMessage().contains(ROW + 0), atBarrier.getMessage());
      PayloadWriter.Payload second = new PayloadWriter.Payload(ROW + 1, "p", bytes.length);
      EmbeddingPayloadWriteException atSubmit = assertThrows(EmbeddingPayloadWriteException.class,
          () -> PayloadWriter.write(writer, pool, second, bytes));
      assertSame(atBarrier, atSubmit);
    }
  }

  /** A payload handed to a closed pool is a write failure of the round, never a failure of the row's content. */
  @Test
  void writeAfterTheTaskClosedItsPoolIsAWriteFailure() {
    String base = temp.toUri().toString();
    OutOfLineWriter writer = writer(storage(base), base, 0);
    OutOfLineWritePool pool = new OutOfLineWritePool();
    pool.close();
    byte[] bytes = payload(10, 2);
    PayloadWriter.Payload target = new PayloadWriter.Payload(ROW + 9, "p", bytes.length);
    EmbeddingPayloadWriteException refused = assertThrows(EmbeddingPayloadWriteException.class,
        () -> PayloadWriter.write(writer, pool, target, bytes));
    assertTrue(refused.getMessage().contains("closed"), refused.getMessage());
    assertTrue(marked.isEmpty(), "nothing marked");
  }

  @Test
  void pendingBytesAreBoundedAndCloseWaitsForStartedWrites() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch lastRunning = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    OutOfLineWritePool pool = new OutOfLineWritePool();
    int quarter = OutOfLineWritePool.MAX_PENDING_BYTES / 4;
    for (int i = 0; i < 4; i++) {
      boolean last = i == 3;
      pool.submit(quarter, () -> {
        release.await();
        if (last) {
          lastRunning.countDown();
          // a running write that ignores the interrupt close() sends, as a store call may
          long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200);
          while (System.nanoTime() < until) {
            try {
              // by design: simulates a write that ignores interrupts for 200 ms; not a wait
              Thread.sleep(10);
            } catch (InterruptedException ignored) {
              // keeps going
            }
          }
          finished.set(true);
        }
      });
    }
    // the budget is taken: one more byte waits for a write to end
    CountDownLatch submitted = new CountDownLatch(1);
    Thread fifth = new Thread(() -> {
      pool.submit(1, () -> { });
      submitted.countDown();
    });
    fifth.start();
    assertFalse(submitted.await(300, TimeUnit.MILLISECONDS), "a submit past the pending-bytes budget waits");
    release.countDown();
    assertTrue(submitted.await(10, TimeUnit.SECONDS), "it goes on once earlier writes end");
    fifth.join();
    // close only once the last write is past its latch: close interrupts running writes, and this one must be running
    assertTrue(lastRunning.await(10, TimeUnit.SECONDS));
    pool.close();
    assertTrue(finished.get(), "close returns only after the writes already started ended");
  }

  /**
   * Before and after on one machine: 64 frames of 60 KB through one writer whose marker calls wait 50 ms (the timeline server's
   * default batch interval), on the task thread and in the background. Runs with {@code -Dout.of.line.bench.out=<file>}.
   */
  @Test
  void benchmarkOutOfLineFrameWrites() throws IOException {
    String out = System.getProperty("out.of.line.bench.out");
    org.junit.jupiter.api.Assumptions.assumeTrue(out != null && !out.isEmpty(), "set -Dout.of.line.bench.out to run");
    int frames = 64;
    List<byte[]> payloads = new ArrayList<>();
    for (int i = 0; i < frames; i++) {
      payloads.add(payload(60_000, i));
    }
    String syncBase = temp.resolve("sync").toUri().toString();
    OutOfLineWriter sync = writer(storage(syncBase), syncBase, 50);
    long start = System.nanoTime();
    for (int i = 0; i < frames; i++) {
      byte[] bytes = payloads.get(i);
      PayloadWriter.stream(sync, new PayloadWriter.Payload(ROW + i, "p", bytes.length), () -> new java.io.ByteArrayInputStream(bytes));
    }
    long syncMillis = (System.nanoTime() - start) / 1_000_000L;
    String asyncBase = temp.resolve("async").toUri().toString();
    OutOfLineWriter async = writer(storage(asyncBase), asyncBase, 50);
    start = System.nanoTime();
    try (OutOfLineWritePool pool = new OutOfLineWritePool()) {
      for (int i = 0; i < frames; i++) {
        byte[] bytes = payloads.get(i);
        PayloadWriter.write(async, pool, new PayloadWriter.Payload(ROW + i, "p", bytes.length), bytes);
      }
      pool.awaitAll();
    }
    long asyncMillis = (System.nanoTime() - start) / 1_000_000L;
    assertEquals(2 * frames, marked.size());
    String report = "frames=" + frames + " frameBytes=60000 markerMillis=50 syncMillis=" + syncMillis + " backgroundMillis="
        + asyncMillis + " markerCalls(sync,background)=" + frames + "," + frames + "\n";
    Files.write(Paths.get(out), Arrays.asList(report.trim()), StandardCharsets.UTF_8);
    assertTrue(asyncMillis < syncMillis, report);
  }
}
