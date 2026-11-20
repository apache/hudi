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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEmbeddingColumnInfoAtomicSwap {
  @TempDir
  Path temp;

  @Test
  void localReplacementReadersSeeOnlyACompleteOldOrNewFile() throws Exception {
    byte[] oldBytes = new byte[8192];
    byte[] newBytes = new byte[16384];
    Arrays.fill(oldBytes, (byte) 'a');
    Arrays.fill(newBytes, (byte) 'b');
    Path target = temp.resolve("embedding-column-info.json");
    Files.write(target, oldBytes);
    HoodieStorage storage = EmbeddingColumnInfoAtomicSwap.metadataStorage(HoodieStorageUtils.getStorage(
        new StoragePath(temp.toUri()), HoodieTestUtils.getDefaultStorageConf()));
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger reads = new AtomicInteger();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    Future<?> reader = pool.submit(() -> {
      while (running.get()) {
        try {
          byte[] observed = Files.readAllBytes(target);
          assertTrue(Arrays.equals(oldBytes, observed) || Arrays.equals(newBytes, observed), "reader saw partial bytes");
          reads.incrementAndGet();
        } catch (IOException e) {
          throw new AssertionError("atomic replacement made the durable path unavailable", e);
        }
      }
    });
    try {
      EmbeddingColumnInfoAtomicSwap swap = new EmbeddingColumnInfoAtomicSwap();
      for (int i = 0; i < 50; i++) {
        Path scratch = temp.resolve("scratch-" + i);
        Files.write(scratch, i % 2 == 0 ? newBytes : oldBytes);
        swap.replace(storage, new StoragePath(scratch.toUri()), new StoragePath(target.toUri()));
      }
    } finally {
      running.set(false);
      try {
        reader.get();
      } finally {
        pool.shutdownNow();
      }
    }
    assertTrue(reads.get() > 0, "concurrent reader exercised replacement");
    assertArrayEquals(oldBytes, Files.readAllBytes(target));
    assertFalse(Files.exists(temp.resolve(".embedding-column-info.json.crc")));
    try (java.io.InputStream normal = HoodieStorageUtils.getStorage(new StoragePath(target.toUri()),
        HoodieTestUtils.getDefaultStorageConf()).open(new StoragePath(target.toUri()))) {
      assertArrayEquals(oldBytes, normal.readAllBytes(), "ordinary checksum-aware Hadoop reader accepts raw metadata");
    }
  }

  @Test
  void localChecksumSidecarsAndUnsupportedBackendsRefuseWithoutChangingOldBytes() throws Exception {
    Path target = temp.resolve("embedding-column-info.json");
    Path scratch = temp.resolve("scratch");
    byte[] old = "old-complete".getBytes(StandardCharsets.UTF_8);
    Files.write(target, old);
    Files.write(scratch, "new-complete".getBytes(StandardCharsets.UTF_8));
    Files.write(temp.resolve(".embedding-column-info.json.crc"), new byte[] {1});
    HoodieStorage local = EmbeddingColumnInfoAtomicSwap.metadataStorage(HoodieStorageUtils.getStorage(
        new StoragePath(temp.toUri()), HoodieTestUtils.getDefaultStorageConf()));
    EmbeddingColumnInfoAtomicSwap swap = new EmbeddingColumnInfoAtomicSwap();
    assertThrows(IOException.class, () -> swap.replace(local, new StoragePath(scratch.toUri()), new StoragePath(target.toUri())));
    assertArrayEquals(old, Files.readAllBytes(target));
    HoodieStorage unsupported = mock(HoodieStorage.class);
    when(unsupported.getScheme()).thenReturn("viewfs");
    assertThrows(IOException.class, () -> swap.replace(unsupported, new StoragePath(scratch.toUri()), new StoragePath(target.toUri())));
    assertArrayEquals(old, Files.readAllBytes(target));
  }

  @Test
  void failedObjectWriteAbortsWithoutClosingOrPublishingPartialBytes() throws Exception {
    AbortableTestOutput output = new AbortableTestOutput(true);
    EmbeddingColumnInfoAtomicSwap.AbortHandle handle = EmbeddingColumnInfoAtomicSwap.abortHandle(output);
    assertNotNull(handle);
    assertThrows(IOException.class, () -> EmbeddingColumnInfoAtomicSwap.writeAndClose(output, handle, new byte[] {1, 2, 3}));
    assertTrue(output.aborted);
    assertFalse(output.closed, "close would publish the failed partial object");
    assertFalse(output.published);
  }

  @Test
  void completeObjectWritePublishesAtCloseAndNonabortableStreamIsUnsupported() throws Exception {
    AbortableTestOutput output = new AbortableTestOutput(false);
    EmbeddingColumnInfoAtomicSwap.writeAndClose(output, EmbeddingColumnInfoAtomicSwap.abortHandle(output), new byte[] {1, 2, 3});
    assertTrue(output.closed);
    assertTrue(output.published);
    assertFalse(output.aborted);
    assertNull(EmbeddingColumnInfoAtomicSwap.abortHandle(new java.io.ByteArrayOutputStream()));
  }

  public static class AbortableTestOutput extends OutputStream {
    private final boolean fail;
    private boolean closed;
    private boolean aborted;
    private boolean published;

    public AbortableTestOutput(boolean fail) {
      this.fail = fail;
    }

    public boolean hasCapability(String capability) {
      return capability.equals("fs.capability.outputstream.abortable");
    }

    public AbortResult abort() {
      aborted = true;
      return new AbortResult(false, null);
    }

    @Override
    public void write(int value) throws IOException {
      if (fail) {
        throw new IOException("injected partial object write failure");
      }
    }

    @Override
    public void close() {
      closed = true;
      published = !aborted;
    }
  }

  @Test
  void abortCleanupFailureRefusesBackendBeforePublishing() throws Exception {
    IOException cleanup = new IOException("reported cleanup failure");
    ResultOutput output = new ResultOutput(new AbortResult(false, cleanup));
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getScheme()).thenReturn("s3a");
    when(storage.create(any(StoragePath.class), eq(false))).thenReturn(output);
    IOException refused = assertThrows(IOException.class,
        () -> new EmbeddingColumnInfoAtomicSwap().checkSupported(storage, new StoragePath("s3a://bucket/info.json")));
    assertSame(cleanup, refused);
    assertFalse(output.closed);
  }

  @Test
  void abortCleanupFailureIsSuppressedOnTheOriginalWriteFailure() throws Exception {
    IOException cleanup = new IOException("reported cleanup failure");
    ResultOutput output = new ResultOutput(new AbortResult(false, cleanup));
    IOException failure = assertThrows(IOException.class, () -> EmbeddingColumnInfoAtomicSwap.writeAndClose(
        output, EmbeddingColumnInfoAtomicSwap.abortHandle(output), new byte[] {1}));
    assertSame(output.writeFailure, failure);
    assertEquals(1, failure.getSuppressed().length);
    assertSame(cleanup, failure.getSuppressed()[0]);
    assertFalse(output.closed);
  }

  @Test
  void alreadyClosedProbeDoesNotEstablishAbortSupport() throws Exception {
    ResultOutput output = new ResultOutput(new AbortResult(true, null));
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getScheme()).thenReturn("s3a");
    when(storage.create(any(StoragePath.class), eq(false))).thenReturn(output);
    assertThrows(IOException.class,
        () -> new EmbeddingColumnInfoAtomicSwap().checkSupported(storage, new StoragePath("s3a://bucket/info.json")));
    assertFalse(output.closed);
  }

  public static class AbortResult {
    private final boolean closed;
    private final IOException cleanup;

    public AbortResult(boolean closed, IOException cleanup) {
      this.closed = closed;
      this.cleanup = cleanup;
    }

    public boolean alreadyClosed() {
      return closed;
    }

    public IOException anyCleanupException() {
      return cleanup;
    }
  }

  public static class ResultOutput extends OutputStream {
    private final AbortResult result;
    private final IOException writeFailure = new IOException("original write failure");
    private boolean closed;

    public ResultOutput(AbortResult result) {
      this.result = result;
    }

    public boolean hasCapability(String capability) {
      return capability.equals("fs.capability.outputstream.abortable");
    }

    public AbortResult abort() {
      return result;
    }

    @Override
    public void write(int value) throws IOException {
      throw writeFailure;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
