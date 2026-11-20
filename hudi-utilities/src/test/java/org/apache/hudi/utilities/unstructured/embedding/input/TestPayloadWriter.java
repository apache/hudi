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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriterTestExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Prepared payload writes against local storage: one fresh managed file per payload, written once with no rename, a failed write
 * removed and reported as an interruption naming the row and the step, and a failure of the payload's own stream kept as the
 * caller classified it. The shared writer's marker and target ordering is covered in TestOutOfLineWriter.
 */
@ExtendWith(OutOfLineWriterTestExtension.class)
class TestPayloadWriter {

  private static final String ROW = "source_path_sha=0123456789abcdef0123456789abcdef01234567 source_modification_time=1761868800000 "
      + "source_file_size=4096 modality=image chunk_id=3";
  private static final String PARTITION = "2026/10/14";
  private static final long THRESHOLD = 16;
  private static final String INSTANT = "20261018120000000";

  @TempDir
  Path temp;

  private String basePath;
  private HoodieStorage storage;
  private OutOfLineWriter writer;
  private String lastPath = ".hoodie/.blob/not-created.bin";

  @BeforeEach
  void setUp() {
    basePath = temp.resolve("embedding").toUri().toString();
    storage = spy(new HoodieHadoopStorage(new StoragePath(basePath), HadoopFSUtils.getStorageConf(new Configuration())));
    TimelineServerBasedWriteMarkers markers = mock(TimelineServerBasedWriteMarkers.class);
    when(markers.create(anyString(), anyString(), eq(IOType.CREATE))).thenAnswer(invocation -> {
      lastPath = invocation.getArgument(0) + "/" + invocation.getArgument(1);
      return Option.of(new StoragePath(lastPath));
    });
    writer = new OutOfLineWriter(storage, basePath, "20261027120000000", Option.of(markers));
  }

  private Path target() {
    return temp.resolve("embedding").resolve(lastPath);
  }

  private static byte[] bytes(int size, int fill) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) fill);
    return bytes;
  }

  /** The payload of the test row, of {@code length} bytes. */
  private static PayloadWriter.Payload payload(long length) {
    return new PayloadWriter.Payload(ROW, PARTITION, length);
  }

  private String write(byte[] bytes) {
    return PayloadWriter.stream(writer, payload(bytes.length), () -> new ByteArrayInputStream(bytes));
  }

  /** One create of the target and no rename. */
  private void verifyOneCreateNoRename() throws IOException {
    verify(storage, times(1)).create(any(StoragePath.class), anyBoolean());
    verify(storage, never()).rename(any(), any());
  }

  @Test
  void writesOnceWithOneCreateAndNoRename() throws Exception {
    byte[] payload = bytes(100, 1);
    String written = write(payload);
    assertEquals(lastPath, written, "the returned path is the marked one");
    verifyOneCreateNoRename();
    verify(storage).create(new StoragePath(basePath, lastPath), false);
    assertTrue(lastPath.startsWith(".hoodie/.blob/" + PARTITION + "/"), lastPath);
    assertArrayEquals(payload, Files.readAllBytes(target()));
  }

  /**
   * Prep's form: {@link PreparedInputs} with a destination writes the payload above the threshold as it prepares the row and
   * returns the reference only (inline field null); at the threshold the row stays INLINE and nothing is written.
   */
  @Test
  void preparedInputsWithADestinationWritesThePayloadAndKeepsTheReferenceOnly() throws Exception {
    byte[] payload = bytes(100, 1);
    PreparedInputs inputs = inputs();
    Row prepared = inputs.prepare(ROW, PARTITION, ImageContentTypes.JPEG, () -> new ByteArrayInputStream(payload),
        payload.length);
    inputs.awaitWrites();
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, prepared.getString(0));
    assertNull(prepared.get(1), "the stored row holds the reference only");
    Row reference = prepared.getStruct(2);
    assertTrue(reference.getString(0).matches("\\.hoodie/\\.blob/2026/10/14/[0-9a-f-]{36}-0_\\d+-\\d+-\\d+_\\d+\\.bin"),
        reference.getString(0));
    assertEquals(0L, reference.getLong(1));
    assertEquals(100L, reference.getLong(2));
    assertTrue(reference.getBoolean(3), "a payload file is managed by the embedding table");
    assertArrayEquals(payload, Files.readAllBytes(temp.resolve("embedding").resolve(reference.getString(0))));

    byte[] small = bytes((int) THRESHOLD, 2);
    Row inline = inputs().prepare(ROW, PARTITION, ImageContentTypes.PNG, () -> new ByteArrayInputStream(small), small.length);
    assertEquals(HoodieSchema.Blob.INLINE, inline.getString(0));
    assertArrayEquals(small, (byte[]) inline.get(1));
  }

  private PreparedInputs inputs() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(THRESHOLD));
    return new PreparedInputs(props, INSTANT, destination());
  }

  private PayloadDestination destination() {
    return new PayloadDestination(HadoopFSUtils.getStorageConf(new Configuration()), basePath, new TypedProperties());
  }

  /** The target's stream fails part way: the partial file is removed, and the write fails with the row and the step. */
  @Test
  void failureDeletesPartialAndThrowsWithFields() throws Exception {
    doAnswer(invocation -> {
      OutputStream real = (OutputStream) invocation.callRealMethod();
      return new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          // half the bytes reach the file, then the stream fails
          real.write(b, off, len / 2);
          real.flush();
          throw new IOException("injected stream failure");
        }

        @Override
        public void close() throws IOException {
          real.close();
        }
      };
    }).when(storage).create(any(StoragePath.class), eq(false));

    byte[] content = bytes(100, 1);
    EmbeddingPayloadWriteException e = assertThrows(EmbeddingPayloadWriteException.class, () -> write(content));

    assertEquals(ROW, e.getRow());
    assertEquals(PayloadWriter.STEP_WRITE, e.getStep());
    for (String field : new String[] {"row=" + ROW, "step=" + PayloadWriter.STEP_WRITE, "injected stream failure"}) {
      assertTrue(e.getMessage().contains(field), field + " in " + e.getMessage());
    }
    assertFalse(Files.exists(target()), "the partial payload file is removed");
  }

  /** A storage failure creating the payload file is an interruption naming the create step. */
  @Test
  void createFailureIsAnInterruption() throws IOException {
    doAnswer(invocation -> {
      throw new IOException("503 Slow Down");
    }).when(storage).create(any(StoragePath.class), anyBoolean());
    byte[] content = bytes(64, 9);
    EmbeddingPayloadWriteException failure = assertThrows(EmbeddingPayloadWriteException.class, () -> write(content));
    assertEquals(PayloadWriter.STEP_CREATE, failure.getStep());
    assertTrue(failure.getMessage().contains("503 Slow Down"), failure.getMessage());
  }

  /** A FileNotFoundException of the TARGET's write (an S3A 404 on completion) is an interruption like any write failure. */
  @Test
  void targetWriteFileNotFoundIsAnInterruption() throws IOException {
    doAnswer(invocation -> new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw new FileNotFoundException("upload gone");
      }
    }).when(storage).create(any(StoragePath.class), anyBoolean());
    byte[] content = bytes(64, 4);
    EmbeddingPayloadWriteException failure = assertThrows(EmbeddingPayloadWriteException.class, () -> write(content));
    assertEquals(PayloadWriter.STEP_WRITE, failure.getStep());
    assertTrue(failure.getCause() instanceof FileNotFoundException, String.valueOf(failure.getCause()));
  }

  /**
   * A failure of the payload's own stream (here a size mismatch the caller reports as an IllegalArgumentException, and a read
   * failure it reports as a HoodieIOException) propagates unchanged, never as a write failure, and leaves no file.
   */
  @Test
  void payloadStreamFailureKeepsTheCallersClassification() {
    IllegalArgumentException shortPayload = new IllegalArgumentException("payload shorter than declared");
    PayloadWriter.Payload shortTarget = payload(64);
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> PayloadWriter.stream(writer, shortTarget, () -> new FilterInputStream(new ByteArrayInputStream(bytes(64, 1))) {
          @Override
          public int read(byte[] buffer, int offset, int len) {
            throw shortPayload;
          }
        }));
    assertSame(shortPayload, thrown);
    assertFalse(Files.exists(target()), "the partial payload file is removed");

    PayloadWriter.Payload failingTarget = payload(64);
    HoodieIOException readFailure = assertThrows(HoodieIOException.class,
        () -> PayloadWriter.stream(writer, failingTarget, () -> new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("Connection reset");
          }
        }));
    assertEquals("Connection reset", readFailure.getCause().getMessage());
    assertFalse(Files.exists(target()), "the partial payload file is removed");
  }
}
