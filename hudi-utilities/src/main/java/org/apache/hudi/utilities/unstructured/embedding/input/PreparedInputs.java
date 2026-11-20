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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.sources.blob.BlobInlineCeiling;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageContentTypes;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWritePool;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;
import org.apache.hudi.utilities.unstructured.storage.StorageReferenceRules;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@code prepared_image} value of a derived payload (a video frame), built while the embedding round preprocesses (the
 * round's instant exists before the transform):
 * <ul>
 *   <li>at most the inline threshold ({@link EmbeddingInputConfig#inlineMaxBytes}: default 1 MiB; on a Parquet without
 *   per-column statistics 80 KiB, also its most): {@code (INLINE, bytes, null)};</li>
 *   <li>larger: {@code (OUT_OF_LINE, null, (<.hoodie/.blob/[<partition>/]<fileId>_<writeToken>_<instantTime>.bin>, 0, length,
 *   managed = true))}:
 *   the payload is written to that final file ({@link PayloadWriter} into the {@link PayloadDestination}, with
 *   timeline-server markers unless {@code hoodie.write.blob.markers.enable} is false) and the value carries the
 *   reference only, relative to the embedding table's base path. A payload of at most
 *   {@link OutOfLineWritePool#MAX_PENDING_BYTES} (an encoded frame) is read whole and its marker and file are written in the
 *   background, while the task prepares its next payloads; {@link #awaitWrites} waits for them before the task reports its
 *   end.</li>
 * </ul>
 * The payload is read as exactly its declared size: whole into the row when inline, whole and then written in the background
 * when out of line up to {@link OutOfLineWritePool#MAX_PENDING_BYTES}, streamed through the writer's bounded buffer when larger,
 * so a payload above that is never held in the heap. Inside a Spark task the inline
 * threshold is checked against the task's own {@link BlobInlineCeiling} before the first payload; the job-start check of the
 * same rule is {@link EmbeddingInputConfig#checkOnDriver}.
 */
public final class PreparedInputs implements Serializable, Closeable {

  private static final long serialVersionUID = 1L;
  /** Spark type of a stored input value: the {@link HoodieSchema.Blob} struct. */
  public static final StructType BLOB_STRUCT = (StructType) BlobType.dataType();
  public static final StructType REFERENCE_STRUCT =
      (StructType) BLOB_STRUCT.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
  /** The largest payload a row can carry: one Java array. */
  static final long MAX_PAYLOAD_BYTES = Integer.MAX_VALUE - 8L;

  private final long inlineMaxBytes;
  private final double inlineMaxHeapFraction;
  private final String instant;
  /** Where payloads are written as they are prepared. */
  private final PayloadDestination destination;
  private transient boolean taskCeilingChecked;
  /**
   * The background writes of this task's out-of-line payloads of at most {@link OutOfLineWritePool#MAX_PENDING_BYTES}; kept
   * once closed, so a write after {@link #close} is refused rather than given new threads. Volatile: the task's completion may
   * close it from another thread.
   */
  private transient AtomicReference<OutOfLineWritePool> writes = new AtomicReference<>();
  /** Set by {@link #close}: a write after it is refused, never given a new pool. */
  private transient volatile boolean closed;

  /**
   * @param instant     the prep instant that owns each payload CREATE marker (when out-of-line markers are enabled)
   * @param destination where each payload above the inline threshold is written as it is prepared
   */
  public PreparedInputs(TypedProperties props, String instant, PayloadDestination destination) {
    Objects.requireNonNull(instant, "instant");
    this.destination = Objects.requireNonNull(destination, "destination");
    this.inlineMaxBytes = EmbeddingInputConfig.inlineMaxBytes(props);
    this.inlineMaxHeapFraction = EmbeddingInputConfig.inlineMaxHeapFraction(props);
    this.instant = instant;
  }

  /**
   * The {@code prepared_image} value of {@code row}'s payload, read from {@code payload}.
   *
   * @param row           the row the payload belongs to, for failure messages (its record key fields)
   * @param partitionPath the folder under {@code .hoodie/.blob} the payload file is written to; null or empty for none
   * @param contentType   one of {@link ImageContentTypes#stored()}
   * @param payload       opens the exact bytes the model receives, once
   * @param sizeBytes     the payload's declared size
   * @return a {@link #BLOB_STRUCT} row
   * @throws IllegalArgumentException when an argument breaks the stored-input rules, the payload is larger than one array, or it
   *                                  reads back shorter or longer than {@code sizeBytes} (a content failure)
   * @throws HoodieIOException        when the payload's own stream cannot be read; the caller classifies it, as it classifies
   *                                  any other read of the payload
   * @throws HoodieValidationException when, in a Spark task, the threshold is above the task's heap ceiling (the job's
   *                                  configuration, never the row's content)
   * @throws EmbeddingPayloadWriteException when the payload file's writer cannot be built for the instant (its markers) or the
   *                                  file cannot be written: a storage failure of the round
   */
  public Row prepare(String row, String partitionPath, String contentType, BlobBytesSupplier payload, long sizeBytes) {
    checkTaskCeiling();
    checkPayload(row, payload, sizeBytes);
    checkInput(partitionPath, contentType);
    if (sizeBytes <= inlineMaxBytes) {
      return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, readWhole(payload, sizeBytes, contentType), null}, BLOB_STRUCT);
    }
    PayloadWriter.Payload target = new PayloadWriter.Payload(row, partitionPath, sizeBytes);
    if (sizeBytes <= OutOfLineWritePool.MAX_PENDING_BYTES) {
      // read whole here, so a payload not of its size fails as before, before anything is marked; written in the background
      byte[] bytes = readWhole(payload, sizeBytes, contentType);
      return outOfLine(PayloadWriter.write(writer(target), writes(), target, bytes), sizeBytes);
    }
    OutOfLineWritePool pool = writes.get();
    if (pool != null) {
      // a background write already failed (or was dropped by close): the round fails now, as it would at its barrier
      try {
        pool.checkFailure();
      } catch (EmbeddingPayloadWriteException e) {
        throw e;
      } catch (HoodieException e) {
        throw new EmbeddingPayloadWriteException(target.row, PayloadWriter.STEP_CREATE, e.getMessage(), e);
      }
    }
    String externalPath = PayloadWriter.stream(writer(target), target, () -> new ExactSizeStream(payload, sizeBytes));
    return outOfLine(externalPath, sizeBytes);
  }

  /**
   * Waits for every payload file this task submitted to be written, then throws the first write failure, if any: called before
   * the task reports its end, so the task succeeds only with every file of its rows. The wait is bounded as the pool's
   * ({@link OutOfLineWritePool#awaitAll}): a stuck write fails the task rather than hold its executor slot.
   *
   * @throws org.apache.hudi.exception.HoodieException when a write does not end within the pool's bound, naming it
   */
  public void awaitWrites() {
    OutOfLineWritePool pool = writes.get();
    if (pool != null) {
      pool.awaitAll();
    }
  }

  /**
   * Stops the write threads: writes not yet started are dropped (after {@link #awaitWrites} there are none; a drop is recorded
   * as a failure, so a later barrier fails), and those running are waited for, up to the pool's bound. Write failures are thrown
   * by {@link #awaitWrites}, not here.
   *
   * @throws org.apache.hudi.exception.HoodieException when a running write does not end within the pool's bound
   */
  @Override
  public void close() {
    closed = true;
    OutOfLineWritePool pool = writes.get();
    if (pool != null) {
      pool.close();
    }
  }

  /** A deserialized copy starts with no write pool, as a transient field did. */
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    writes = new AtomicReference<>();
  }

  /** The task's write pool, started on its first background write. */
  private OutOfLineWritePool writes() {
    OutOfLineWritePool pool = writes.get();
    if (pool == null) {
      pool = new OutOfLineWritePool();
      // published before closed is read: a close racing this either sees the pool or is seen here
      writes.set(pool);
      if (closed) {
        // closed before its first background write: the refusal comes from a closed pool, as for any write after close
        pool.close();
      }
    }
    return pool;
  }

  /**
   * The instant's shared writer, built on the first out-of-line payload. Building it creates the instant's markers, which can
   * fail: a failure of the round's storage, never the row's. (A marker setup that would fall back to direct markers is refused
   * earlier, on the driver, by {@link PayloadDestination}.)
   */
  private OutOfLineWriter writer(PayloadWriter.Payload target) {
    try {
      return destination.writer(instant);
    } catch (RuntimeException e) {
      throw new EmbeddingPayloadWriteException(target.row, PayloadWriter.STEP_CREATE,
          "the payload writer of instant " + instant + " cannot be built: " + e.getMessage(), e);
    }
  }

  private static void checkPayload(String row, BlobBytesSupplier payload, long sizeBytes) {
    if (payload == null || sizeBytes < 0 || sizeBytes > MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("A stored embedding input needs a payload of 0 to " + MAX_PAYLOAD_BYTES + " bytes: "
          + row + ", size " + sizeBytes);
    }
  }

  private static void checkInput(String partition, String contentType) {
    if (!ImageContentTypes.stored().contains(contentType)) {
      throw new IllegalArgumentException("A stored embedding input is one of " + ImageContentTypes.stored() + ", not " + contentType);
    }
    OutOfLineWriter.validatePartition(partition);
  }

  private static Row outOfLine(String externalPath, long sizeBytes) {
    StorageReferenceRules.validateWriteOwnership(externalPath, true);
    Row reference = new GenericRowWithSchema(new Object[] {externalPath, 0L, sizeBytes, true}, REFERENCE_STRUCT);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB_STRUCT);
  }

  /** The inline threshold: a payload of at most this many bytes is INLINE. */
  public long inlineMaxBytes() {
    return inlineMaxBytes;
  }

  /**
   * Exactly {@code sizeBytes} bytes of a stored input, including one read without a payload destination.
   *
   * @throws IllegalArgumentException for an unsupported content type, invalid payload size or a size mismatch
   * @throws HoodieIOException when the payload stream cannot be read
   */
  public static byte[] readWhole(BlobBytesSupplier payload, long sizeBytes, String contentType) {
    checkInput(null, contentType);
    return readWhole(payload, sizeBytes);
  }

  /** Exactly the declared bytes of a generic inline payload without an input-store content-type restriction. */
  public static byte[] readWhole(BlobBytesSupplier payload, long sizeBytes) {
    checkPayload("inline payload", payload, sizeBytes);
    byte[] bytes = new byte[(int) sizeBytes];
    try (InputStream in = payload.open()) {
      int read = 0;
      while (read < bytes.length) {
        int n = in.read(bytes, read, bytes.length - read);
        if (n < 0) {
          throw new IllegalArgumentException(sizeMismatch(read, sizeBytes));
        }
        read += n;
      }
      if (in.read() >= 0) {
        throw new IllegalArgumentException(sizeMismatch(sizeBytes + 1, sizeBytes));
      }
    } catch (IOException e) {
      throw readFailure(e);
    }
    return bytes;
  }

  /**
   * Exactly {@code declared} bytes of {@code payload}, checked as {@link #readWhole} checks them, for a payload streamed out of
   * line: a payload that ends early, or has a byte past its size, is an {@link IllegalArgumentException}; a read failure a
   * {@link HoodieIOException}.
   */
  private static final class ExactSizeStream extends InputStream {
    private final InputStream in;
    private final long declared;
    private long read;

    ExactSizeStream(BlobBytesSupplier payload, long declared) {
      try {
        this.in = payload.open();
      } catch (IOException e) {
        throw readFailure(e);
      }
      this.declared = declared;
    }

    @Override
    public int read() {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) {
      if (len == 0) {
        return 0;
      }
      try {
        if (read == declared) {
          if (in.read() >= 0) {
            throw new IllegalArgumentException(sizeMismatch(declared + 1, declared));
          }
          return -1;
        }
        int n = in.read(buffer, offset, (int) Math.min(len, declared - read));
        if (n < 0) {
          throw new IllegalArgumentException(sizeMismatch(read, declared));
        }
        read += n;
        return n;
      } catch (IOException e) {
        throw readFailure(e);
      }
    }

    @Override
    public void close() {
      try {
        in.close();
      } catch (IOException e) {
        throw readFailure(e);
      }
    }
  }

  private static HoodieIOException readFailure(IOException e) {
    return new HoodieIOException("Failed to read a stored embedding input", e);
  }

  private static String sizeMismatch(long read, long declared) {
    return "Payload of " + (read > declared ? "more than " + declared : String.valueOf(read)) + " bytes, but its chunk declares "
        + declared;
  }

  private void checkTaskCeiling() {
    if (taskCeilingChecked) {
      return;
    }
    if (TaskContext.get() != null) {
      EmbeddingInputConfig.checkCeiling(inlineMaxBytes, BlobInlineCeiling.inTask(SparkEnv.get().conf(), inlineMaxHeapFraction),
          "in the task before its first stored input");
      // marked only once the check ran: a first use outside a task (or on a non-task thread) is checked later in one
      taskCeilingChecked = true;
    }
  }
}
