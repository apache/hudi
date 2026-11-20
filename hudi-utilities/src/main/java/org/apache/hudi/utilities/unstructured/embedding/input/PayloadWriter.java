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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWritePool;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriter;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * Streams a prepared payload through the shared out-of-line writer into a fresh managed file. The payload stream's own failures
 * keep the caller's classification; a failure to write the file interrupts the round. Used while the embedding round
 * preprocesses, before its row write.
 */
public final class PayloadWriter {

  public static final String STEP_CREATE = "create";
  public static final String STEP_WRITE = "write";
  /** Bytes one read of a payload asks for: the writer's own heap per payload. */
  static final int COPY_BUFFER_BYTES = 64 * 1024;

  private PayloadWriter() {
  }

  /** One row's payload: the row it belongs to, its destination partition under the blob folder, and its declared length. */
  public static final class Payload {
    final String row;
    final String partitionPath;
    final long length;

    public Payload(String row, String partitionPath, long length) {
      this.row = row;
      this.partitionPath = partitionPath;
      this.length = length;
    }

    /** An interruption (the round fails and is retried). */
    EmbeddingPayloadWriteException failure(String step, String cause, Throwable throwable) {
      return new EmbeddingPayloadWriteException(row, step, cause, throwable);
    }
  }

  /**
   * Streams the payload {@code bytes} opens to a fresh managed file through a bounded buffer, never holding it whole, and
   * returns its generated relative path after a successful close. The shared writer owns marker and target creation and removes
   * the target when the write fails. The stream's own failures (to open, read or close it) are the caller's classification and
   * propagate unchanged; a failure to create or write the file is an {@link EmbeddingPayloadWriteException}.
   *
   * @param bytes opens exactly the payload's bytes; its stream fails with an unchecked exception of the caller's choosing
   */
  public static String stream(OutOfLineWriter writer, Payload payload, Supplier<InputStream> bytes) {
    String[] step = {STEP_CREATE};
    try {
      return writer.write(payload.partitionPath, out -> {
        step[0] = STEP_WRITE;
        try (InputStream in = new PayloadReadSide(bytes)) {
          byte[] buffer = new byte[(int) Math.min(COPY_BUFFER_BYTES, Math.max(1L, payload.length))];
          for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
            out.write(buffer, 0, n);
          }
        }
      });
    } catch (PayloadReadFailure e) {
      throw e.unwrap();
    } catch (IOException | RuntimeException e) {
      throw payload.failure(step[0], e.getMessage(), e);
    }
  }

  /**
   * As {@link #stream} for a payload already read whole: names the file now, on the calling task's thread, and writes it on
   * {@code pool}, which throws a failure to create or write it, an {@link EmbeddingPayloadWriteException} as {@link #stream}
   * makes it, at the task's next submit or at its {@link OutOfLineWritePool#awaitAll}. A submit refused (the task was
   * interrupted while waiting for room, its pool is closed, or no room came back within the pool's bound: a stuck write) is an
   * {@link EmbeddingPayloadWriteException} of this payload: a failure of the round, never of the row's content. Returns the
   * generated relative path.
   */
  public static String write(OutOfLineWriter writer, OutOfLineWritePool pool, Payload payload, byte[] bytes) {
    String relative = writer.newPath(payload.partitionPath);
    try {
      submit(writer, pool, payload, bytes, relative);
    } catch (EmbeddingPayloadWriteException e) {
      throw e;
    } catch (HoodieException e) {
      throw payload.failure(STEP_CREATE, e.getMessage(), e);
    }
    return relative;
  }

  private static void submit(OutOfLineWriter writer, OutOfLineWritePool pool, Payload payload, byte[] bytes, String relative) {
    pool.submit(bytes.length, relative, () -> {
      String[] step = {STEP_CREATE};
      try {
        writer.writeAt(relative, out -> {
          step[0] = STEP_WRITE;
          out.write(bytes);
        });
      } catch (IOException | RuntimeException e) {
        throw payload.failure(step[0], e.getMessage(), e);
      }
    });
  }

  /** A failure of the payload stream {@link #stream} reads, carried past the write's own classification. */
  private static final class PayloadReadFailure extends RuntimeException {
    private final RuntimeException failure;

    PayloadReadFailure(RuntimeException failure) {
      super(failure.getMessage(), null, true, false);
      this.failure = failure;
    }

    /** The stream's failure, with what was suppressed on the way out (a close failure, the target's cleanup). */
    RuntimeException unwrap() {
      for (Throwable suppressed : getSuppressed()) {
        failure.addSuppressed(suppressed instanceof PayloadReadFailure ? ((PayloadReadFailure) suppressed).failure : suppressed);
      }
      return failure;
    }
  }

  /** The payload stream of {@link #stream}, each of whose failures is a {@link PayloadReadFailure}. */
  private static final class PayloadReadSide extends InputStream {
    private final InputStream in;

    PayloadReadSide(Supplier<InputStream> bytes) {
      try {
        in = bytes.get();
      } catch (RuntimeException e) {
        throw new PayloadReadFailure(e);
      }
    }

    @Override
    public int read() {
      try {
        return in.read();
      } catch (IOException e) {
        throw new PayloadReadFailure(new HoodieIOException("Failed to read a payload", e));
      } catch (RuntimeException e) {
        throw new PayloadReadFailure(e);
      }
    }

    @Override
    public int read(byte[] buffer, int offset, int len) {
      try {
        return in.read(buffer, offset, len);
      } catch (IOException e) {
        throw new PayloadReadFailure(new HoodieIOException("Failed to read a payload", e));
      } catch (RuntimeException e) {
        throw new PayloadReadFailure(e);
      }
    }

    @Override
    public void close() {
      try {
        in.close();
      } catch (IOException e) {
        throw new PayloadReadFailure(new HoodieIOException("Failed to close a payload", e));
      } catch (RuntimeException e) {
        throw new PayloadReadFailure(e);
      }
    }
  }
}
