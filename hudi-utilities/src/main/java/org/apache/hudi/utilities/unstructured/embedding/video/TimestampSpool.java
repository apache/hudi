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

package org.apache.hudi.utilities.unstructured.embedding.video;

import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Selected timestamp pairs staged sequentially, with a fixed heap bound independent of video length. Local pair operations
 * and cleanup serialize, so a task-completion listener cannot abandon a file created by a scan still in progress.
 */
final class TimestampSpool implements AutoCloseable {
  private static final int BUFFER_BYTES = 64 * 1024;
  private final Path directory;
  private Path path;
  private DataOutputStream output;
  private OutputStream outputHandle;
  private DataInputStream input;
  private InputStream inputHandle;
  private long remaining;
  private boolean reading;
  private boolean closed;

  TimestampSpool() {
    this(null);
  }

  TimestampSpool(Path directory) {
    this.directory = directory;
  }

  synchronized void add(long relativeMicros, long timestampMicros) {
    if (reading || closed) {
      throw new PrepPlatformException("Cannot append to a timestamp schedule after reading or closing it", null);
    }
    try {
      if (output == null) {
        path = directory == null ? Files.createTempFile("hudi-video-timestamps-", ".bin")
            : Files.createTempFile(directory, "hudi-video-timestamps-", ".bin");
        outputHandle = Files.newOutputStream(path);
        output = new DataOutputStream(new BufferedOutputStream(outputHandle, BUFFER_BYTES));
      }
      output.writeLong(relativeMicros);
      output.writeLong(timestampMicros);
      remaining++;
    } catch (Throwable failure) {
      throw failed("Cannot stage chosen video timestamps", failure);
    }
  }

  synchronized long[] next() {
    if (closed) {
      return null;
    }
    try {
      if (!reading) {
        reading = true;
        if (output != null) {
          output.close();
          output = null;
          outputHandle = null;
          inputHandle = Files.newInputStream(path);
          input = new DataInputStream(new BufferedInputStream(inputHandle, BUFFER_BYTES));
        }
      }
      if (remaining == 0) {
        close();
        return null;
      }
      long[] pair = {input.readLong(), input.readLong()};
      if (--remaining == 0) {
        close();
      }
      return pair;
    } catch (Throwable failure) {
      throw failed("Cannot read chosen video timestamps", failure);
    }
  }

  private PrepPlatformException failed(String message, Throwable failure) {
    try {
      close();
    } catch (RuntimeException | Error cleanup) {
      if (cleanup != failure) {
        failure.addSuppressed(cleanup);
      }
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    return new PrepPlatformException(message, failure);
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    Throwable failure = release(() -> {
      if (output != null) {
        output.close();
      } else if (outputHandle != null) {
        outputHandle.close();
      }
    }, null);
    failure = release(() -> {
      if (input != null) {
        input.close();
      } else if (inputHandle != null) {
        inputHandle.close();
      }
    }, failure);
    failure = release(() -> {
      if (path != null) {
        Files.deleteIfExists(path);
      }
    }, failure);
    output = null;
    outputHandle = null;
    input = null;
    inputHandle = null;
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    if (failure != null) {
      throw new PrepPlatformException("Cannot release chosen video timestamps " + path, failure);
    }
  }

  private static Throwable release(IoAction action, Throwable failure) {
    try {
      action.run();
    } catch (Throwable cleanup) {
      if (failure == null) {
        return cleanup;
      }
      if (cleanup != failure) {
        failure.addSuppressed(cleanup);
      }
    }
    return failure;
  }

  private interface IoAction {
    void run() throws IOException;
  }
}
