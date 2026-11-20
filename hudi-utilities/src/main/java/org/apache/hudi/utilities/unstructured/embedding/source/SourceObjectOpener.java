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

package org.apache.hudi.utilities.unstructured.embedding.source;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.hadoop.HadoopSeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Opens a stored object for reading through its Hadoop file system, using what the caller already knows about it:
 * <ul>
 *   <li>its length: on S3A the object is opened through {@code FileSystem#openFile} with an {@code S3AFileStatus} of that
 *       length, so S3A does not send its own HEAD request before the first GET. Without a checked status, every other file
 *       system, and a Hadoop without that API, gets the plain {@code FileSystem#open};</li>
 *   <li>a read-ahead: set on the opened stream when the stream takes one ({@code CanSetReadahead}, as S3A's does), so a
 *       reader that seeks reads that many bytes per request instead of the file system's default.</li>
 * </ul>
 * A checked status is passed to the public openFile builder on any connector that supports it, retaining available
 * conditional-read metadata. A storage that is not a Hadoop file system, and an open without a known length, read-ahead or checked status, is opened as
 * {@link HoodieStorage#openSeekable} opens it. A failure to open is the
 * {@link HoodieIOException} {@link HoodieStorage#openSeekable} throws, so callers classify it as before.
 *
 * <p>This module compiles against Hadoop 2, which has neither {@code openFile} nor {@code S3AFileStatus}; both are reached
 * reflectively, and a missing method, class or constructor falls back to the plain open.
 */
final class SourceObjectOpener {

  private static final Logger LOG = LoggerFactory.getLogger(SourceObjectOpener.class);
  static final String S3A_FILE_SYSTEM = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  static final String S3A_FILE_STATUS = "org.apache.hadoop.fs.s3a.S3AFileStatus";
  /** The standard openFile length option (Hadoop 3.3.5+); older S3A ignores it and relies on the file status. */
  static final String OPENFILE_LENGTH = "fs.option.openfile.length";

  private static final OpenFileApi OPEN_FILE_API = OpenFileApi.load();

  private SourceObjectOpener() {
  }

  /**
   * Opens {@code path} for random access.
   *
   * @param knownLength the object's length when the caller knows it (from a listing), else null
   * @param readaheadBytes the read-ahead to set on the stream, or 0 to keep the file system's
   */
  static SeekableDataInputStream open(HoodieStorage storage, StoragePath path, int bufferSize, Long knownLength,
                                      long readaheadBytes) throws IOException {
    return open(storage, path, bufferSize, knownLength, readaheadBytes, null);
  }

  /** Uses a previously checked status, retaining its ETag/version for a connector's conditional GET. */
  static SeekableDataInputStream open(HoodieStorage storage, StoragePath path, int bufferSize, Long knownLength,
                                      long readaheadBytes, FileStatus checkedStatus) throws IOException {
    Object fileSystem = storage.getFileSystem();
    if (!(fileSystem instanceof FileSystem) || (knownLength == null && readaheadBytes <= 0 && checkedStatus == null)) {
      // nothing to add to the storage's own open: open exactly as it does
      return storage.openSeekable(path, bufferSize, false);
    }
    FileSystem fs = (FileSystem) fileSystem;
    Path hadoopPath = new Path(path.toUri());
    FSDataInputStream in;
    try {
      in = checkedStatus != null && OPEN_FILE_API.available()
          ? OPEN_FILE_API.open(fs, hadoopPath, checkedStatus, checkedStatus.getLen(), bufferSize)
          : knownLength == null ? null : openWithKnownLength(fs, hadoopPath, knownLength, bufferSize);
      if (in == null) {
        in = fs.open(hadoopPath, bufferSize);
      }
    } catch (IOException e) {
      // the same wrapping HoodieStorage#openSeekable applies, so a failure reads as it did before
      throw new HoodieIOException(String.format("Exception creating input stream from file: %s", path), e);
    }
    try {
      if (readaheadBytes > 0) {
        setReadahead(in, readaheadBytes);
      }
      return new HadoopSeekableDataInputStream(in);
    } catch (RuntimeException | Error failure) {
      try {
        in.close();
      } catch (IOException | RuntimeException | Error closeFailure) {
        if (closeFailure != failure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
  }

  private static void setReadahead(FSDataInputStream in, long readaheadBytes) {
    try {
      in.setReadahead(readaheadBytes);
    } catch (IOException | RuntimeException e) {
      SourceFailures.rethrowJvmFailure(e);
      // a stream without read-ahead (a local file: UnsupportedOperationException) reads as it always has
      LOG.debug("Stream does not take a read-ahead of {} bytes", readaheadBytes, e);
    }
  }

  /**
   * S3A opened with the known length, so it skips its HEAD; null when {@code fs} is not S3A or this Hadoop cannot pass a
   * file status to an open.
   */
  static FSDataInputStream openWithKnownLength(FileSystem fs, Path path, long length, int bufferSize) throws IOException {
    if (!isS3A(fs.getClass())) {
      return null;
    }
    if (!OPEN_FILE_API.available()) {
      warnOnce("this Hadoop has no FileSystem#openFile");
      return null;
    }
    FileStatus status = s3aStatus(fs, path, length);
    if (status == null) {
      warnOnce("no S3AFileStatus could be built");
      return null;
    }
    return OPEN_FILE_API.open(fs, path, status, length, bufferSize);
  }

  private static volatile boolean warned;

  /** Once per JVM: S3A objects are opened with a status request, so the saving is gone; visible, never a failure. */
  private static void warnOnce(String why) {
    if (!warned) {
      warned = true;
      LOG.warn("S3A source objects are opened with a status request: {}", why);
    }
  }

  static boolean isS3A(Class<?> fileSystemClass) {
    for (Class<?> c = fileSystemClass; c != null; c = c.getSuperclass()) {
      if (S3A_FILE_SYSTEM.equals(c.getName())) {
        return true;
      }
    }
    return false;
  }

  /** An S3AFileStatus of the object: its length and path, no etag or version (S3A then takes them from the first GET). */
  private static FileStatus s3aStatus(FileSystem fs, Path path, long length) {
    return s3aStatus(fs.getClass().getClassLoader(), S3A_FILE_STATUS, fs, path, length);
  }

  /**
   * {@link #s3aStatus(FileSystem, Path, long)} with the status class named and loaded explicitly: a class or constructor that is
   * absent is an optional API missing (null, the caller falls back); a JVM failure while constructing or initializing it is not.
   */
  static FileStatus s3aStatus(ClassLoader loader, String statusClassName, FileSystem fs, Path path, long length) {
    Constructor<?> constructor;
    try {
      Class<?> statusClass = Class.forName(statusClassName, false, loader);
      constructor = statusClass.getConstructor(long.class, long.class, Path.class, long.class, String.class,
          String.class, String.class);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      if (!(e instanceof LinkageError)) {
        SourceFailures.rethrowJvmFailure(e);
      }
      LOG.debug("No S3AFileStatus for {}; opening it with a status request", path, e);
      return null;
    }
    try {
      // Initialize only during invocation: a constructor or initializer failure is not an absent optional API.
      Path qualified = fs.makeQualified(path);
      return (FileStatus) constructor.newInstance(length, 0L, qualified, fs.getDefaultBlockSize(qualified), null, null, null);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      SourceFailures.rethrowJvmFailure(e);
      LOG.debug("Cannot construct S3AFileStatus for {}; opening it with a status request", path, e);
      return null;
    }
  }

  /** The Hadoop 3 {@code openFile} builder, reached reflectively. */
  static final class OpenFileApi {
    private final Method openFile;
    private final Method withFileStatus;
    private final Method opt;
    private final Method build;

    private OpenFileApi(Method openFile, Method withFileStatus, Method opt, Method build) {
      this.openFile = openFile;
      this.withFileStatus = withFileStatus;
      this.opt = opt;
      this.build = build;
    }

    static OpenFileApi load() {
      try {
        Method openFile = FileSystem.class.getMethod("openFile", Path.class);
        Class<?> builder = openFile.getReturnType();
        return new OpenFileApi(openFile, builder.getMethod("withFileStatus", FileStatus.class),
            builder.getMethod("opt", String.class, String.class), builder.getMethod("build"));
      } catch (NoSuchMethodException | RuntimeException | LinkageError e) {
        if (!(e instanceof LinkageError)) {
          SourceFailures.rethrowJvmFailure(e);
        }
        return new OpenFileApi(null, null, null, null);
      }
    }

    boolean available() {
      return openFile != null;
    }

    FSDataInputStream open(FileSystem fs, Path path, FileStatus status, long length, int bufferSize) throws IOException {
      Object future;
      try {
        Object builder = openFile.invoke(fs, path);
        builder = withFileStatus.invoke(builder, status);
        builder = opt.invoke(builder, OPENFILE_LENGTH, Long.toString(length));
        builder = opt.invoke(builder, "fs.option.openfile.buffer.size", Integer.toString(bufferSize));
        future = build.invoke(builder);
      } catch (InvocationTargetException e) {
        SourceFailures.rethrowJvmFailure(e);
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        // a builder that refuses the status (not for this path) or the call: the plain open reads as before
        warnOnce("openFile refused the file status (" + cause + ")");
        return null;
      } catch (IllegalAccessException | RuntimeException e) {
        SourceFailures.rethrowJvmFailure(e);
        warnOnce("openFile is not callable (" + e + ")");
        return null;
      }
      return await((CompletableFuture<?>) future, path);
    }

    /**
     * The opened stream, or null for an open that refused its arguments (an IllegalArgumentException: a status S3A does not take
     * for this path), which the plain open then serves; Hadoop's own unwrapping (FutureIO#awaitFuture) is followed otherwise.
     */
    private static FSDataInputStream await(CompletableFuture<?> future, Path path) throws IOException {
      try {
        return (FSDataInputStream) future.get();
      } catch (InterruptedException e) {
        // The backend may finish opening after this task stops waiting. Cancelling its future could discard that stream.
        future.whenComplete((opened, failure) -> {
          if (opened instanceof FSDataInputStream) {
            try {
              ((FSDataInputStream) opened).close();
            } catch (IOException | RuntimeException closeFailure) {
              LOG.warn("Cannot close an abandoned stored-object stream for {}", path, closeFailure);
            }
          }
        });
        Thread.currentThread().interrupt();
        InterruptedIOException interrupted = new InterruptedIOException("Interrupted while opening a stored object");
        interrupted.initCause(e);
        throw interrupted;
      } catch (ExecutionException e) {
        return openFailure(e, path);
      }
    }

    /** Unwraps a failed open: rethrows its cause, or returns null (after a warning) for an IllegalArgumentException. */
    private static FSDataInputStream openFailure(ExecutionException e, Path path) throws IOException {
      SourceFailures.rethrowJvmFailure(e);
      Throwable cause = e.getCause();
      while ((cause instanceof CompletionException || cause instanceof ExecutionException)
          && cause.getCause() != null) {
        cause = cause.getCause();
      }
      if (cause instanceof UncheckedIOException) {
        throw ((UncheckedIOException) cause).getCause();
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof IllegalArgumentException) {
        warnOnce("openFile refused the file status of " + path + " (" + cause + ")");
        return null;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new IOException(cause);
    }
  }
}
