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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A storage that fetches the log files of one file slice concurrently, each with one whole-file read, and
 * serves later {@code open} and {@code openSeekable} calls on those files from memory. Every other call goes to
 * the wrapped storage, including block payloads read through an inline storage from {@link #newInstance}
 * (Parquet data blocks, HFile key lookups). The log scan still reads the files in order, but no longer waits on
 * one request per file in turn. The fetched bytes are held until {@link #close()}, so close it once the log scan
 * is done.
 */
public final class PrefetchedLogFileStorage extends HoodieStorage {

  private static final int MAX_FETCH_THREADS = 64;
  private static final AtomicInteger THREAD_ID = new AtomicInteger();
  // Shared by all instances so concurrent fetches stay bounded per JVM; its daemon threads exit when idle, so it
  // needs no shutdown
  private static final ExecutorService FETCH_POOL = new ThreadPoolExecutor(
      0, MAX_FETCH_THREADS, 30, TimeUnit.SECONDS, new SynchronousQueue<>(),
      runnable -> {
        Thread thread = new Thread(runnable, "hudi-log-prefetch-" + THREAD_ID.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      },
      // When every fetch thread is busy, the caller reads the file itself
      new ThreadPoolExecutor.CallerRunsPolicy());

  private final HoodieStorage delegate;
  private final Map<StoragePath, CompletableFuture<byte[]>> fetched;

  private PrefetchedLogFileStorage(HoodieStorage delegate, Map<StoragePath, CompletableFuture<byte[]>> fetched) {
    super(delegate.getConf());
    this.delegate = delegate;
    this.fetched = fetched;
  }

  /**
   * Reads a whole file of a known size. An engine whose file system has a positional read passes one, so each
   * file is fetched with one ranged read instead of a stream that a seek may abort.
   */
  @FunctionalInterface
  public interface FileFetcher {
    void readFully(StoragePath path, byte[] buffer) throws IOException;
  }

  /**
   * Starts fetching the given log files, up to {@code maxBytes} in total for this call, by streaming each through
   * the wrapped storage. A file is served as exactly {@link HoodieLogFile#getFileSize()} bytes, so the sizes must be
   * the files' lengths. Files whose size is unknown or would exceed the remaining budget are left to the wrapped
   * storage. A failed fetch surfaces as an {@link IOException} when the file is opened.
   */
  public static PrefetchedLogFileStorage prefetch(HoodieStorage delegate, List<HoodieLogFile> logFiles, long maxBytes) {
    return prefetch(delegate, logFiles, maxBytes, (path, buffer) -> readFully(delegate, path, buffer));
  }

  /**
   * As {@link #prefetch(HoodieStorage, List, long)}, fetching each file with {@code fetcher}.
   */
  public static PrefetchedLogFileStorage prefetch(HoodieStorage delegate, List<HoodieLogFile> logFiles, long maxBytes, FileFetcher fetcher) {
    Map<StoragePath, CompletableFuture<byte[]>> fetched = new HashMap<>();
    long budget = maxBytes;
    for (HoodieLogFile logFile : logFiles) {
      long size = logFile.getFileSize();
      if (size <= 0 || size > Integer.MAX_VALUE || size > budget) {
        continue;
      }
      budget -= size;
      StoragePath path = logFile.getPath();
      // Keyed by the qualified path, which is what the log reader opens
      fetched.put(FSUtils.makeQualified(delegate, path), CompletableFuture.supplyAsync(() -> fetch(fetcher, path, (int) size), FETCH_POOL));
    }
    return new PrefetchedLogFileStorage(delegate, fetched);
  }

  private static byte[] fetch(FileFetcher fetcher, StoragePath path, int size) {
    byte[] bytes = new byte[size];
    try {
      fetcher.readFully(path, bytes);
    } catch (IOException e) {
      throw new CompletionException(e);
    }
    return bytes;
  }

  private static void readFully(HoodieStorage storage, StoragePath path, byte[] bytes) throws IOException {
    int size = bytes.length;
    try (InputStream in = storage.open(path)) {
      int read = 0;
      while (read < size) {
        int n = in.read(bytes, read, size - read);
        if (n < 0) {
          throw new EOFException("Log file " + path + " ended after " + read + " of " + size + " bytes");
        }
        read += n;
      }
    }
  }

  private byte[] fetchedBytes(StoragePath path) throws IOException {
    CompletableFuture<byte[]> future = fetched.isEmpty() || !path.isAbsolute() ? null : fetched.get(FSUtils.makeQualified(delegate, path));
    if (future == null) {
      return null;
    }
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted = new InterruptedIOException("Interrupted waiting for log file " + path);
      interrupted.initCause(e);
      throw interrupted;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Error) {
        throw (Error) e.getCause();
      }
      if (e.getCause() instanceof InterruptedIOException) {
        throw (InterruptedIOException) e.getCause();
      }
      throw new IOException("Failed to prefetch log file " + path, e.getCause());
    }
  }

  @Override
  public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream) throws IOException {
    byte[] bytes = fetchedBytes(path);
    return bytes == null
        ? delegate.openSeekable(path, bufferSize, wrapStream)
        : new InMemorySeekableDataInputStream(new InMemoryFileStream(bytes));
  }

  /**
   * A whole file in memory, read with the end-of-file behavior of a file system stream: a read at the end returns
   * -1, and a seek past the end throws {@link EOFException}, which the log reader takes as the end of the file or a
   * truncated block.
   */
  static final class InMemoryFileStream extends InputStream {
    private final byte[] bytes;
    private int pos;

    InMemoryFileStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int read() {
      return pos < bytes.length ? bytes[pos++] & 0xFF : -1;
    }

    @Override
    public int read(byte[] target, int offset, int length) {
      if (length == 0) {
        return 0;
      }
      if (pos >= bytes.length) {
        return -1;
      }
      int n = Math.min(length, bytes.length - pos);
      System.arraycopy(bytes, pos, target, offset, n);
      pos += n;
      return n;
    }

    @Override
    public long skip(long n) {
      long skipped = Math.max(0, Math.min(n, bytes.length - pos));
      pos += (int) skipped;
      return skipped;
    }

    @Override
    public int available() {
      return bytes.length - pos;
    }

    long position() {
      return pos;
    }

    void seek(long target) throws EOFException {
      if (target < 0 || target > bytes.length) {
        throw new EOFException("Cannot seek to " + target + " in an in-memory file of " + bytes.length + " bytes");
      }
      pos = (int) target;
    }
  }

  private static final class InMemorySeekableDataInputStream extends SeekableDataInputStream {
    private final InMemoryFileStream stream;

    InMemorySeekableDataInputStream(InMemoryFileStream stream) {
      super(stream);
      this.stream = stream;
    }

    @Override
    public long getPos() {
      return stream.position();
    }

    @Override
    public void seek(long pos) throws IOException {
      stream.seek(pos);
    }
  }

  @Override
  public InputStream open(StoragePath path) throws IOException {
    byte[] bytes = fetchedBytes(path);
    return bytes == null ? delegate.open(path) : new ByteArrayInputStream(bytes);
  }

  @Override
  public HoodieStorage newInstance(StoragePath path, StorageConfiguration<?> storageConf) {
    return delegate.newInstance(path, storageConf);
  }

  @Override
  public String getScheme() {
    return delegate.getScheme();
  }

  @Override
  public int getDefaultBlockSize(StoragePath path) {
    return delegate.getDefaultBlockSize(path);
  }

  @Override
  public int getDefaultBufferSize() {
    return delegate.getDefaultBufferSize();
  }

  @Override
  public short getDefaultReplication(StoragePath path) {
    return delegate.getDefaultReplication(path);
  }

  @Override
  public URI getUri() {
    return delegate.getUri();
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
    return delegate.create(path, overwrite);
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite, Integer bufferSize, Short replication, Long sizeThreshold)
      throws IOException {
    return delegate.create(path, overwrite, bufferSize, replication, sizeThreshold);
  }

  @Override
  public OutputStream append(StoragePath path) throws IOException {
    return delegate.append(path);
  }

  @Override
  public boolean exists(StoragePath path) throws IOException {
    return delegate.exists(path);
  }

  @Override
  public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
    return delegate.getPathInfo(path);
  }

  @Override
  public boolean createDirectory(StoragePath path) throws IOException {
    return delegate.createDirectory(path);
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException {
    return delegate.listDirectEntries(path);
  }

  @Override
  public List<StoragePathInfo> listFiles(StoragePath path) throws IOException {
    return delegate.listFiles(path);
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path, StoragePathFilter filter) throws IOException {
    return delegate.listDirectEntries(path, filter);
  }

  @Override
  public void setModificationTime(StoragePath path, long modificationTimeInMillisEpoch) throws IOException {
    delegate.setModificationTime(path, modificationTimeInMillisEpoch);
  }

  @Override
  public List<StoragePathInfo> globEntries(StoragePath pathPattern, StoragePathFilter filter) throws IOException {
    return delegate.globEntries(pathPattern, filter);
  }

  @Override
  public boolean rename(StoragePath oldPath, StoragePath newPath) throws IOException {
    return delegate.rename(oldPath, newPath);
  }

  @Override
  public boolean deleteDirectory(StoragePath path) throws IOException {
    return delegate.deleteDirectory(path);
  }

  @Override
  public boolean deleteFile(StoragePath path) throws IOException {
    return delegate.deleteFile(path);
  }

  @Override
  public Object getFileSystem() {
    return delegate.getFileSystem();
  }

  @Override
  public HoodieStorage getRawStorage() {
    return delegate.getRawStorage();
  }

  @Override
  public boolean isEmpty(StoragePath path) throws IOException {
    return delegate.isEmpty(path);
  }

  @Override
  public void close() throws IOException {
    // The wrapped storage belongs to the caller; drop the fetched bytes
    fetched.clear();
  }
}
