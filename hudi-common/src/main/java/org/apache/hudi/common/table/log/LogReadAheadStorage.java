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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockContentLocation;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A storage that reads the log files of one file slice ahead of the log scan, within a byte window, and serves the
 * scan's reads of those files from memory. Every other call goes to the wrapped storage.
 *
 * <p>The log scan reads a file slice twice. The first pass walks the block headers of every log file in log order;
 * the second pass reads the content of the blocks that survive rollbacks, oldest instant first, and merges each block
 * into the record buffer before reading the next. The read-ahead follows both passes:
 * <ul>
 *   <li>During the header pass, whole log files are fetched concurrently ahead of the file the scan is on. A file the
 *   scan is done with is kept for the content pass while that leaves room in the window for the files still ahead,
 *   and dropped otherwise.</li>
 *   <li>Once the header pass hands over the blocks to merge ({@link #planBlockReads}), the content of each block
 *   whose file was not kept is fetched concurrently ahead of the merge, again within the window. A fetched block,
 *   or a kept file, is dropped as soon as the merge moves past it, which makes room for the next fetch.</li>
 * </ul>
 * The merge therefore runs on one block while the next blocks are on the way, and never holds more than the window
 * in fetched bytes. When everything fits in the window, each file is read once, all at the start.
 *
 * <p>The bytes held by all instances in the JVM are capped as well ({@link #defaultExecutorBudget}), and a scan can
 * lower that cap for its own fetches. A fetch holds its bytes against both caps until it ends, even when the scan
 * drops it while it runs. A file or block that cannot be fetched ahead, because it is larger than the window, because
 * the cap is used up when the scan reaches it, or because its fetch failed, is read in line through the wrapped
 * storage, as without this storage.
 *
 * <p>Streams over fetched bytes behave as file system streams do at the end of a file: a read there returns -1, and a
 * seek past it throws {@link EOFException}.
 */
@Slf4j
public final class LogReadAheadStorage extends HoodieStorage {

  // Fetches wait on object store latency rather than on CPU, so a few per core keep the cores busy
  static final int FETCH_THREADS = Math.max(8, 4 * Runtime.getRuntime().availableProcessors());
  private static final AtomicInteger THREAD_ID = new AtomicInteger();
  private static final ThreadPoolExecutor FETCH_POOL = createFetchPool();
  private static final ExecutorBudget EXECUTOR_BUDGET = new ExecutorBudget(defaultExecutorBudget());

  private final HoodieStorage delegate;
  private final long windowBytes;
  // The bytes the read-ahead of all scans may hold for this scan to start a fetch
  private final long executorLimitBytes;
  // Log files in the order the scan reads them
  private final List<FileEntry> files;
  private final Map<String, FileEntry> filesByPath;
  private final Stats stats = new Stats();
  // Block contents in merge order, set once the header pass is over
  private List<Unit> ranges;
  // Index of the file the header pass is on
  private int scanCursor = -1;
  // Index of the earliest block content the merge has not finished with
  private int mergeCursor;
  private long heldBytes;
  private long retainedBytes;
  private boolean closed;

  private LogReadAheadStorage(HoodieStorage delegate, List<HoodieLogFile> logFiles, long windowBytes, long executorLimitBytes) {
    super(delegate.getConf());
    this.delegate = delegate;
    this.windowBytes = Math.max(0L, windowBytes);
    this.executorLimitBytes = Math.max(0L, executorLimitBytes);
    this.files = new ArrayList<>(logFiles.size());
    this.filesByPath = new HashMap<>();
    for (HoodieLogFile logFile : logFiles) {
      String key = key(logFile.getPath());
      if (filesByPath.containsKey(key)) {
        continue;
      }
      FileEntry entry = new FileEntry(files.size(), logFile);
      files.add(entry);
      filesByPath.put(key, entry);
    }
    // Bytes of the files after each one, which the header pass still has to fetch
    long after = 0;
    for (int i = files.size() - 1; i >= 0; i--) {
      files.get(i).bytesAfter = after;
      after += Math.max(0L, files.get(i).unit.length);
    }
  }

  /**
   * Starts reading the given log files ahead of the log scan, holding at most {@code windowBytes} of them at a time.
   * The scan starts a fetch only while the read-ahead of all scans in this JVM holds less than both
   * {@code executorLimitBytes} and the JVM-wide cap.
   */
  public static LogReadAheadStorage create(HoodieStorage delegate, List<HoodieLogFile> logFiles, long windowBytes,
                                           long executorLimitBytes) {
    List<HoodieLogFile> ordered = logFiles.stream()
        .filter(logFile -> !logFile.isCDC())
        .sorted(HoodieLogFile.getLogFileComparator())
        .collect(Collectors.toList());
    LogReadAheadStorage storage = new LogReadAheadStorage(delegate, ordered, windowBytes, executorLimitBytes);
    synchronized (storage) {
      storage.fillWindow();
    }
    return storage;
  }

  /**
   * @return the default cap of the bytes held by the read-ahead of all log scans in this JVM: 10% of the heap, at most
   * 512 MB
   */
  public static long defaultExecutorBudget() {
    return Math.min(512L * 1024 * 1024, Runtime.getRuntime().maxMemory() / 10);
  }

  /**
   * @return the bytes the read-ahead of all log scans in this JVM holds now
   */
  static long executorBytesInUse() {
    return EXECUTOR_BUDGET.used.get();
  }

  Stats getStats() {
    return stats;
  }

  /**
   * Called by the log scan once its header pass is over, with the blocks it is about to merge, in merge order. The
   * content of these blocks is then read ahead of the merge.
   */
  public synchronized void planBlockReads(List<HoodieLogBlock> blocksInMergeOrder) {
    if (closed) {
      return;
    }
    if (ranges != null) {
      // A second scan with the same storage: start over from the wrapped storage
      releaseAll();
    }
    // The header pass is over, so every file it fetched is either kept for the merge or dropped
    for (FileEntry file : files) {
      if (file.unit.state == State.FETCHING) {
        finishScan(file);
      }
    }
    ranges = new ArrayList<>();
    for (HoodieLogBlock block : blocksInMergeOrder) {
      if (!readsContentThroughStorage(block)) {
        continue;
      }
      HoodieLogBlockContentLocation location = block.getBlockContentLocation().get();
      FileEntry file = filesByPath.get(key(location.getLogFile().getPath()));
      long length = location.getBlockSize();
      if (file == null || length <= 0 || length > Integer.MAX_VALUE) {
        continue;
      }
      Unit range = new Unit(file, location.getContentPositionInLogFile(), (int) length);
      range.index = ranges.size();
      if (file.unit.state == State.RETAINED) {
        range.state = State.IN_FILE;
        file.lastRange = range.index;
      }
      ranges.add(range);
      file.ranges.add(range);
    }
    for (FileEntry file : files) {
      if (file.unit.state == State.RETAINED && file.lastRange < 0) {
        release(file.unit);
      }
    }
    mergeCursor = 0;
    fillWindow();
  }

  private boolean readsContentThroughStorage(HoodieLogBlock block) {
    if (!block.getBlockContentLocation().isPresent()) {
      return false;
    }
    switch (block.getBlockType()) {
      case AVRO_DATA_BLOCK:
      case HFILE_DATA_BLOCK:
      case CDC_DATA_BLOCK:
      case DELETE_BLOCK:
        return true;
      default:
        // Parquet blocks are read through the in-line file system, not through this storage
        return false;
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Scheduling
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * Starts fetches in scan order from the current position until the window or the executor-wide cap is full.
   */
  private void fillWindow() {
    if (closed) {
      return;
    }
    if (ranges == null) {
      for (int i = Math.max(scanCursor, 0); i < files.size(); i++) {
        Unit unit = files.get(i).unit;
        if (unit.state == State.PENDING && !tryStart(unit)) {
          return;
        }
      }
    } else {
      for (int i = mergeCursor; i < ranges.size(); i++) {
        Unit unit = ranges.get(i);
        if (unit.state == State.PENDING && !tryStart(unit)) {
          return;
        }
      }
    }
  }

  /**
   * @return false when the fetch has to wait for bytes to be released
   */
  private boolean tryStart(Unit unit) {
    if (unit.length <= 0 || unit.length > windowBytes) {
      // Unknown size, or never fits: read in line when the scan gets to it
      unit.state = State.INLINE;
      stats.inlineReads++;
      return true;
    }
    if (heldBytes + unit.length > windowBytes) {
      return false;
    }
    if (!EXECUTOR_BUDGET.tryAcquire(unit.length, executorLimitBytes)) {
      stats.executorCapDenials++;
      return false;
    }
    heldBytes += unit.length;
    stats.peakHeldBytes = Math.max(stats.peakHeldBytes, heldBytes);
    unit.state = State.FETCHING;
    unit.bytes = new CompletableFuture<>();
    if (unit.wholeFile) {
      stats.filesFetched++;
    } else {
      stats.blocksFetched++;
    }
    FETCH_POOL.execute(() -> runFetch(unit));
    return true;
  }

  private void runFetch(Unit unit) {
    CompletableFuture<byte[]> future;
    synchronized (this) {
      future = unit.bytes;
      if (future == null || unit.fetchClaimed) {
        return;
      }
      unit.fetchClaimed = true;
      unit.fetchRunning = true;
    }
    byte[] bytes = null;
    Throwable failure = null;
    try {
      bytes = new byte[unit.length];
      try (SeekableDataInputStream in = delegate.openSeekable(unit.file.path, false)) {
        if (unit.offset > 0) {
          in.seek(unit.offset);
        }
        in.readFully(bytes, 0, bytes.length);
      }
    } catch (Throwable t) {
      failure = t;
    }
    onFetchEnd(unit, future, bytes, failure);
  }

  /**
   * Hands the bytes of a fetch to the scan, or returns them to the window and the executor-wide cap when the scan
   * dropped the unit while the fetch ran. After a failure the scan reads the unit in line.
   */
  private synchronized void onFetchEnd(Unit unit, CompletableFuture<byte[]> future, byte[] bytes, Throwable failure) {
    unit.fetchRunning = false;
    if (unit.bytes != future) {
      returnReservation(unit);
      fillWindow();
      return;
    }
    if (failure == null) {
      future.complete(bytes);
      return;
    }
    // Release before completing, so that the bytes are back by the time the scan sees the failure
    boolean wasRetained = unit.state == State.RETAINED;
    release(unit);
    unit.state = wasRetained || unit.wholeFile ? State.RELEASED : State.INLINE;
    stats.inlineReads++;
    fillWindow();
    future.completeExceptionally(failure);
  }

  /**
   * Waits for the bytes of a fetched unit, fetching them on this thread if no fetch thread has started yet.
   *
   * @return the bytes, or null when the unit has been released or its fetch failed
   */
  private byte[] awaitBytes(Unit unit) throws IOException {
    CompletableFuture<byte[]> future = unit.bytes;
    if (future == null) {
      return null;
    }
    runFetch(unit);
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while reading ahead " + unit);
    } catch (ExecutionException e) {
      log.warn("Failed to read ahead {}; reading it through the wrapped storage", unit, e.getCause());
      return null;
    } catch (CancellationException e) {
      return null;
    }
  }

  private void release(Unit unit) {
    if (unit.state == State.FETCHING || unit.state == State.RETAINED) {
      if (unit.state == State.RETAINED) {
        retainedBytes -= unit.length;
      }
      // A fetch that has not started yet does not run; one that is running returns the bytes when it ends
      unit.fetchClaimed = true;
      CompletableFuture<byte[]> future = unit.bytes;
      if (future != null) {
        future.cancel(false);
      }
      unit.bytes = null;
      if (!unit.fetchRunning) {
        returnReservation(unit);
      }
    }
    unit.state = State.RELEASED;
  }

  private void returnReservation(Unit unit) {
    heldBytes -= unit.length;
    EXECUTOR_BUDGET.release(unit.length);
  }

  private void releaseAll() {
    for (FileEntry file : files) {
      release(file.unit);
      file.ranges.clear();
      file.lastRange = -1;
    }
    if (ranges != null) {
      for (Unit range : ranges) {
        release(range);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Consumption
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The header pass is done with a file: keep it for the merge if that leaves room in the window for the files still
   * ahead of the header pass, else drop it.
   */
  private void finishScan(FileEntry file) {
    Unit unit = file.unit;
    if (unit.state != State.FETCHING) {
      return;
    }
    long reserve = Math.min(windowBytes / 2, file.bytesAfter);
    if (retainedBytes + unit.length + reserve <= windowBytes) {
      unit.state = State.RETAINED;
      retainedBytes += unit.length;
    } else {
      release(unit);
    }
  }

  /**
   * The header pass opens a file.
   */
  private void onScanOpen(FileEntry file) {
    if (file.index > scanCursor) {
      for (int i = Math.max(scanCursor, 0); i < file.index; i++) {
        finishScan(files.get(i));
      }
      scanCursor = file.index;
    }
    if (file.unit.state == State.PENDING) {
      file.unit.state = State.INLINE;
      stats.inlineReads++;
    }
    fillWindow();
  }

  /**
   * The merge reads the block content at {@code index}: everything before it is done.
   */
  private void advanceMerge(int index) {
    if (index <= mergeCursor) {
      return;
    }
    for (int i = mergeCursor; i < index && i < ranges.size(); i++) {
      Unit range = ranges.get(i);
      if (range.state == State.FETCHING) {
        release(range);
      } else if (range.state == State.PENDING) {
        range.state = State.RELEASED;
      }
    }
    for (FileEntry file : files) {
      if (file.unit.state == State.RETAINED && file.lastRange < index) {
        release(file.unit);
      }
    }
    mergeCursor = index;
    fillWindow();
  }

  /**
   * Finds the fetched bytes holding the given position of a file, if any, and moves the merge to the block there.
   */
  private synchronized Lookup lookup(FileEntry file, long pos, ReadAheadStream stream) {
    if (closed) {
      return Lookup.NONE;
    }
    if (ranges == null) {
      Unit unit = file.unit;
      return unit.state == State.FETCHING || unit.state == State.RETAINED ? new Lookup(unit, Long.MAX_VALUE) : Lookup.NONE;
    }
    Unit range = null;
    long nextStart = Long.MAX_VALUE;
    for (Unit candidate : file.ranges) {
      if (candidate.offset <= pos && pos < candidate.offset + candidate.length) {
        if (range == null || (range.index < mergeCursor && candidate.index >= mergeCursor)) {
          range = candidate;
        }
      } else if (candidate.offset > pos) {
        nextStart = Math.min(nextStart, candidate.offset);
      }
    }
    if (range != null) {
      stream.maxRange = Math.max(stream.maxRange, range.index);
      advanceMerge(range.index);
      if (range.state == State.PENDING) {
        range.state = State.INLINE;
        stats.inlineReads++;
        fillWindow();
      }
    }
    if (file.unit.state == State.RETAINED) {
      return new Lookup(file.unit, Long.MAX_VALUE);
    }
    if (range != null && range.state == State.FETCHING) {
      return new Lookup(range, range.offset + range.length);
    }
    // Nothing in memory up to the next block content of the file
    return new Lookup(null, range != null ? range.offset + range.length : nextStart);
  }

  private synchronized void onStreamClosed(ReadAheadStream stream) {
    if (!closed && ranges != null && stream.maxRange >= 0) {
      advanceMerge(stream.maxRange + 1);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // HoodieStorage
  // ---------------------------------------------------------------------------------------------------------------

  @Override
  public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream) throws IOException {
    FileEntry file;
    synchronized (this) {
      file = closed ? null : filesByPath.get(key(path));
      if (file != null && ranges == null) {
        onScanOpen(file);
      }
    }
    if (file == null) {
      return delegate.openSeekable(path, bufferSize, wrapStream);
    }
    return new ReadAheadSeekableStream(new ReadAheadStream(file, () -> delegate.openSeekable(path, bufferSize, wrapStream)));
  }

  @Override
  public InputStream open(StoragePath path) throws IOException {
    return delegate.open(path);
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
  public boolean isEmpty(StoragePath path) throws IOException {
    return delegate.isEmpty(path);
  }

  @Override
  public HoodieStorage getRawStorage() {
    return delegate.getRawStorage();
  }

  /**
   * Drops every fetched byte and returns it to the executor-wide cap. The wrapped storage belongs to the caller.
   */
  @Override
  public synchronized void close() {
    if (!closed) {
      releaseAll();
      closed = true;
    }
  }

  private static String key(StoragePath path) {
    return path.getPathWithoutSchemeAndAuthority().toString();
  }

  private static ThreadPoolExecutor createFetchPool() {
    ThreadPoolExecutor pool = new ThreadPoolExecutor(
        FETCH_THREADS, FETCH_THREADS, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
        runnable -> {
          Thread thread = new Thread(runnable, "hudi-log-read-ahead-" + THREAD_ID.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        });
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Model
  // ---------------------------------------------------------------------------------------------------------------

  private enum State {
    // Not fetched yet
    PENDING,
    // Fetched or being fetched; its bytes count against the window
    FETCHING,
    // A file the header pass is done with, kept for the merge; its bytes count against the window
    RETAINED,
    // A block content served from its retained file
    IN_FILE,
    // Read through the wrapped storage
    INLINE,
    // Done with; its bytes are dropped
    RELEASED
  }

  private static final class FileEntry {
    private final int index;
    private final StoragePath path;
    private final long size;
    private final Unit unit;
    private final List<Unit> ranges = new ArrayList<>();
    private long bytesAfter;
    private int lastRange = -1;

    private FileEntry(int index, HoodieLogFile logFile) {
      this.index = index;
      this.path = logFile.getPath();
      this.size = logFile.getFileSize();
      long length = size > 0 && size <= Integer.MAX_VALUE ? size : -1;
      this.unit = new Unit(this, 0, (int) length);
      this.unit.wholeFile = true;
    }
  }

  /**
   * A whole log file or the content of one block: what is fetched, held and released as one.
   */
  private static final class Unit {
    private final FileEntry file;
    private final long offset;
    private final int length;
    // Guarded by the storage: a thread has taken the fetch, and that thread has not handed its bytes over yet
    private boolean fetchClaimed;
    private boolean fetchRunning;
    private boolean wholeFile;
    private int index = -1;
    private State state = State.PENDING;
    private volatile CompletableFuture<byte[]> bytes;

    private Unit(FileEntry file, long offset, int length) {
      this.file = file;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public String toString() {
      return wholeFile ? "log file " + file.path : "bytes [" + offset + ", " + (offset + length) + ") of " + file.path;
    }
  }

  private static final class Lookup {
    private static final Lookup NONE = new Lookup(null, Long.MAX_VALUE);
    // The unit holding the position, or null when it is not in memory
    private final Unit unit;
    // The position up to which this lookup holds
    private final long validUntil;

    private Lookup(Unit unit, long validUntil) {
      this.unit = unit;
      this.validUntil = validUntil;
    }
  }

  static final class Stats {
    int filesFetched;
    int blocksFetched;
    int inlineReads;
    int executorCapDenials;
    long peakHeldBytes;
  }

  private static final class ExecutorBudget {
    private final AtomicLong used = new AtomicLong();
    private final long cap;

    private ExecutorBudget(long cap) {
      this.cap = cap;
    }

    private boolean tryAcquire(long bytes, long limit) {
      long effectiveCap = Math.min(cap, limit);
      while (true) {
        long current = used.get();
        if (current + bytes > effectiveCap) {
          return false;
        }
        if (used.compareAndSet(current, current + bytes)) {
          return true;
        }
      }
    }

    private void release(long bytes) {
      used.addAndGet(-bytes);
    }
  }

  @FunctionalInterface
  private interface StreamOpener {
    SeekableDataInputStream open() throws IOException;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Streams
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * A stream over one log file that reads from fetched bytes where it can and from the wrapped storage elsewhere.
   */
  private final class ReadAheadStream extends InputStream {
    private final FileEntry file;
    private final StreamOpener opener;
    private SeekableDataInputStream inline;
    private long inlinePos = -1;
    private long pos;
    // Fetched bytes around the position, if any, and the position they start at
    private byte[] buffer;
    private long bufferStart;
    // The position up to which the last lookup holds
    private long lookupStart = -1;
    private long lookupEnd = -1;
    private int maxRange = -1;

    private ReadAheadStream(FileEntry file, StreamOpener opener) {
      this.file = file;
      this.opener = opener;
    }

    private long knownSize() {
      return file.size > 0 ? file.size : -1;
    }

    private void locate() throws IOException {
      if (pos >= lookupStart && pos < lookupEnd) {
        return;
      }
      Lookup lookup = lookup(file, pos, this);
      byte[] bytes = lookup.unit == null ? null : awaitBytes(lookup.unit);
      if (bytes != null && pos >= lookup.unit.offset && pos < lookup.unit.offset + bytes.length) {
        buffer = bytes;
        bufferStart = lookup.unit.offset;
        lookupStart = bufferStart;
        lookupEnd = bufferStart + bytes.length;
      } else {
        buffer = null;
        lookupStart = pos;
        lookupEnd = lookup.validUntil;
      }
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] target, int offset, int length) throws IOException {
      if (length == 0) {
        return 0;
      }
      long size = knownSize();
      if (size >= 0 && pos >= size) {
        return -1;
      }
      locate();
      if (buffer != null) {
        int n = (int) Math.min(length, bufferStart + buffer.length - pos);
        System.arraycopy(buffer, (int) (pos - bufferStart), target, offset, n);
        pos += n;
        return n;
      }
      SeekableDataInputStream in = inlineStream();
      if (inlinePos != pos) {
        in.seek(pos);
        inlinePos = pos;
      }
      // Stop at the next fetched bytes, so that they are read from memory
      int max = lookupEnd > pos ? (int) Math.min(length, lookupEnd - pos) : length;
      int n = in.read(target, offset, max);
      if (n > 0) {
        pos += n;
        inlinePos = pos;
      }
      return n;
    }

    private SeekableDataInputStream inlineStream() throws IOException {
      if (inline == null) {
        inline = opener.open();
        inlinePos = 0;
      }
      return inline;
    }

    @Override
    public long skip(long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      long size = knownSize();
      long target = size >= 0 ? Math.min(pos + n, size) : pos + n;
      long skipped = Math.max(0, target - pos);
      pos += skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      if (buffer != null && pos >= bufferStart && pos < bufferStart + buffer.length) {
        return (int) (bufferStart + buffer.length - pos);
      }
      return 0;
    }

    private long getPos() {
      return pos;
    }

    private void seek(long target) throws IOException {
      long size = knownSize();
      if (target < 0 || (size >= 0 && target > size)) {
        throw new EOFException("Cannot seek to " + target + " in " + file.path + " of " + size + " bytes");
      }
      if (size < 0 && !(buffer != null && target >= bufferStart && target <= bufferStart + buffer.length)) {
        // The size is unknown: let the file system decide whether the position exists
        SeekableDataInputStream in = inlineStream();
        in.seek(target);
        inlinePos = target;
      }
      pos = target;
    }

    @Override
    public void close() throws IOException {
      try {
        if (inline != null) {
          inline.close();
          inline = null;
        }
      } finally {
        buffer = null;
        onStreamClosed(this);
      }
    }
  }

  private static final class ReadAheadSeekableStream extends SeekableDataInputStream {
    private final ReadAheadStream stream;

    private ReadAheadSeekableStream(ReadAheadStream stream) {
      super(stream);
      this.stream = stream;
    }

    @Override
    public long getPos() {
      return stream.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
      stream.seek(pos);
    }
  }
}
