/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieRangeReadHandle.ByteRange;
import org.apache.hudi.storage.HoodieRangeReadHandle.RangeReadResult;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parquet {@link InputFile} serving reads contained by prefetched ranges from memory. Reads that do
 * not fit entirely inside one cached range use a normal seekable stream and are classified so a
 * range-alignment bug cannot masquerade as successful prefetching.
 */
public final class HoodiePrefetchedParquetInputFile implements InputFile {

  public enum RegionKind {
    DICTIONARY,
    PAGE,
    METADATA
  }

  /** A planner-known region used only to explain cache misses. */
  public static final class ReadRegion {
    private final long offset;
    private final long end;
    private final RegionKind kind;

    public ReadRegion(long offset, long length, RegionKind kind) {
      if (offset < 0 || length <= 0 || offset > Long.MAX_VALUE - length) {
        throw new IllegalArgumentException("Invalid read region: offset=" + offset + ", length=" + length);
      }
      this.offset = offset;
      this.end = offset + length;
      this.kind = Objects.requireNonNull(kind, "kind");
    }

    private boolean overlaps(long readStart, long readEnd) {
      return offset < readEnd && readStart < end;
    }
  }

  /** Thread-safe counters shared by every stream opened from this input file. */
  public static final class Metrics {
    private final AtomicLong prefetchHitBytes = new AtomicLong();
    private final AtomicLong partialOverlapBytes = new AtomicLong();
    private final AtomicLong metadataMissBytes = new AtomicLong();
    private final AtomicLong dictionaryMissBytes = new AtomicLong();
    private final AtomicLong uncoveredPageMissBytes = new AtomicLong();

    public long getPrefetchHitBytes() {
      return prefetchHitBytes.get();
    }

    public long getPartialOverlapBytes() {
      return partialOverlapBytes.get();
    }

    public long getMetadataMissBytes() {
      return metadataMissBytes.get();
    }

    public long getDictionaryMissBytes() {
      return dictionaryMissBytes.get();
    }

    public long getUncoveredPageMissBytes() {
      return uncoveredPageMissBytes.get();
    }
  }

  private static final class CachedRange {
    private final long offset;
    private final long end;
    private final ByteBuffer data;

    private CachedRange(RangeReadResult result) {
      ByteRange range = result.getRange();
      this.offset = range.getOffset();
      this.end = range.getEnd();
      this.data = result.getData();
      if (data.remaining() != range.getLength()) {
        throw new IllegalArgumentException(
            "Prefetched buffer length " + data.remaining() + " does not match " + range);
      }
    }

    private boolean contains(long readStart, long readEnd) {
      return offset <= readStart && readEnd <= end;
    }

    private boolean overlaps(long readStart, long readEnd) {
      return offset < readEnd && readStart < end;
    }

    private void copy(long readStart, byte[] target, int targetOffset, int length) {
      ByteBuffer source = data.duplicate();
      source.position(Math.toIntExact(readStart - offset));
      source.limit(source.position() + length);
      source.get(target, targetOffset, length);
    }

    private void copy(long readStart, ByteBuffer target, int length) {
      ByteBuffer source = data.duplicate();
      source.position(Math.toIntExact(readStart - offset));
      source.limit(source.position() + length);
      target.put(source);
    }
  }

  private final HoodieStorage storage;
  private final StoragePath path;
  private final long length;
  private final int bufferSize;
  private final List<CachedRange> cachedRanges;
  private final List<ReadRegion> readRegions;
  private final Metrics metrics = new Metrics();

  public HoodiePrefetchedParquetInputFile(HoodieStorage storage,
                                           StoragePath path,
                                           long length,
                                           List<RangeReadResult> prefetched,
                                           List<ReadRegion> readRegions) {
    this.storage = Objects.requireNonNull(storage, "storage");
    this.path = Objects.requireNonNull(path, "path");
    this.length = length;
    this.bufferSize = storage.getDefaultBufferSize();
    this.cachedRanges = new ArrayList<>(prefetched.size());
    for (RangeReadResult result : prefetched) {
      cachedRanges.add(new CachedRange(result));
    }
    cachedRanges.sort(Comparator.comparingLong(range -> range.offset));
    assertNonOverlapping(cachedRanges);
    this.readRegions = new ArrayList<>(readRegions);
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new PrefetchedSeekableInputStream();
  }

  public Metrics getMetrics() {
    return metrics;
  }

  private static void assertNonOverlapping(List<CachedRange> ranges) {
    for (int i = 1; i < ranges.size(); i++) {
      if (ranges.get(i - 1).end > ranges.get(i).offset) {
        throw new IllegalArgumentException("Prefetched ranges overlap; planner must merge them first");
      }
    }
  }

  private CachedRange containingRange(long offset, int requestedLength) {
    long end = checkedEnd(offset, requestedLength);
    int low = 0;
    int high = cachedRanges.size() - 1;
    int candidate = -1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (cachedRanges.get(mid).offset <= offset) {
        candidate = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return candidate >= 0 && cachedRanges.get(candidate).contains(offset, end)
        ? cachedRanges.get(candidate) : null;
  }

  private boolean partiallyOverlaps(long offset, int requestedLength) {
    long end = checkedEnd(offset, requestedLength);
    for (CachedRange range : cachedRanges) {
      if (range.offset >= end) {
        return false;
      }
      if (range.overlaps(offset, end)) {
        return true;
      }
    }
    return false;
  }

  private void recordMiss(long offset, int requestedLength) {
    if (partiallyOverlaps(offset, requestedLength)) {
      metrics.partialOverlapBytes.addAndGet(requestedLength);
    }
    long end = checkedEnd(offset, requestedLength);
    RegionKind reason = RegionKind.METADATA;
    for (ReadRegion region : readRegions) {
      if (region.overlaps(offset, end)) {
        reason = region.kind;
        if (reason == RegionKind.DICTIONARY) {
          break;
        }
      }
    }
    switch (reason) {
      case DICTIONARY:
        metrics.dictionaryMissBytes.addAndGet(requestedLength);
        break;
      case PAGE:
        metrics.uncoveredPageMissBytes.addAndGet(requestedLength);
        break;
      case METADATA:
      default:
        metrics.metadataMissBytes.addAndGet(requestedLength);
    }
  }

  private static long checkedEnd(long offset, int requestedLength) {
    if (offset < 0 || requestedLength < 0 || offset > Long.MAX_VALUE - requestedLength) {
      throw new IllegalArgumentException("Invalid read: offset=" + offset + ", length=" + requestedLength);
    }
    return offset + requestedLength;
  }

  private final class PrefetchedSeekableInputStream extends SeekableInputStream {
    private long position;
    private SeekableDataInputStream fallback;

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void seek(long newPosition) throws IOException {
      if (newPosition < 0 || newPosition > length) {
        throw new IOException("Invalid seek to " + newPosition + " for " + path + " length=" + length);
      }
      position = newPosition;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int read = read(one, 0, 1);
      return read < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] target, int offset, int requestedLength) throws IOException {
      if (offset < 0 || requestedLength < 0 || requestedLength > target.length - offset) {
        throw new IndexOutOfBoundsException(
            "offset=" + offset + ", length=" + requestedLength + ", targetLength=" + target.length);
      }
      if (requestedLength == 0) {
        return 0;
      }
      if (position >= length) {
        return -1;
      }
      int available = Math.toIntExact(Math.min(requestedLength, length - position));
      CachedRange cached = containingRange(position, available);
      if (cached != null) {
        cached.copy(position, target, offset, available);
        metrics.prefetchHitBytes.addAndGet(available);
      } else {
        recordMiss(position, available);
        SeekableDataInputStream stream = fallback();
        stream.seek(position);
        int total = 0;
        while (total < available) {
          int count = stream.read(target, offset + total, available - total);
          if (count < 0) {
            break;
          }
          total += count;
        }
        if (total == 0) {
          return -1;
        }
        available = total;
      }
      position += available;
      return available;
    }

    @Override
    public void readFully(byte[] target) throws IOException {
      readFully(target, 0, target.length);
    }

    @Override
    public void readFully(byte[] target, int offset, int requestedLength) throws IOException {
      int total = 0;
      while (total < requestedLength) {
        int count = read(target, offset + total, requestedLength - total);
        if (count < 0) {
          throw new EOFException("Reached EOF reading " + requestedLength + " bytes at " + position);
        }
        total += count;
      }
    }

    @Override
    public int read(ByteBuffer target) throws IOException {
      if (!target.hasRemaining()) {
        return 0;
      }
      if (position >= length) {
        return -1;
      }
      int available = Math.toIntExact(Math.min(target.remaining(), length - position));
      CachedRange cached = containingRange(position, available);
      if (cached != null) {
        cached.copy(position, target, available);
        metrics.prefetchHitBytes.addAndGet(available);
        position += available;
        return available;
      }
      byte[] bytes = new byte[available];
      int count = read(bytes, 0, available);
      if (count > 0) {
        target.put(bytes, 0, count);
      }
      return count;
    }

    @Override
    public void readFully(ByteBuffer target) throws IOException {
      while (target.hasRemaining()) {
        if (read(target) < 0) {
          throw new EOFException("Reached EOF reading ByteBuffer at " + position);
        }
      }
    }

    private SeekableDataInputStream fallback() throws IOException {
      if (fallback == null) {
        fallback = storage.openSeekable(path, bufferSize, false);
      }
      return fallback;
    }

    @Override
    public void close() throws IOException {
      if (fallback != null) {
        fallback.close();
      }
    }
  }
}
