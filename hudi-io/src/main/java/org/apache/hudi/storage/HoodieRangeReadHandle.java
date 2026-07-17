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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Reads known byte ranges with bounded concurrency while keeping storage-specific APIs out of
 * format and engine code.
 */
public interface HoodieRangeReadHandle extends AutoCloseable {

  /**
   * Reads every range or fails the whole batch. Implementations must not silently degrade to a
   * serial or partial result when the deadline or allocation budget is exceeded.
   */
  RangeReadBatch readRanges(StoragePath path,
                            List<ByteRange> ranges,
                            long deadlineNanos,
                            long maxPlannedBytes) throws IOException;

  /** Immutable half-open byte range. */
  final class ByteRange {
    private final long offset;
    private final int length;

    public ByteRange(long offset, int length) {
      if (offset < 0 || length <= 0) {
        throw new IllegalArgumentException("Invalid byte range: offset=" + offset + ", length=" + length);
      }
      this.offset = offset;
      this.length = length;
    }

    public long getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }

    public long getEnd() {
      return offset + length;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ByteRange)) {
        return false;
      }
      ByteRange that = (ByteRange) other;
      return offset == that.offset && length == that.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(offset, length);
    }

    @Override
    public String toString() {
      return "[" + offset + ", " + getEnd() + ")";
    }
  }

  /** One range and its fully populated data buffer. */
  final class RangeReadResult {
    private final ByteRange range;
    private final ByteBuffer data;

    public RangeReadResult(ByteRange range, ByteBuffer data) {
      this.range = Objects.requireNonNull(range, "range");
      this.data = Objects.requireNonNull(data, "data").asReadOnlyBuffer();
    }

    public ByteRange getRange() {
      return range;
    }

    public ByteBuffer getData() {
      return data.asReadOnlyBuffer();
    }
  }

  /** Metrics for the I/O lifecycle only; decode happens after permits have been released. */
  final class RangeReadMetrics {
    private final long rangeCount;
    private final long byteCount;
    private final long retryCount;
    private final int maxRangesInFlight;
    private final long openNanos;
    private final long readNanos;
    private final long waitNanos;
    private final int effectiveConcurrency;

    public RangeReadMetrics(long rangeCount,
                            long byteCount,
                            long retryCount,
                            int maxRangesInFlight,
                            long openNanos,
                            long readNanos,
                            long waitNanos,
                            int effectiveConcurrency) {
      this.rangeCount = rangeCount;
      this.byteCount = byteCount;
      this.retryCount = retryCount;
      this.maxRangesInFlight = maxRangesInFlight;
      this.openNanos = openNanos;
      this.readNanos = readNanos;
      this.waitNanos = waitNanos;
      this.effectiveConcurrency = effectiveConcurrency;
    }

    public long getRangeCount() {
      return rangeCount;
    }

    public long getByteCount() {
      return byteCount;
    }

    public long getRetryCount() {
      return retryCount;
    }

    public int getMaxRangesInFlight() {
      return maxRangesInFlight;
    }

    public long getOpenNanos() {
      return openNanos;
    }

    public long getReadNanos() {
      return readNanos;
    }

    public long getWaitNanos() {
      return waitNanos;
    }

    public int getEffectiveConcurrency() {
      return effectiveConcurrency;
    }
  }

  /** Ordered range results and their aggregate I/O metrics. */
  final class RangeReadBatch {
    private final List<RangeReadResult> results;
    private final RangeReadMetrics metrics;

    public RangeReadBatch(List<RangeReadResult> results, RangeReadMetrics metrics) {
      this.results = Collections.unmodifiableList(Objects.requireNonNull(results, "results"));
      this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    public List<RangeReadResult> getResults() {
      return results;
    }

    public RangeReadMetrics getMetrics() {
      return metrics;
    }
  }
}
