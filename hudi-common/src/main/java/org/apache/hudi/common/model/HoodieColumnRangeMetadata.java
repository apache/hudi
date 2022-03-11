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

package org.apache.hudi.common.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Hoodie Range metadata.
 */
public class HoodieColumnRangeMetadata<T> implements Serializable {
  private final String filePath;
  private final String columnName;
  private final T minValue;
  private final T maxValue;
  private final long nullCount;
  private final long valueCount;
  private final long totalSize;
  private final long totalUncompressedSize;

  public static final BiFunction<HoodieColumnRangeMetadata<Comparable>, HoodieColumnRangeMetadata<Comparable>, HoodieColumnRangeMetadata<Comparable>> COLUMN_RANGE_MERGE_FUNCTION =
      (oldColumnRange, newColumnRange) -> new HoodieColumnRangeMetadata<>(
          newColumnRange.getFilePath(),
          newColumnRange.getColumnName(),
          (Comparable) Arrays.asList(oldColumnRange.getMinValue(), newColumnRange.getMinValue())
              .stream().filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null),
          (Comparable) Arrays.asList(oldColumnRange.getMinValue(), newColumnRange.getMinValue())
              .stream().filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null),
          oldColumnRange.getNullCount() + newColumnRange.getNullCount(),
          oldColumnRange.getValueCount() + newColumnRange.getValueCount(),
          oldColumnRange.getTotalSize() + newColumnRange.getTotalSize(),
          oldColumnRange.getTotalUncompressedSize() + newColumnRange.getTotalUncompressedSize()
      );

  public HoodieColumnRangeMetadata(final String filePath, final String columnName, final T minValue, final T maxValue,
                                   final long nullCount, long valueCount, long totalSize, long totalUncompressedSize) {
    this.filePath = filePath;
    this.columnName = columnName;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.nullCount = nullCount;
    this.valueCount = valueCount;
    this.totalSize = totalSize;
    this.totalUncompressedSize = totalUncompressedSize;
  }

  public String getFilePath() {
    return this.filePath;
  }

  public String getColumnName() {
    return this.columnName;
  }

  public T getMinValue() {
    return this.minValue;
  }

  public T getMaxValue() {
    return this.maxValue;
  }

  public long getNullCount() {
    return nullCount;
  }

  public long getValueCount() {
    return valueCount;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public long getTotalUncompressedSize() {
    return totalUncompressedSize;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HoodieColumnRangeMetadata<?> that = (HoodieColumnRangeMetadata<?>) o;
    return Objects.equals(getFilePath(), that.getFilePath())
        && Objects.equals(getColumnName(), that.getColumnName())
        && Objects.equals(getMinValue(), that.getMinValue())
        && Objects.equals(getMaxValue(), that.getMaxValue())
        && Objects.equals(getNullCount(), that.getNullCount())
        && Objects.equals(getValueCount(), that.getValueCount())
        && Objects.equals(getTotalSize(), that.getTotalSize())
        && Objects.equals(getTotalUncompressedSize(), that.getTotalUncompressedSize());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumnName(), getMinValue(), getMaxValue(), getNullCount());
  }

  @Override
  public String toString() {
    return "HoodieColumnRangeMetadata{"
        + "filePath ='" + filePath + '\''
        + ", columnName='" + columnName + '\''
        + ", minValue=" + minValue
        + ", maxValue=" + maxValue
        + ", nullCount=" + nullCount
        + ", valueCount=" + valueCount
        + ", totalSize=" + totalSize
        + ", totalUncompressedSize=" + totalUncompressedSize
        + '}';
  }

  /**
   * Statistics that is collected in {@link org.apache.hudi.metadata.MetadataPartitionType#COLUMN_STATS} index.
   */
  public static final class Stats {
    public static final String VALUE_COUNT = "value_count";
    public static final String NULL_COUNT = "null_count";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String TOTAL_SIZE = "total_size";
    public static final String TOTAL_UNCOMPRESSED_SIZE = "total_uncompressed_size";

    private Stats() {  }
  }
}
