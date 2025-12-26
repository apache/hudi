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

package org.apache.hudi.stats;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.hudi.stats.ValueMetadata.getEmptyValueMetadata;

/**
 * Hoodie metadata for the column range of data stored in columnar format (like Parquet)
 *
 * NOTE: {@link Comparable} is used as raw-type so that we can handle polymorphism, where
 *        caller apriori is not aware of the type {@link HoodieColumnRangeMetadata} is
 *        associated with
 */
@SuppressWarnings("rawtype")
@Value
public class HoodieColumnRangeMetadata<T extends Comparable> implements Serializable {

  String filePath;
  String columnName;
  @Nullable
  T minValue;
  @Nullable
  T maxValue;
  long nullCount;
  long valueCount;
  long totalSize;
  long totalUncompressedSize;
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  ValueMetadata valueMetadata;

  public Object getMinValueWrapped() {
    return getValueMetadata().wrapValue(getMinValue());
  }

  public Object getMaxValueWrapped() {
    return getValueMetadata().wrapValue(getMaxValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumnName(), getMinValue(), getMaxValue(), getNullCount());
  }

  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<T> create(String filePath,
                                                                              String columnName,
                                                                              @Nullable T minValue,
                                                                              @Nullable T maxValue,
                                                                              long nullCount,
                                                                              long valueCount,
                                                                              long totalSize,
                                                                              long totalUncompressedSize,
                                                                              ValueMetadata valueMetadata) throws IllegalArgumentException {
    valueMetadata.validate(minValue, maxValue);
    return new HoodieColumnRangeMetadata<>(filePath, columnName, minValue, maxValue, nullCount, valueCount, totalSize, totalUncompressedSize, valueMetadata);
  }

  /**
   * Converts instance of {@link HoodieMetadataColumnStats} to {@link HoodieColumnRangeMetadata}
   */
  public static HoodieColumnRangeMetadata<Comparable> fromColumnStats(HoodieMetadataColumnStats columnStats) {
    ValueMetadata valueMetadata = ValueMetadata.getValueMetadata(columnStats.getValueType());
    return HoodieColumnRangeMetadata.<Comparable>create(
        columnStats.getFileName(),
        columnStats.getColumnName(),
        valueMetadata.unwrapValue(columnStats.getMinValue()),
        valueMetadata.unwrapValue(columnStats.getMaxValue()),
        columnStats.getNullCount(),
        columnStats.getValueCount(),
        columnStats.getTotalSize(),
        columnStats.getTotalUncompressedSize(),
        valueMetadata);
  }

  @SuppressWarnings("rawtype")
  public static HoodieColumnRangeMetadata<Comparable> stub(String filePath,
                                                           String columnName,
                                                           HoodieIndexVersion indexVersion) {
    return new HoodieColumnRangeMetadata<>(filePath, columnName, null, null, -1, -1, -1, -1, getEmptyValueMetadata(indexVersion));
  }

  public static HoodieColumnRangeMetadata<Comparable> createEmpty(String filePath,
                                                                  String columnName,
                                                                  HoodieIndexVersion indexVersion) {
    return new HoodieColumnRangeMetadata(filePath, columnName, null, null, 0L, 0L, 0L, 0L, getEmptyValueMetadata(indexVersion));
  }

  /**
   * Merges the given two column range metadata.
   */
  public static <T extends Comparable<T>> HoodieColumnRangeMetadata<T> merge(
      HoodieColumnRangeMetadata<T> left,
      HoodieColumnRangeMetadata<T> right) {
    if (left == null || right == null) {
      return left == null ? right : left;
    }

    if (left.getValueMetadata().getValueType() != right.getValueMetadata().getValueType()) {
      throw new IllegalArgumentException("Value types should be the same for merging column ranges");
    } else if (left.getValueMetadata().getValueType() != ValueType.V1) {
      if (left.minValue != null && right.minValue != null && left.minValue.getClass() != right.minValue.getClass()) {
        throw new IllegalArgumentException("Value types should be the same for merging column ranges");
      } else if (left.maxValue != null && right.maxValue != null && left.maxValue.getClass() != right.maxValue.getClass()) {
        throw new IllegalArgumentException("Value types should be the same for merging column ranges");
      }
    }

    ValidationUtils.checkArgument(left.getColumnName().equals(right.getColumnName()),
        "Column names should be the same for merging column ranges");
    String filePath = left.getFilePath();
    String columnName = left.getColumnName();
    T min = minVal(left.getMinValue(), right.getMinValue());
    T max = maxVal(left.getMaxValue(), right.getMaxValue());
    long nullCount = left.getNullCount() + right.getNullCount();
    long valueCount = left.getValueCount() + right.getValueCount();
    long totalSize = left.getTotalSize() + right.getTotalSize();
    long totalUncompressedSize = left.getTotalUncompressedSize() + right.getTotalUncompressedSize();

    return new HoodieColumnRangeMetadata<>(filePath, columnName, min, max, nullCount, valueCount, totalSize, totalUncompressedSize, left.getValueMetadata());
  }

  private static <T extends Comparable<T>> T minVal(T val1, T val2) {
    if (val1 == null) {
      return val2;
    }
    if (val2 == null) {
      return val1;
    }
    return val1.compareTo(val2) < 0 ? val1 : val2;
  }

  private static <T extends Comparable<T>> T maxVal(T val1, T val2) {
    if (val1 == null) {
      return val2;
    }
    if (val2 == null) {
      return val1;
    }
    return val1.compareTo(val2) > 0 ? val1 : val2;
  }
}
