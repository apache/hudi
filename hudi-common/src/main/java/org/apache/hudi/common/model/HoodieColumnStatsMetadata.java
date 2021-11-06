package org.apache.hudi.common.model;

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

import java.util.Objects;

/**
 * Hoodie Column Stats metadata.
 */
public class HoodieColumnStatsMetadata<T> {

  private final String partitionPath;
  private final String filePath;
  private final String columnName;
  private final T minValue;
  private final T maxValue;
  private final boolean isDeleted;

  public HoodieColumnStatsMetadata(final String partitionPath, final String filePath, final String columnName, final T minValue, final T maxValue,
                                   boolean isDeleted) {
    this.partitionPath = partitionPath;
    this.filePath = filePath;
    this.columnName = columnName;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.isDeleted = isDeleted;
  }

  public String getPartitionPath() {
    return partitionPath;
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

  public boolean isDeleted() {
    return isDeleted;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HoodieColumnStatsMetadata<?> that = (HoodieColumnStatsMetadata<?>) o;
    return Objects.equals(getPartitionPath(), that.getPartitionPath())
        && Objects.equals(getFilePath(), that.getFilePath())
        && Objects.equals(getColumnName(), that.getColumnName())
        && Objects.equals(getMinValue(), that.getMinValue())
        && Objects.equals(getMaxValue(), that.getMaxValue())
        && Objects.equals(isDeleted(), that.isDeleted());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumnName(), getMinValue(), getMaxValue());
  }

  @Override
  public String toString() {
    return "HoodieColumnRangeMetadata{"
        + "partitionPath ='" + partitionPath + '\''
        + "filePath ='" + filePath + '\''
        + "columnName='" + columnName + '\''
        + ", minValue=" + minValue
        + ", maxValue=" + maxValue
        + ", isDeleted=" + isDeleted + '}';
  }
}