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

import org.apache.parquet.schema.PrimitiveStringifier;

import java.util.Objects;

/**
 * Hoodie Range metadata.
 */
public class HoodieColumnRangeMetadata<T> {
  private final String filePath;
  private final String columnName;
  private final T minValue;
  private final T maxValue;
  private final long numNulls;
  private final PrimitiveStringifier stringifier;

  public HoodieColumnRangeMetadata(final String filePath, final String columnName, final T minValue, final T maxValue, final long numNulls, final PrimitiveStringifier stringifier) {
    this.filePath = filePath;
    this.columnName = columnName;
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.numNulls = numNulls;
    this.stringifier = stringifier;
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

  public PrimitiveStringifier getStringifier() {
    return stringifier;
  }

  public long getNumNulls() {
    return numNulls;
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
        && Objects.equals(getNumNulls(), that.getNumNulls());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumnName(), getMinValue(), getMaxValue(), getNumNulls());
  }

  @Override
  public String toString() {
    return "HoodieColumnRangeMetadata{"
        + "filePath ='" + filePath + '\''
        + "columnName='" + columnName + '\''
        + ", minValue=" + minValue
        + ", maxValue=" + maxValue
        + ", numNulls=" + numNulls + '}';
  }
}
