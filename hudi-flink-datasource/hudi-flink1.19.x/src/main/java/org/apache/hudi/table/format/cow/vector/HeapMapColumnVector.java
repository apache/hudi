/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow.vector;

import lombok.Getter;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.columnar.ColumnarMapData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.MapColumnVector;
import org.apache.flink.table.data.columnar.vector.heap.AbstractHeapVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

/**
 * This class represents a nullable heap map column vector.
 *
 * <p>Mirrors {@code org.apache.flink.table.data.columnar.vector.heap.HeapMapVector} from
 * Flink 2.1 (FLINK-35702). One deliberate divergence from upstream is preserved for backward
 * compatibility: the {@code keys} / {@code values} fields are typed
 * {@link WritableColumnVector} rather than upstream's {@link ColumnVector}, so the existing
 * Lombok-generated {@code getKeys()} / {@code getValues()} accessors keep their original
 * signature. Callers wanting the Flink-2.1 contract (a {@code ColumnVector}) use
 * {@link #getKeyColumnVector()} / {@link #getValueColumnVector()}.
 */
public class HeapMapColumnVector extends AbstractHeapVector
    implements WritableColumnVector, MapColumnVector {

  @Getter
  private WritableColumnVector keys;
  @Getter
  private WritableColumnVector values;

  // ---------------------------------------------------------------------------------------------
  // Flink 2.1 Dremel-style state. Populated by {@link
  // org.apache.hudi.table.format.cow.vector.reader.NestedColumnReader} (FLINK-35702 port) and
  // consumed by {@link #getMap(int)}.
  // ---------------------------------------------------------------------------------------------
  private long[] offsets;
  private long[] lengths;
  private int size;

  public HeapMapColumnVector(int len, WritableColumnVector keys, WritableColumnVector values) {
    super(len);
    this.offsets = new long[len];
    this.lengths = new long[len];
    this.keys = keys;
    this.values = values;
  }

  public long[] getOffsets() {
    return offsets;
  }

  public void setOffsets(long[] offsets) {
    this.offsets = offsets;
  }

  public long[] getLengths() {
    return lengths;
  }

  public void setLengths(long[] lengths) {
    this.lengths = lengths;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public void setKeys(WritableColumnVector keys) {
    this.keys = keys;
  }

  public void setValues(WritableColumnVector values) {
    this.values = values;
  }

  /**
   * Returns the keys child vector typed as {@link ColumnVector}, matching the Flink 2.1 contract
   * consumed by {@code NestedColumnReader}. Functionally equivalent to {@link #getKeys()}.
   */
  public ColumnVector getKeyColumnVector() {
    return keys;
  }

  /** Counterpart of {@link #getKeyColumnVector()} for the values child vector. */
  public ColumnVector getValueColumnVector() {
    return values;
  }

  @Override
  public MapData getMap(int rowId) {
    long offset = offsets[rowId];
    long length = lengths[rowId];
    return new ColumnarMapData(keys, values, (int) offset, (int) length);
  }
}
