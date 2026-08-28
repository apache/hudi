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

import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.RowColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.heap.AbstractHeapVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

/**
 * This class represents a nullable heap row column vector.
 */
public class HeapRowColumnVector extends AbstractHeapVector
    implements WritableColumnVector, RowColumnVector {

  public WritableColumnVector[] vectors;

  public HeapRowColumnVector(int len, WritableColumnVector... vectors) {
    super(len);
    this.vectors = vectors;
  }

  /**
   * Flink 2.1-compatible accessor for the children vectors. Backed by the existing public {@code
   * vectors} field so legacy callers continue to work; the new {@link
   * org.apache.hudi.table.format.cow.vector.reader.NestedColumnReader} (FLINK-35702 port) and any
   * future Flink-2.1-style caller use this accessor.
   */
  public WritableColumnVector[] getFields() {
    return vectors;
  }

  /** Counterpart of {@link #getFields()}. */
  public void setFields(WritableColumnVector[] fields) {
    this.vectors = fields;
  }

  @Override
  public ColumnarRowData getRow(int i) {
    ColumnarRowData columnarRowData = new ColumnarRowData(new VectorizedColumnBatch(vectors));
    columnarRowData.setRowId(i);
    return columnarRowData;
  }

  @Override
  public void reset() {
    super.reset();
    for (WritableColumnVector vector : vectors) {
      vector.reset();
    }
  }
}
