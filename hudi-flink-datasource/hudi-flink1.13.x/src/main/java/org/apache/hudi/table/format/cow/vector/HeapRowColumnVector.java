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

import org.apache.hudi.table.data.ColumnarRowData;
import org.apache.hudi.table.data.vector.RowColumnVector;
import org.apache.hudi.table.data.vector.VectorizedColumnBatch;

import org.apache.flink.table.data.vector.heap.AbstractHeapVector;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;

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

  @Override
  public ColumnarRowData getRow(int i) {
    ColumnarRowData columnarRowData = new ColumnarRowData(new VectorizedColumnBatch(vectors));
    columnarRowData.setRowId(i);
    return columnarRowData;
  }
}
