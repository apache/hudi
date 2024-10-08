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

package org.apache.spark.sql.vectorized;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

/**
 * Used to project a columnar batch. Currently only supports change order or remove top level fields
 * TODO: [HUDI-8325] add type casting capability
 */
public class ColumnarBatchProjection extends ColumnarBatch {

  public static UnaryOperator<ColumnarBatch> generateProjection(StructType from, StructType to) {
    if (from.length() < to.length()) {
      throw new IllegalStateException(from + " has less columns than " + to);
    }

    if (from.equals(to)) {
      return UnaryOperator.identity();
    }

    int[] projection = new int[to.size()];
    for (int i = 0; i < to.length(); i++) {
      projection[i] = from.fieldIndex(to.fields()[i].name());
    }

    return columnarBatch -> new ColumnarBatchProjection(columnarBatch, projection);
  }

  protected final int[] projection;
  protected final ColumnarBatchRowProjection rowProjection;
  protected ColumnarBatchProjection(ColumnarBatch batch, int[] projection) {
    super(batch.columns, batch.numRows());
    if (batch instanceof ColumnarBatchProjection) {
      this.projection = new int[projection.length];
      for (int i = 0; i < projection.length; i++) {
        this.projection[i] = ((ColumnarBatchProjection) batch).projection[projection[i]];
      }
    } else {
      this.projection = projection;
    }
    this.rowProjection = new ColumnarBatchRowProjection(columns, this.projection);
  }

  @Override
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = numRows;
    final ColumnarBatchRowProjection row = new ColumnarBatchRowProjection(columns, projection);
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }
    };
  }

  @Override
  public int numCols() {
    return projection.length;
  }

  @Override
  public ColumnVector column(int ordinal) {
    return super.column(projection[ordinal]);
  }

  @Override
  public InternalRow getRow(int rowId) {
    assert(rowId >= 0 && rowId < numRows);
    rowProjection.rowId = rowId;
    return rowProjection;
  }

}
