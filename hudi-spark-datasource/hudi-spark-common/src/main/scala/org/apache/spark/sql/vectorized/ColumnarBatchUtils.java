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

import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ColumnarBatchUtils {

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

    return columnarBatch -> {
      ColumnVector[] vectors = new ColumnVector[projection.length];
      for (int i = 0; i < projection.length; i++) {
        vectors[i] = columnarBatch.column(projection[i]);
      }

      //do this until we get rid of spark 2
      ColumnarBatch b = new ColumnarBatch(vectors);
      b.setNumRows(columnarBatch.numRows());
      return b;
    };
  }

  //  public static UnaryOperator<ColumnarBatch> generateProjectionInplace(StructType from, StructType to) {
  //    if (from.length() != to.length()) {
  //      throw new IllegalStateException(from + " and " + to + "do not have the same length");
  //    }
  //
  //    if (from.equals(to)) {
  //      return UnaryOperator.identity();
  //    }
  //
  //    List<Integer> currentPositions = Arrays.stream(from.fields()).map(f -> to.fieldIndex(f.name())).collect(Collectors.toList());
  //    List<Pair<Integer,Integer>> swaps = new ArrayList<>();
  //    for (int i = 0; i < currentPositions.size(); i++) {
  //      Integer j;
  //      while ((j = currentPositions.get(i)) != i) {
  //        currentPositions.set(i, currentPositions.get(j));
  //        currentPositions.set(j, j);
  //        swaps.add(Pair.of(i, j));
  //      }
  //    }
  //
  //    return columnarBatch -> {
  //      if (from.length() != columnarBatch.numCols()) {
  //        throw new IllegalStateException("Schemas have length " + from.length() + " but columnar batch has " + columnarBatch.numCols() + " columns");
  //      }
  //      for (Pair<Integer,Integer> s : swaps) {
  //        swap(columnarBatch, s.getLeft(), s.getRight());
  //      }
  //      return columnarBatch;
  //    };
  //  }
  //
  //  private static void swap(ColumnarBatch b, int i, int j) {
  //    ColumnVector tmp = b.columns[i];
  //    b.columns[i] = b.columns[j];
  //    b.columns[j] = tmp;
  //  }
}
