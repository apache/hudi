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

import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnarBatchModifier {

  //This only works  there is an index x such that
  //for all i, j where i < j <= x:  from.fieldIndex(to[i]) < from.fieldIndex(to[j])
  //and for all i, j  where x < i < j: from.fieldIndex(to[i]) < from.fieldIndex(to[j])
  public static ColumnarBatch projectBatch(ColumnarBatch b, StructType from, StructType to) {
    if (from.length() != to.length()) {
      throw new IllegalStateException(from + " and " + to + "do not have the same length");
    } else if (from.length() != b.numCols()) {
      throw new IllegalStateException("Schemas have length " + from.length() + " but columnar batch has " + b.numCols() + " columns");
    }
    List<Integer> currentPositions = Arrays.stream(from.fields()).map(f -> to.fieldIndex(f.name())).collect(Collectors.toList());
    for (int i = 0; i < currentPositions.size(); i++) {
      while (i != currentPositions.get(i)) {
        swap(b, currentPositions, i, currentPositions.get(i));
      }
    }
    return b;
  }

  private static void swap(ColumnarBatch b, List<Integer> correctPositions, int i, int j) {
    Integer tmpi = correctPositions.get(i);
    correctPositions.set(i, correctPositions.get(j));
    correctPositions.set(j, tmpi);
    ColumnVector tmp = b.columns[i];
    b.columns[i] = b.columns[j];
    b.columns[j] = tmp;
  }
}
