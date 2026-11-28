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

package org.apache.hudi.table;

import org.apache.hudi.common.function.SerializableSupplier;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Synchronous exact vector search over a Hudi table snapshot.
 *
 * <p>The function reads the table once when it is opened and keeps the reference rows in memory.
 * Every {@link #vectorSearch(int, RowData)} call scores those rows by cosine similarity and returns
 * the top-K matches. This is intended for small reference tables; it does not use an index.
 *
 * <p>The returned rows have the table columns in table order followed by a {@code score} column of
 * type {@code DOUBLE}, which is the row type Flink derives for {@code VECTOR_SEARCH}.
 *
 * <p>Rows whose search column is null or contains a null element are skipped. A zero-norm vector
 * scores {@code 0.0}. A vector whose dimension differs from the query dimension fails the search.
 */
@Slf4j
public class HoodieVectorSearchFunction extends VectorSearchFunction {

  private static final long serialVersionUID = 1L;

  private final SerializableSupplier<InputFormat<RowData, ?>> snapshotReader;
  private final Configuration conf;
  private final RowType tableRowType;
  private final int searchColumnIndex;
  private final boolean floatVector;

  private transient Object[][] rows;
  private transient double[][] vectors;
  private transient double[] vectorNorms;
  private transient RowData.FieldGetter[] fieldGetters;

  public HoodieVectorSearchFunction(
      SerializableSupplier<InputFormat<RowData, ?>> snapshotReader,
      Configuration conf,
      RowType tableRowType,
      int searchColumnIndex) {
    this.snapshotReader = snapshotReader;
    this.conf = conf;
    this.tableRowType = tableRowType;
    this.searchColumnIndex = searchColumnIndex;
    this.floatVector = resolveFloatVector(tableRowType, searchColumnIndex);
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    this.fieldGetters = new RowData.FieldGetter[tableRowType.getFieldCount()];
    for (int i = 0; i < fieldGetters.length; i++) {
      fieldGetters[i] = RowData.createFieldGetter(tableRowType.getTypeAt(i), i);
    }
    loadSnapshot();
  }

  @Override
  public Collection<RowData> vectorSearch(int topK, RowData queryData) throws IOException {
    if (rows == null) {
      throw new IllegalStateException("open() must be called before vectorSearch()");
    }
    if (topK <= 0 || rows.length == 0 || queryData == null || queryData.isNullAt(0)) {
      return Collections.emptyList();
    }

    ArrayData queryArray = queryData.getArray(0);
    if (queryArray == null) {
      return Collections.emptyList();
    }
    double[] query = toVector(queryArray);
    if (query == null || query.length == 0) {
      return Collections.emptyList();
    }
    double queryNorm = norm(query);

    int heapSize = Math.min(topK, rows.length);
    PriorityQueue<Hit> top =
        new PriorityQueue<>(
            heapSize,
            Comparator.comparingDouble((Hit h) -> h.score)
                .thenComparingInt(h -> -h.index));
    for (int i = 0; i < vectors.length; i++) {
      double[] vector = vectors[i];
      if (vector == null) {
        continue;
      }
      if (vector.length != query.length) {
        throw new IllegalArgumentException(
            String.format(
                "Vector dimension mismatch: the query has %d dimensions but row %d has %d",
                query.length, i, vector.length));
      }
      double score = cosine(query, queryNorm, vector, vectorNorms[i]);
      if (top.size() < heapSize) {
        top.add(new Hit(i, score));
      } else if (top.peek() != null && top.peek().score < score) {
        top.poll();
        top.add(new Hit(i, score));
      }
    }

    List<Hit> hits = new ArrayList<>(top);
    hits.sort(
        Comparator.comparingDouble((Hit h) -> h.score).reversed().thenComparingInt(h -> h.index));
    List<RowData> results = new ArrayList<>(hits.size());
    for (Hit hit : hits) {
      results.add(toResultRow(hit.index, hit.score));
    }
    return results;
  }

  @Override
  public void close() throws Exception {
    this.rows = null;
    this.vectors = null;
    this.vectorNorms = null;
    this.fieldGetters = null;
    super.close();
  }

  @SuppressWarnings("unchecked")
  private void loadSnapshot() throws IOException {
    InputFormat<RowData, InputSplit> inputFormat =
        (InputFormat<RowData, InputSplit>) snapshotReader.get();
    if (inputFormat == null) {
      this.rows = new Object[0][];
      this.vectors = new double[0][];
      this.vectorNorms = new double[0];
      return;
    }

    inputFormat.configure(conf);
    boolean richInputFormat = inputFormat instanceof RichInputFormat;
    if (richInputFormat) {
      ((RichInputFormat<RowData, InputSplit>) inputFormat).openInputFormat();
    }

    List<Object[]> loadedRows = new ArrayList<>();
    List<double[]> loadedVectors = new ArrayList<>();
    List<Double> loadedNorms = new ArrayList<>();
    TypeSerializer<RowData> serializer = InternalSerializers.create(tableRowType);
    try {
      InputSplit[] splits = inputFormat.createInputSplits(1);
      if (splits != null) {
        for (InputSplit split : splits) {
          inputFormat.open(split);
          try {
            while (!inputFormat.reachedEnd()) {
              RowData record = inputFormat.nextRecord(null);
              if (record == null) {
                break;
              }
              RowData copied = serializer.copy(record);
              Object[] values = new Object[fieldGetters.length];
              for (int i = 0; i < fieldGetters.length; i++) {
                values[i] = fieldGetters[i].getFieldOrNull(copied);
              }
              loadedRows.add(values);
              double[] vector = toVector(copied);
              loadedVectors.add(vector);
              loadedNorms.add(vector == null ? 0.0d : norm(vector));
            }
          } finally {
            inputFormat.close();
          }
        }
      }
    } finally {
      if (richInputFormat) {
        ((RichInputFormat<RowData, InputSplit>) inputFormat).closeInputFormat();
      }
    }

    this.rows = loadedRows.toArray(new Object[0][]);
    this.vectors = loadedVectors.toArray(new double[0][]);
    this.vectorNorms = new double[loadedNorms.size()];
    for (int i = 0; i < loadedNorms.size(); i++) {
      this.vectorNorms[i] = loadedNorms.get(i);
    }
    log.info("Loaded {} row(s) for exact vector search", this.rows.length);
  }

  private RowData toResultRow(int rowIndex, double score) {
    Object[] values = rows[rowIndex];
    GenericRowData row = new GenericRowData(values.length + 1);
    for (int i = 0; i < values.length; i++) {
      row.setField(i, values[i]);
    }
    row.setField(values.length, score);
    return row;
  }

  private double[] toVector(RowData row) {
    if (row.isNullAt(searchColumnIndex)) {
      return null;
    }
    ArrayData array = row.getArray(searchColumnIndex);
    return array == null ? null : toVector(array);
  }

  private double[] toVector(ArrayData array) {
    int size = array.size();
    if (size == 0) {
      return new double[0];
    }
    double[] vector = new double[size];
    for (int i = 0; i < size; i++) {
      if (array.isNullAt(i)) {
        return null;
      }
      vector[i] = floatVector ? array.getFloat(i) : array.getDouble(i);
    }
    return vector;
  }

  private static double norm(double[] vector) {
    double sum = 0.0d;
    for (double value : vector) {
      sum += value * value;
    }
    return Math.sqrt(sum);
  }

  private static double cosine(double[] left, double leftNorm, double[] right, double rightNorm) {
    if (leftNorm == 0.0d || rightNorm == 0.0d) {
      return 0.0d;
    }
    double dot = 0.0d;
    for (int i = 0; i < left.length; i++) {
      dot += left[i] * right[i];
    }
    double similarity = dot / (leftNorm * rightNorm);
    return Math.max(-1.0d, Math.min(1.0d, similarity));
  }

  private static boolean resolveFloatVector(RowType rowType, int searchColumnIndex) {
    if (searchColumnIndex < 0 || searchColumnIndex >= rowType.getFieldCount()) {
      throw new IllegalArgumentException(
          "Search column index " + searchColumnIndex + " is out of range for " + rowType);
    }
    LogicalType searchType = rowType.getTypeAt(searchColumnIndex);
    if (!(searchType instanceof ArrayType)) {
      throw new IllegalArgumentException("Search column must be an ARRAY but was " + searchType);
    }
    LogicalType elementType = ((ArrayType) searchType).getElementType();
    if (elementType.is(LogicalTypeRoot.FLOAT)) {
      return true;
    }
    if (elementType.is(LogicalTypeRoot.DOUBLE)) {
      return false;
    }
    throw new IllegalArgumentException(
        "Search column element type must be FLOAT or DOUBLE but was " + elementType);
  }

  private static final class Hit {
    private final int index;
    private final double score;

    private Hit(int index, double score) {
      this.index = index;
      this.score = score;
    }
  }
}
