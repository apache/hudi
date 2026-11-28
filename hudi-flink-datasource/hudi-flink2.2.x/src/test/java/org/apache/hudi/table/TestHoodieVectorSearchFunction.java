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

import org.apache.hudi.adapter.CollectionInputFormatAdapter;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link HoodieVectorSearchFunction}. */
class TestHoodieVectorSearchFunction {

  private static final RowType TABLE_ROW_TYPE =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("id", DataTypes.INT()),
                  DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())))
              .notNull()
              .getLogicalType();

  private static final RowType DOUBLE_ROW_TYPE =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("id", DataTypes.INT()),
                  DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.DOUBLE())))
              .notNull()
              .getLogicalType();

  private static final int SEARCH_COLUMN_INDEX = 1;

  @Test
  void testEmptyTableReturnsNoRows() throws Exception {
    HoodieVectorSearchFunction function = function();
    function.open(null);

    Collection<RowData> results = function.vectorSearch(5, query(1.0f, 0.0f));

    assertTrue(results.isEmpty());
  }

  @Test
  void testEmptyInputFormatWithoutSerializerReturnsNoRows() throws Exception {
    HoodieVectorSearchFunction function =
        new HoodieVectorSearchFunction(
            () ->
                (InputFormat<RowData, ?>)
                    new CollectionInputFormatAdapter<RowData>(
                        Collections.emptyList(), null),
            new Configuration(),
            TABLE_ROW_TYPE,
            SEARCH_COLUMN_INDEX);
    function.open(null);

    assertTrue(function.vectorSearch(5, query(1.0f, 0.0f)).isEmpty());
  }

  @Test
  void testTopKTruncatesResults() throws Exception {
    HoodieVectorSearchFunction function =
        function(
            row(1, 1.0f, 0.0f),
            row(2, 0.9f, 0.1f),
            row(3, 0.5f, 0.5f),
            row(4, 0.0f, 1.0f),
            row(5, -1.0f, 0.0f));
    function.open(null);

    List<RowData> results = new ArrayList<>(function.vectorSearch(3, query(1.0f, 0.0f)));

    assertEquals(3, results.size());
    assertEquals(1, results.get(0).getInt(0));
    assertEquals(2, results.get(1).getInt(0));
    assertEquals(3, results.get(2).getInt(0));
    assertEquals(1.0d, results.get(0).getDouble(2), 1e-6);
    assertEquals(3, results.get(0).getArity());
    assertTrue(results.get(0).getDouble(2) > results.get(1).getDouble(2));
    assertTrue(results.get(1).getDouble(2) > results.get(2).getDouble(2));
  }

  @Test
  void testTopKLargerThanRowCountReturnsAllRows() throws Exception {
    HoodieVectorSearchFunction function =
        function(row(1, 1.0f, 0.0f), row(2, 0.0f, 1.0f));
    function.open(null);

    List<RowData> results = new ArrayList<>(function.vectorSearch(10, query(1.0f, 0.0f)));

    assertEquals(2, results.size());
    assertEquals(1, results.get(0).getInt(0));
    assertEquals(2, results.get(1).getInt(0));
  }

  @Test
  void testDoubleVectorSearch() throws Exception {
    HoodieVectorSearchFunction function =
        function(
            DOUBLE_ROW_TYPE,
            doubleRow(1, 1.0d, 0.0d),
            doubleRow(2, 0.0d, 1.0d));
    function.open(null);

    List<RowData> results =
        new ArrayList<>(function.vectorSearch(2, doubleQuery(1.0d, 0.0d)));

    assertEquals(2, results.size());
    assertEquals(1, results.get(0).getInt(0));
    assertEquals(1.0d, results.get(0).getDouble(2), 1e-6);
  }

  @Test
  void testNullVectorsAreSkipped() throws Exception {
    HoodieVectorSearchFunction function =
        function(row(1, 1.0f, 0.0f), nullRow(2), row(3, 0.0f, 1.0f));
    function.open(null);

    List<RowData> results = new ArrayList<>(function.vectorSearch(5, query(1.0f, 0.0f)));

    assertEquals(2, results.size());
    assertEquals(1, results.get(0).getInt(0));
    assertEquals(3, results.get(1).getInt(0));
  }

  @Test
  void testZeroVectorScoresZero() throws Exception {
    HoodieVectorSearchFunction function = function(row(1, 0.0f, 0.0f), row(2, 1.0f, 0.0f));
    function.open(null);

    List<RowData> results = new ArrayList<>(function.vectorSearch(2, query(1.0f, 0.0f)));

    assertEquals(2, results.size());
    assertEquals(2, results.get(0).getInt(0));
    assertEquals(0.0d, results.get(1).getDouble(2), 1e-6);
  }

  @Test
  void testDimensionMismatchFails() throws Exception {
    HoodieVectorSearchFunction function = function(row(1, 1.0f, 0.0f));
    function.open(null);

    assertThrows(
        IllegalArgumentException.class, () -> function.vectorSearch(2, query(1.0f, 0.0f, 0.0f)));
  }

  private static HoodieVectorSearchFunction function(RowData... rows) {
    return function(TABLE_ROW_TYPE, rows);
  }

  private static HoodieVectorSearchFunction function(RowType rowType, RowData... rows) {
    List<RowData> data = Arrays.asList(rows);
    return new HoodieVectorSearchFunction(
        () ->
            (InputFormat<RowData, ?>)
                new CollectionInputFormatAdapter<RowData>(
                    data, InternalSerializers.create(rowType)),
        new Configuration(),
        rowType,
        SEARCH_COLUMN_INDEX);
  }

  private static RowData row(int id, float... vector) {
    return GenericRowData.of(id, new GenericArrayData(vector));
  }

  private static RowData doubleRow(int id, double... vector) {
    return GenericRowData.of(id, new GenericArrayData(vector));
  }

  private static RowData nullRow(int id) {
    return GenericRowData.of(id, null);
  }

  private static RowData query(float... vector) {
    return GenericRowData.of(new GenericArrayData(vector));
  }

  private static RowData doubleQuery(double... vector) {
    return GenericRowData.of(new GenericArrayData(vector));
  }
}
