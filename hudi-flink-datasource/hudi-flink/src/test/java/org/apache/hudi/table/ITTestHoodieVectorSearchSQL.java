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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * SQL-level integration test for {@code VECTOR_SEARCH} on a Hudi table.
 *
 * <p>The test is skipped on Flink versions that do not ship the vector search API.
 */
@ExtendWith(FlinkMiniCluster.class)
class ITTestHoodieVectorSearchSQL {

  private static final DataType TABLE_DATA_TYPE =
      DataTypes.ROW(
              DataTypes.FIELD("uuid", DataTypes.STRING().notNull()),
              DataTypes.FIELD("ts", DataTypes.BIGINT()),
              DataTypes.FIELD("partition", DataTypes.STRING()),
              DataTypes.FIELD("id", DataTypes.INT()),
              DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())))
          .notNull();

  @TempDir File tempFile;

  @ParameterizedTest
  @ValueSource(strings = {"COPY_ON_WRITE", "MERGE_ON_READ"})
  void testVectorSearchSQL(String tableType) throws Exception {
    assumeTrue(vectorSearchApiAvailable(), "Flink 2.2 vector search API is not available");

    String path = tempFile.toURI().toString();
    org.apache.flink.configuration.Configuration conf =
        TestConfigurations.getDefaultConf(path, TABLE_DATA_TYPE);
    conf.set(FlinkOptions.TABLE_TYPE, tableType);
    TestData.writeData(
        Arrays.asList(
            row(RowKind.INSERT, "id1", 1L, 1, 1.0f, 0.0f),
            row(RowKind.INSERT, "id2", 1L, 2, 0.0f, 1.0f),
            row(RowKind.INSERT, "id3", 1L, 3, 0.9f, 0.1f)),
        conf);
    TestData.writeData(
        Arrays.asList(
            row(RowKind.UPDATE_BEFORE, "id2", 2L, 2, 0.0f, 1.0f),
            row(RowKind.UPDATE_AFTER, "id2", 3L, 2, 0.99f, 0.01f),
            row(RowKind.DELETE, "id3", 4L, 3, 0.9f, 0.1f)),
        conf);

    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    tableEnv.executeSql(
        String.format(
            "CREATE TABLE docs (\n"
                + "  uuid STRING,\n"
                + "  ts BIGINT,\n"
                + "  `partition` STRING,\n"
                + "  id INT,\n"
                + "  embedding ARRAY<FLOAT>,\n"
                + "  PRIMARY KEY (uuid) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector' = 'hudi',\n"
                + "  'path' = '%s',\n"
                + "  'table.type' = '%s',\n"
                + "  'read.streaming.enabled' = 'false'\n"
                + ")",
            path, tableType));

    TableResult result =
        tableEnv.executeSql(
            "SELECT id, score\n"
                + "FROM TABLE(\n"
                + "  VECTOR_SEARCH(\n"
                + "    TABLE docs,\n"
                + "    DESCRIPTOR(embedding),\n"
                + "    ARRAY[CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)],\n"
                + "    10\n"
                + "  )\n"
                + ")");

    List<Row> rows = CollectionUtil.iteratorToList(result.collect());
    assertEquals(2, rows.size());
    Set<Integer> ids = new HashSet<>();
    rows.forEach(row -> ids.add((Integer) row.getField(0)));
    assertEquals(new HashSet<>(Arrays.asList(1, 2)), ids);
    Row first =
        rows.stream()
            .filter(row -> Integer.valueOf(1).equals(row.getField(0)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("row 1 is missing"));
    Row updated =
        rows.stream()
            .filter(row -> Integer.valueOf(2).equals(row.getField(0)))
            .findFirst()
            .orElseThrow(() -> new AssertionError("row 2 is missing"));
    assertTrue((double) first.getField(1) > (double) updated.getField(1));
    assertTrue((double) updated.getField(1) > 0.9d);
  }

  private static boolean vectorSearchApiAvailable() {
    try {
      Class.forName("org.apache.flink.table.connector.source.VectorSearchTableSource");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static RowData row(RowKind kind, String uuid, long ts, int id, float... embedding) {
    GenericRowData row =
        GenericRowData.of(
            StringData.fromString(uuid),
            ts,
            StringData.fromString("par1"),
            id,
            new GenericArrayData(embedding));
    row.setRowKind(kind);
    return row;
  }
}
