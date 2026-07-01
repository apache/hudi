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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.sink.clustering.ClusteringOperator;
import org.apache.hudi.sink.clustering.ClusteringPlanSourceFunction;
import org.apache.hudi.sink.clustering.FlinkClusteringConfig;
import org.apache.hudi.sink.compact.CompactOperator;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanSourceFunction;
import org.apache.hudi.sink.compact.FlinkCompactionConfig;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.sink.utils.FlinkTransformationUtils.setManagedMemoryWeight;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for Flink vector column read/write support.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVectorDataSource {

  @TempDir
  Path tempDir;

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testVectorRoundTrip(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("round_trip_" + tableType.name()).toUri().toString();
    createVectorTable(
        tableEnv,
        "vector_table",
        tablePath,
        tableType,
        "embedding:4,features:3,codes:4,nullable_embedding:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table VALUES "
            + "('id1', "
            + " ARRAY[CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT), CAST(3.0 AS FLOAT), CAST(4.0 AS FLOAT)], "
            + " ARRAY[CAST(0.1 AS DOUBLE), CAST(0.2 AS DOUBLE), CAST(0.3 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT), CAST(3 AS TINYINT), CAST(4 AS TINYINT)], "
            + " ARRAY[CAST(9.0 AS FLOAT), CAST(9.5 AS FLOAT)], "
            + " 'label1', ARRAY['red', 'blue'], 1000), "
            + "('id2', "
            + " ARRAY[CAST(-1.0 AS FLOAT), CAST(-2.0 AS FLOAT), CAST(-3.0 AS FLOAT), CAST(-4.0 AS FLOAT)], "
            + " ARRAY[CAST(1.1 AS DOUBLE), CAST(1.2 AS DOUBLE), CAST(1.3 AS DOUBLE)], "
            + " ARRAY[CAST(-1 AS TINYINT), CAST(-2 AS TINYINT), CAST(-3 AS TINYINT), CAST(-4 AS TINYINT)], "
            + " CAST(NULL AS ARRAY<FLOAT>), "
            + " 'label2', ARRAY['green'], 2000)");

    assertStoredVectorSchema(tablePath, "embedding", 4, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "features", 3, HoodieSchema.Vector.VectorElementType.DOUBLE);
    assertStoredVectorSchema(tablePath, "codes", 4, HoodieSchema.Vector.VectorElementType.INT8);
    assertStoredVectorSchema(tablePath, "nullable_embedding", 2, HoodieSchema.Vector.VectorElementType.FLOAT);

    List<Row> rows = collect(tableEnv,
        "SELECT id, embedding, features, codes, nullable_embedding, label, tags, ts FROM vector_table ORDER BY id");
    assertEquals(2, rows.size());

    Row first = rows.get(0);
    assertEquals("id1", first.getField(0));
    assertFloatArray(first.getField(1), new float[] {1.0f, 2.0f, 3.0f, 4.0f});
    assertDoubleArray(first.getField(2), new double[] {0.1d, 0.2d, 0.3d});
    assertByteArray(first.getField(3), new byte[] {1, 2, 3, 4});
    assertFloatArray(first.getField(4), new float[] {9.0f, 9.5f});
    assertEquals("label1", first.getField(5));
    assertObjectArray(first.getField(6), new Object[] {"red", "blue"});
    assertEquals(1000L, first.getField(7));

    Row second = rows.get(1);
    assertEquals("id2", second.getField(0));
    assertFloatArray(second.getField(1), new float[] {-1.0f, -2.0f, -3.0f, -4.0f});
    assertDoubleArray(second.getField(2), new double[] {1.1d, 1.2d, 1.3d});
    assertByteArray(second.getField(3), new byte[] {-1, -2, -3, -4});
    assertNull(second.getField(4));
    assertEquals("label2", second.getField(5));
    assertObjectArray(second.getField(6), new Object[] {"green"});
    assertEquals(2000L, second.getField(7));

    List<Row> nonVectorProjection = collect(tableEnv, "SELECT id, label, ts FROM vector_table ORDER BY id");
    assertEquals(Row.of("id1", "label1", 1000L), nonVectorProjection.get(0));
    assertEquals(Row.of("id2", "label2", 2000L), nonVectorProjection.get(1));

    List<Row> vectorProjection = collect(tableEnv, "SELECT id, embedding FROM vector_table WHERE id = 'id2'");
    assertEquals(1, vectorProjection.size());
    assertFloatArray(vectorProjection.get(0).getField(1), new float[] {-1.0f, -2.0f, -3.0f, -4.0f});
  }

  @Test
  public void testColumnProjectionContainsVectorColumn() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("projection_with_vector").toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, HoodieTableType.COPY_ON_WRITE, "embedding:2,features:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, label, tags, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.5 AS FLOAT)], "
            + " ARRAY[CAST(10.0 AS DOUBLE), CAST(10.5 AS DOUBLE)], 'label1', ARRAY['red', 'blue'], 1), "
            + "('id2', ARRAY[CAST(2.0 AS FLOAT), CAST(2.5 AS FLOAT)], "
            + " ARRAY[CAST(20.0 AS DOUBLE), CAST(20.5 AS DOUBLE)], 'label2', ARRAY['green'], 2)");

    List<Row> rows = collect(tableEnv,
        "SELECT label, embedding, tags, features FROM vector_table WHERE id = 'id1'");
    assertEquals(1, rows.size());
    Row row = rows.get(0);
    assertEquals("label1", row.getField(0));
    assertFloatArray(row.getField(1), new float[] {1.0f, 1.5f});
    assertObjectArray(row.getField(2), new Object[] {"red", "blue"});
    assertDoubleArray(row.getField(3), new double[] {10.0d, 10.5d});
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testVectorAppendWriteRoundTrip(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("append_write_" + tableType.name()).toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, tableType, "embedding:3,features:2,codes:3", "insert");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, codes, label, ts) VALUES "
            + "('id1', "
            + " ARRAY[CAST(1.0 AS FLOAT), CAST(1.5 AS FLOAT), CAST(2.0 AS FLOAT)], "
            + " ARRAY[CAST(0.1 AS DOUBLE), CAST(0.2 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT), CAST(3 AS TINYINT)], "
            + " 'append1', 1000), "
            + "('id2', "
            + " ARRAY[CAST(3.0 AS FLOAT), CAST(3.5 AS FLOAT), CAST(4.0 AS FLOAT)], "
            + " ARRAY[CAST(1.1 AS DOUBLE), CAST(1.2 AS DOUBLE)], "
            + " ARRAY[CAST(-1 AS TINYINT), CAST(-2 AS TINYINT), CAST(-3 AS TINYINT)], "
            + " 'append2', 2000)");

    assertStoredVectorSchema(tablePath, "embedding", 3, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "features", 2, HoodieSchema.Vector.VectorElementType.DOUBLE);
    assertStoredVectorSchema(tablePath, "codes", 3, HoodieSchema.Vector.VectorElementType.INT8);

    List<Row> rows = collect(tableEnv, "SELECT id, embedding, features, codes, label, ts FROM vector_table ORDER BY id");
    assertEquals(2, rows.size());

    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {1.0f, 1.5f, 2.0f});
    assertDoubleArray(rows.get(0).getField(2), new double[] {0.1d, 0.2d});
    assertByteArray(rows.get(0).getField(3), new byte[] {1, 2, 3});
    assertEquals("append1", rows.get(0).getField(4));
    assertEquals(1000L, rows.get(0).getField(5));

    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {3.0f, 3.5f, 4.0f});
    assertDoubleArray(rows.get(1).getField(2), new double[] {1.1d, 1.2d});
    assertByteArray(rows.get(1).getField(3), new byte[] {-1, -2, -3});
    assertEquals("append2", rows.get(1).getField(4));
    assertEquals(2000L, rows.get(1).getField(5));
  }

  @Test
  public void testVectorWithNonVectorArrayColumn() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("vector_with_array").toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, HoodieTableType.COPY_ON_WRITE, "embedding:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, tags, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.1 AS FLOAT)], 'array1', ARRAY['red', 'blue'], 1), "
            + "('id2', ARRAY[CAST(2.0 AS FLOAT), CAST(2.2 AS FLOAT)], 'array2', ARRAY['green'], 2)");

    assertStoredVectorSchema(tablePath, "embedding", 2, HoodieSchema.Vector.VectorElementType.FLOAT);

    List<Row> rows = collect(tableEnv, "SELECT id, embedding, tags FROM vector_table ORDER BY id");
    assertEquals(2, rows.size());
    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {1.0f, 1.1f});
    assertObjectArray(rows.get(0).getField(2), new Object[] {"red", "blue"});
    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {2.0f, 2.2f});
    assertObjectArray(rows.get(1).getField(2), new Object[] {"green"});
  }

  @Test
  public void testVectorColumnsWithDifferentDimensions() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("different_dimensions").toUri().toString();
    createDifferentDimensionVectorTable(tableEnv, tablePath);

    execInsertSql(tableEnv,
        "INSERT INTO dimension_vector_table VALUES "
            + "('id1', "
            + floatArrayLiteral(2) + ", "
            + floatArrayLiteral(128) + ", "
            + floatArrayLiteral(2048) + ", "
            + "1)");

    assertStoredVectorSchema(tablePath, "vec2", 2, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "vec128", 128, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "vec2048", 2048, HoodieSchema.Vector.VectorElementType.FLOAT);

    List<Row> rows = collect(tableEnv, "SELECT vec2, vec128, vec2048 FROM dimension_vector_table WHERE id = 'id1'");
    assertEquals(1, rows.size());
    assertFloatArray(rows.get(0).getField(0), new float[] {0.0f, 1.0f});
    assertFloatArrayLengthAndEndpoints(rows.get(0).getField(1), 128, 0.0f, 127.0f);
    assertFloatArrayLengthAndEndpoints(rows.get(0).getField(2), 2048, 0.0f, 2047.0f);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testVectorUpsert(HoodieTableType tableType) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("upsert_" + tableType.name()).toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, tableType, "embedding:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.1 AS FLOAT)], 'old1', 1), "
            + "('id2', ARRAY[CAST(2.0 AS FLOAT), CAST(2.2 AS FLOAT)], 'old2', 1)");
    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, ts) VALUES "
            + "('id1', ARRAY[CAST(9.0 AS FLOAT), CAST(9.9 AS FLOAT)], 'new1', 10), "
            + "('id3', ARRAY[CAST(3.0 AS FLOAT), CAST(3.3 AS FLOAT)], 'new3', 1)");

    List<Row> rows = collect(tableEnv, "SELECT id, embedding, label, ts FROM vector_table ORDER BY id");
    assertEquals(3, rows.size());

    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {9.0f, 9.9f});
    assertEquals("new1", rows.get(0).getField(2));
    assertEquals(10L, rows.get(0).getField(3));

    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {2.0f, 2.2f});
    assertEquals("old2", rows.get(1).getField(2));

    assertEquals("id3", rows.get(2).getField(0));
    assertFloatArray(rows.get(2).getField(1), new float[] {3.0f, 3.3f});
    assertEquals("new3", rows.get(2).getField(2));
  }

  @Test
  public void testVectorCopyOnWriteClustering() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("clustering").toUri().toString();
    String vectorColsConf = "embedding:2,features:2,codes:2";
    createVectorTable(tableEnv, "vector_table", tablePath, HoodieTableType.COPY_ON_WRITE, vectorColsConf, "insert");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, codes, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.1 AS FLOAT)], ARRAY[CAST(10.0 AS DOUBLE), CAST(10.1 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT)], 'cluster1', 1), "
            + "('id2', ARRAY[CAST(2.0 AS FLOAT), CAST(2.2 AS FLOAT)], ARRAY[CAST(20.0 AS DOUBLE), CAST(20.2 AS DOUBLE)], "
            + " ARRAY[CAST(3 AS TINYINT), CAST(4 AS TINYINT)], 'cluster2', 2), "
            + "('id3', ARRAY[CAST(3.0 AS FLOAT), CAST(3.3 AS FLOAT)], ARRAY[CAST(30.0 AS DOUBLE), CAST(30.3 AS DOUBLE)], "
            + " ARRAY[CAST(5 AS TINYINT), CAST(6 AS TINYINT)], 'cluster3', 3)");

    runClustering(tablePath, vectorColsConf);

    assertStoredVectorSchema(tablePath, "embedding", 2, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "features", 2, HoodieSchema.Vector.VectorElementType.DOUBLE);
    assertStoredVectorSchema(tablePath, "codes", 2, HoodieSchema.Vector.VectorElementType.INT8);
    assertClusteredVectorRows(tableEnv);
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro", "parquet"})
  public void testVectorMergeOnReadCompaction(String logBlockFormat) throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("compaction_" + logBlockFormat).toUri().toString();
    String vectorColsConf = "embedding:2,features:2,codes:2";
    createVectorTable(
        tableEnv,
        "vector_table",
        tablePath,
        HoodieTableType.MERGE_ON_READ,
        vectorColsConf,
        null,
        Collections.singletonMap(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logBlockFormat));

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, codes, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.1 AS FLOAT)], ARRAY[CAST(10.0 AS DOUBLE), CAST(10.1 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT)], 'old1', 1), "
            + "('id2', ARRAY[CAST(2.0 AS FLOAT), CAST(2.2 AS FLOAT)], ARRAY[CAST(20.0 AS DOUBLE), CAST(20.2 AS DOUBLE)], "
            + " ARRAY[CAST(3 AS TINYINT), CAST(4 AS TINYINT)], 'old2', 2)");
    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, codes, label, ts) VALUES "
            + "('id1', ARRAY[CAST(9.0 AS FLOAT), CAST(9.9 AS FLOAT)], ARRAY[CAST(90.0 AS DOUBLE), CAST(90.9 AS DOUBLE)], "
            + " ARRAY[CAST(9 AS TINYINT), CAST(8 AS TINYINT)], 'new1', 10), "
            + "('id3', ARRAY[CAST(3.0 AS FLOAT), CAST(3.3 AS FLOAT)], ARRAY[CAST(30.0 AS DOUBLE), CAST(30.3 AS DOUBLE)], "
            + " ARRAY[CAST(5 AS TINYINT), CAST(6 AS TINYINT)], 'new3', 3)");

    runCompaction(tablePath, vectorColsConf);

    assertStoredVectorSchema(tablePath, "embedding", 2, HoodieSchema.Vector.VectorElementType.FLOAT);
    assertStoredVectorSchema(tablePath, "features", 2, HoodieSchema.Vector.VectorElementType.DOUBLE);
    assertStoredVectorSchema(tablePath, "codes", 2, HoodieSchema.Vector.VectorElementType.INT8);
    assertCompactedVectorRows(tableEnv);
  }

  @Test
  public void testDimensionMismatchOnWrite() {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tablePath = tempDir.resolve("dimension_mismatch").toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, HoodieTableType.COPY_ON_WRITE, "embedding:3");

    Exception exception = assertThrows(Exception.class, () -> execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)], 'bad', 1)"));
    assertTrue(containsMessage(exception, "dimension mismatch"),
        "Expected dimension mismatch in exception chain, got: " + exception.getMessage());
  }

  @Test
  public void testParquetFooterContainsVectorMetadata() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    Path tableDir = tempDir.resolve("vector_footer");
    String tablePath = tableDir.toUri().toString();
    createVectorTable(tableEnv, "vector_table", tablePath, HoodieTableType.COPY_ON_WRITE, "embedding:2,features:2,codes:2");

    execInsertSql(tableEnv,
        "INSERT INTO vector_table(id, embedding, features, codes, label, ts) VALUES "
            + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(1.5 AS FLOAT)], ARRAY[CAST(10.0 AS DOUBLE), CAST(10.5 AS DOUBLE)], "
            + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT)], 'footer', 1)");

    Path parquetFile = findFirstParquetFile(tableDir);
    try (ParquetFileReader reader = ParquetFileReader.open(
        new org.apache.hadoop.conf.Configuration(), new org.apache.hadoop.fs.Path(parquetFile.toUri()))) {
      ParquetMetadata metadata = reader.getFooter();
      assertEquals("embedding:VECTOR(2),features:VECTOR(2, DOUBLE),codes:VECTOR(2, INT8)",
          metadata.getFileMetaData().getKeyValueMetaData().get(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY));

      PrimitiveType embeddingType = metadata.getFileMetaData().getSchema().getType("embedding").asPrimitiveType();
      assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, embeddingType.getPrimitiveTypeName());
      assertEquals(8, embeddingType.getTypeLength());
      PrimitiveType featuresType = metadata.getFileMetaData().getSchema().getType("features").asPrimitiveType();
      assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, featuresType.getPrimitiveTypeName());
      assertEquals(16, featuresType.getTypeLength());
      PrimitiveType codesType = metadata.getFileMetaData().getSchema().getType("codes").asPrimitiveType();
      assertEquals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, codesType.getPrimitiveTypeName());
      assertEquals(2, codesType.getTypeLength());
    }
  }

  @ParameterizedTest
  @MethodSource("invalidVectorColumnConfigs")
  public void testInvalidVectorColumns(String vectorColumns, String expectedMessage) {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    String tableName = "invalid_vector_" + Integer.toHexString(vectorColumns.hashCode()).replace('-', '_');
    String tablePath = tempDir.resolve(tableName).toUri().toString();

    Exception exception = assertThrows(Exception.class, () -> {
      createVectorTable(tableEnv, tableName, tablePath, HoodieTableType.COPY_ON_WRITE, vectorColumns);
      execInsertSql(tableEnv,
          "INSERT INTO " + tableName + "(id, embedding, features, codes, label, tags, ts) VALUES "
              + "('id1', ARRAY[CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)], "
              + " ARRAY[CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)], "
              + " ARRAY[CAST(1 AS TINYINT), CAST(2 AS TINYINT)], "
              + " 'label1', ARRAY['tag1'], 1)");
    });
    assertTrue(containsMessage(exception, expectedMessage),
        "Expected exception chain to contain '" + expectedMessage + "', got: " + exception.getMessage());
  }

  private static void createVectorTable(
      TableEnvironment tableEnv,
      String tableName,
      String tablePath,
      HoodieTableType tableType,
      String vectorColumns) {
    createVectorTable(tableEnv, tableName, tablePath, tableType, vectorColumns, null);
  }

  private static void createVectorTable(
      TableEnvironment tableEnv,
      String tableName,
      String tablePath,
      HoodieTableType tableType,
      String vectorColumns,
      String writeOperation) {
    createVectorTable(tableEnv, tableName, tablePath, tableType, vectorColumns, writeOperation, Collections.emptyMap());
  }

  private static void createVectorTable(
      TableEnvironment tableEnv,
      String tableName,
      String tablePath,
      HoodieTableType tableType,
      String vectorColumns,
      String writeOperation,
      Map<String, String> extraOptions) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("connector", "hudi");
    options.put("path", tablePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.name());
    options.put(FlinkOptions.ORDERING_FIELDS.key(), "ts");
    options.put(FlinkOptions.VECTOR_COLUMNS.key(), vectorColumns);
    options.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), "false");
    options.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), "false");
    if (writeOperation != null) {
      options.put(FlinkOptions.OPERATION.key(), writeOperation);
    }
    options.put(FlinkOptions.WRITE_TASKS.key(), "1");
    options.put(FlinkOptions.READ_TASKS.key(), "1");
    options.putAll(extraOptions);

    tableEnv.executeSql(
        "CREATE TABLE " + tableName + " ("
            + " id STRING,"
            + " embedding ARRAY<FLOAT>,"
            + " features ARRAY<DOUBLE>,"
            + " codes ARRAY<TINYINT>,"
            + " nullable_embedding ARRAY<FLOAT>,"
            + " label STRING,"
            + " tags ARRAY<STRING>,"
            + " ts BIGINT,"
            + " PRIMARY KEY (id) NOT ENFORCED"
            + ") WITH ("
            + formatOptions(options)
            + ")");
  }

  private static void createDifferentDimensionVectorTable(TableEnvironment tableEnv, String tablePath) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("connector", "hudi");
    options.put("path", tablePath);
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.ORDERING_FIELDS.key(), "ts");
    options.put(FlinkOptions.VECTOR_COLUMNS.key(), "vec2:2,vec128:128,vec2048:2048");
    options.put(FlinkOptions.WRITE_TASKS.key(), "1");
    options.put(FlinkOptions.READ_TASKS.key(), "1");

    tableEnv.executeSql(
        "CREATE TABLE dimension_vector_table ("
            + " id STRING,"
            + " vec2 ARRAY<FLOAT>,"
            + " vec128 ARRAY<FLOAT>,"
            + " vec2048 ARRAY<FLOAT>,"
            + " ts BIGINT,"
            + " PRIMARY KEY (id) NOT ENFORCED"
            + ") WITH ("
            + formatOptions(options)
            + ")");
  }

  private static void runClustering(String tablePath, String vectorColsConf) throws Exception {
    FlinkClusteringConfig cfg = new FlinkClusteringConfig();
    cfg.path = tablePath;
    cfg.targetPartitions = 1;
    cfg.sortMemory = 128;
    Configuration conf = FlinkClusteringConfig.toFlinkConfig(cfg);

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    conf.set(FlinkOptions.VECTOR_COLUMNS, vectorColsConf);
    CompactionUtil.setPartitionField(conf, metaClient);
    CompactionUtil.setAvroSchema(conf, metaClient);

    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();
      Option<String> clusteringInstantTime = writeClient.scheduleClustering(Option.empty());
      assertTrue(clusteringInstantTime.isPresent(), "The clustering plan should be scheduled");

      table.getMetaClient().reloadActiveTimeline();
      HoodieTimeline timeline = table.getActiveTimeline().filterPendingClusteringTimeline()
          .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlanOption =
          ClusteringUtils.getClusteringPlan(table.getMetaClient(), timeline.lastInstant().get());
      HoodieClusteringPlan clusteringPlan = clusteringPlanOption.get().getRight();
      assertTrue(clusteringPlan.getInputGroups().size() > 0, "The clustering plan should contain input groups");

      HoodieInstant instant = INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringInstantTime.get());
      table.getActiveTimeline().transitionClusterRequestedToInflight(instant, Option.empty());

      HoodieSchema tableSchema = StreamerUtil.getTableSchema(table.getMetaClient(), false);
      DataType rowDataType = HoodieSchemaConverter.convertToDataType(tableSchema);
      RowType rowType = (RowType) rowDataType.getLogicalType();

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      DataStream<ClusteringCommitEvent> dataStream = env.addSource(new ClusteringPlanSourceFunction(clusteringInstantTime.get(), clusteringPlan, conf))
          .name("clustering_source")
          .uid("uid_vector_clustering_source")
          .rebalance()
          .transform("clustering_task",
              TypeInformation.of(ClusteringCommitEvent.class),
              new ClusteringOperator(conf, rowType))
          .setParallelism(clusteringPlan.getInputGroups().size());

      setManagedMemoryWeight(dataStream.getTransformation(),
          conf.get(FlinkOptions.WRITE_SORT_MEMORY) * 1024L * 1024L);

      dataStream
          .addSink(new ClusteringCommitSink(conf))
          .name("clustering_commit")
          .uid("uid_vector_clustering_commit")
          .setParallelism(1);

      env.execute("flink_hudi_vector_clustering");
      assertTrue(table.getMetaClient().reloadActiveTimeline().filterCompletedInstants().containsInstant(instant.requestedTime()));
    }
  }

  private static void runCompaction(String tablePath, String vectorColsConf) throws Exception {
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    cfg.path = tablePath;
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    conf.set(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());
    conf.set(FlinkOptions.RECORD_KEY_FIELD, metaClient.getTableConfig().getRecordKeyFieldProp());
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.VECTOR_COLUMNS, vectorColsConf);
    CompactionUtil.setPartitionField(conf, metaClient);
    CompactionUtil.setAvroSchema(conf, metaClient);
    CompactionUtil.inferChangelogMode(conf, metaClient);

    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(conf)) {
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();
      Option<String> compactionInstantTime = writeClient.scheduleCompaction(Option.empty());
      assertTrue(compactionInstantTime.isPresent(), "The compaction plan should be scheduled");

      HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
          table.getMetaClient(), compactionInstantTime.get());
      assertTrue(compactionPlan.getOperations().size() > 0, "The compaction plan should contain operations");

      HoodieInstant instant = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstantTime.get());
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.addSource(new CompactionPlanSourceFunction(Collections.singletonList(Pair.of(compactionInstantTime.get(), compactionPlan)), conf))
          .name("compaction_source")
          .uid("uid_vector_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(compactionPlan.getOperations().size())
          .addSink(new CompactionCommitSink(conf))
          .name("compaction_commit")
          .uid("uid_vector_compaction_commit")
          .setParallelism(1);

      env.execute("flink_hudi_vector_compaction");
      assertTrue(table.getMetaClient().reloadActiveTimeline().filterCompletedInstants().containsInstant(instant.requestedTime()));
    }
  }

  private static Stream<Object[]> invalidVectorColumnConfigs() {
    return Stream.of(
        new Object[] {"missing:2", "does not exist"},
        new Object[] {"label:2", "must be declared as ARRAY<FLOAT>, ARRAY<DOUBLE>, or ARRAY<TINYINT>"},
        new Object[] {"tags:2", "must use ARRAY<FLOAT>, ARRAY<DOUBLE>, or ARRAY<TINYINT>"},
        new Object[] {"embedding:0", "dimension must be positive"},
        new Object[] {"embedding:2,embedding:3", "Duplicate VECTOR column descriptor"});
  }

  private static String formatOptions(Map<String, String> options) {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (builder.length() > 0) {
        builder.append(",");
      }
      builder.append(" '").append(entry.getKey()).append("' = '").append(entry.getValue()).append("'");
    }
    return builder.toString();
  }

  private static void assertCompactedVectorRows(TableEnvironment tableEnv) {
    List<Row> rows = collect(tableEnv, "SELECT id, embedding, features, codes, label, ts FROM vector_table ORDER BY id");
    assertEquals(3, rows.size());

    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {9.0f, 9.9f});
    assertDoubleArray(rows.get(0).getField(2), new double[] {90.0d, 90.9d});
    assertByteArray(rows.get(0).getField(3), new byte[] {9, 8});
    assertEquals("new1", rows.get(0).getField(4));
    assertEquals(10L, rows.get(0).getField(5));

    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {2.0f, 2.2f});
    assertDoubleArray(rows.get(1).getField(2), new double[] {20.0d, 20.2d});
    assertByteArray(rows.get(1).getField(3), new byte[] {3, 4});
    assertEquals("old2", rows.get(1).getField(4));

    assertEquals("id3", rows.get(2).getField(0));
    assertFloatArray(rows.get(2).getField(1), new float[] {3.0f, 3.3f});
    assertDoubleArray(rows.get(2).getField(2), new double[] {30.0d, 30.3d});
    assertByteArray(rows.get(2).getField(3), new byte[] {5, 6});
    assertEquals("new3", rows.get(2).getField(4));
  }

  private static void assertClusteredVectorRows(TableEnvironment tableEnv) {
    List<Row> rows = collect(tableEnv, "SELECT id, embedding, features, codes, label, ts FROM vector_table ORDER BY id");
    assertEquals(3, rows.size());

    assertEquals("id1", rows.get(0).getField(0));
    assertFloatArray(rows.get(0).getField(1), new float[] {1.0f, 1.1f});
    assertDoubleArray(rows.get(0).getField(2), new double[] {10.0d, 10.1d});
    assertByteArray(rows.get(0).getField(3), new byte[] {1, 2});
    assertEquals("cluster1", rows.get(0).getField(4));

    assertEquals("id2", rows.get(1).getField(0));
    assertFloatArray(rows.get(1).getField(1), new float[] {2.0f, 2.2f});
    assertDoubleArray(rows.get(1).getField(2), new double[] {20.0d, 20.2d});
    assertByteArray(rows.get(1).getField(3), new byte[] {3, 4});
    assertEquals("cluster2", rows.get(1).getField(4));

    assertEquals("id3", rows.get(2).getField(0));
    assertFloatArray(rows.get(2).getField(1), new float[] {3.0f, 3.3f});
    assertDoubleArray(rows.get(2).getField(2), new double[] {30.0d, 30.3d});
    assertByteArray(rows.get(2).getField(3), new byte[] {5, 6});
    assertEquals("cluster3", rows.get(2).getField(4));
  }

  private static void execInsertSql(TableEnvironment tableEnv, String insert) throws ExecutionException, InterruptedException {
    TableResult tableResult = tableEnv.executeSql(insert);
    tableResult.await();
  }

  private static List<Row> collect(TableEnvironment tableEnv, String query) {
    return CollectionUtil.iteratorToList(tableEnv.executeSql(query).collect());
  }

  private static Path findFirstParquetFile(Path tableDir) throws IOException {
    try (Stream<Path> paths = Files.walk(tableDir)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".parquet"))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Expected a parquet base file under " + tableDir));
    }
  }

  private static String floatArrayLiteral(int dimension) {
    StringBuilder builder = new StringBuilder("ARRAY[");
    for (int i = 0; i < dimension; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append("CAST(").append(i).append(".0 AS FLOAT)");
    }
    return builder.append(']').toString();
  }

  private static void assertStoredVectorSchema(
      String tablePath,
      String fieldName,
      int dimension,
      HoodieSchema.Vector.VectorElementType elementType) {
    Configuration conf = new Configuration();
    HoodieSchema tableSchema = StreamerUtil.getTableConfig(tablePath, HadoopConfigurations.getHadoopConf(conf))
        .flatMap(config -> config.getTableCreateSchema())
        .orElseThrow(() -> new AssertionError("Expected table create schema for " + tablePath));
    Option<HoodieSchemaField> fieldOpt = tableSchema.getField(fieldName);
    assertTrue(fieldOpt.isPresent(), "Expected field " + fieldName + " in table schema");
    HoodieSchema fieldSchema = fieldOpt.get().schema().getNonNullType();
    assertEquals(HoodieSchemaType.VECTOR, fieldSchema.getType());
    HoodieSchema.Vector vector = (HoodieSchema.Vector) fieldSchema;
    assertEquals(dimension, vector.getDimension());
    assertEquals(elementType, vector.getVectorElementType());
  }

  private static int arrayLength(Object value) {
    assertNotNull(value);
    if (value instanceof List) {
      return ((List<?>) value).size();
    }
    return Array.getLength(value);
  }

  private static Object arrayValue(Object value, int index) {
    if (value instanceof List) {
      return ((List<?>) value).get(index);
    }
    return Array.get(value, index);
  }

  private static void assertFloatArray(Object value, float[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], ((Number) arrayValue(value, i)).floatValue(), 0.00001f);
    }
  }

  private static void assertFloatArrayLengthAndEndpoints(Object value, int expectedLength, float first, float last) {
    assertEquals(expectedLength, arrayLength(value));
    assertEquals(first, ((Number) arrayValue(value, 0)).floatValue(), 0.00001f);
    assertEquals(last, ((Number) arrayValue(value, expectedLength - 1)).floatValue(), 0.00001f);
  }

  private static void assertDoubleArray(Object value, double[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], ((Number) arrayValue(value, i)).doubleValue(), 0.0000001d);
    }
  }

  private static void assertByteArray(Object value, byte[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], ((Number) arrayValue(value, i)).byteValue());
    }
  }

  private static void assertObjectArray(Object value, Object[] expected) {
    assertEquals(expected.length, arrayLength(value));
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], arrayValue(value, i));
    }
  }

  private static boolean containsMessage(Throwable throwable, String message) {
    return throwable != null && ((throwable.getMessage() != null && throwable.getMessage().contains(message))
        || containsMessage(throwable.getCause(), message));
  }
}
