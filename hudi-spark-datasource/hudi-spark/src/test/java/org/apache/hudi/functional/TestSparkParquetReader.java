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

package org.apache.hudi.functional;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.util.JavaConversions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestSparkParquetReader extends TestBootstrapReadBase {

  @Test
  public void testReader() {
    dataGen = new HoodieTestDataGenerator(dashPartitionPaths);
    int n = 10;
    Dataset<Row> inserts = makeInsertDf("000", n);
    inserts.write().format("parquet").save(bootstrapBasePath);
    Dataset<Row> parquetReadRows = JavaConversions.createTestDataFrame(sparkSession, bootstrapBasePath, HoodieFileFormat.PARQUET);
    Dataset<Row> datasourceReadRows = sparkSession.read().format("parquet").load(bootstrapBasePath);
    assertEquals(datasourceReadRows.count(), n);
    assertEquals(parquetReadRows.count(), n);
    assertEquals(datasourceReadRows.except(parquetReadRows).count(), 0);
    assertEquals(parquetReadRows.except(datasourceReadRows).count(), 0);
  }

  /**
   * Base files holding ShortType / ByteType columns as INT32 annotated INT(16) / INT(8) exist in
   * tables written by bulk_insert before #20503 (and by Spark's own ParquetWriteSupport), and by
   * Flink's bulk_insert and append writers up to 1.2.0 for TINYINT / SMALLINT, while the table
   * schema records the columns as int. Reading them with the table schema must widen the values
   * on the vectorized path too, not hand an int consumer the file's short vector.
   */
  @Test
  public void testReadInt16AndInt8ColumnsWithWiderTableSchema() {
    StructType fileSchema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("small", DataTypes.ShortType, true)
        .add("tiny", DataTypes.ByteType, true)
        .add("small_as_long", DataTypes.ShortType, true);
    List<Row> rows = Arrays.asList(
        RowFactory.create(1, (short) -5, (byte) -7, Short.MIN_VALUE),
        RowFactory.create(2, (short) 300, Byte.MAX_VALUE, Short.MAX_VALUE),
        RowFactory.create(3, null, null, null));
    sparkSession.createDataFrame(rows, fileSchema).coalesce(1).write().parquet(bootstrapBasePath);
    // Spark maps INT32 + INT(16) / INT(8) back to ShortType / ByteType, so this pins that the
    // file carries the annotations under test
    assertEquals(fileSchema.fields()[1].dataType(), sparkSession.read().parquet(bootstrapBasePath).schema().apply("small").dataType());
    assertEquals(fileSchema.fields()[2].dataType(), sparkSession.read().parquet(bootstrapBasePath).schema().apply("tiny").dataType());

    StructType tableSchema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("small", DataTypes.IntegerType, true)
        .add("tiny", DataTypes.IntegerType, true)
        .add("small_as_long", DataTypes.LongType, true);
    assertEquals("true", sparkSession.conf().get("spark.sql.parquet.enableVectorizedReader"));
    List<Row> actual = JavaConversions.createTestDataFrame(sparkSession, bootstrapBasePath, HoodieFileFormat.PARQUET, tableSchema)
        .collectAsList().stream()
        .sorted(Comparator.comparingInt(r -> r.getInt(0)))
        .collect(Collectors.toList());
    assertEquals(Arrays.asList(
        RowFactory.create(1, -5, -7, (long) Short.MIN_VALUE),
        RowFactory.create(2, 300, (int) Byte.MAX_VALUE, (long) Short.MAX_VALUE),
        RowFactory.create(3, null, null, null)), actual);
  }
}
