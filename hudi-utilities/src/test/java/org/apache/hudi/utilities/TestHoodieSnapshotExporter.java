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

package org.apache.hudi.utilities;

import com.beust.jcommander.ParameterException;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.HoodieSnapshotExporter.OutputFormatValidator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.config.HoodieWriteConfig.PRECOMBINE_FIELD_PROP;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY;
import static org.apache.spark.sql.SaveMode.Append;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieSnapshotExporter {

  private SparkSession spark;
  private static final String TABLE_NAME = "users";
  private static final StructType SCHEMA = DataTypes.createStructType(Arrays.asList(
          DataTypes.createStructField("id", DataTypes.LongType, false),
          DataTypes.createStructField("name", DataTypes.StringType, false),
          DataTypes.createStructField("age", DataTypes.LongType, false)
  ));

  @TempDir
  public static Path workDir;

  @BeforeAll
  static void createHudiDataset() {
    SparkSession spark = SparkSession
            .builder()
            .appName("Hoodie-data-writer")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    List<List<Object>> createData = Arrays.asList(Arrays.asList(1L, "User1", 10L), Arrays.asList(2L, "User2", 20L));
    List<List<Object>> updateData = Arrays.asList(Arrays.asList(2L, "User2", 30L));

    JavaRDD<Row> createRdd = jsc.parallelize(createData).map((Function<List<Object>, Row>) record -> RowFactory.create(record.get(0), record.get(1), record.get(2)));
    Dataset<Row> createDf = spark.createDataFrame(createRdd, SCHEMA);

    JavaRDD<Row> updateRdd = jsc.parallelize(updateData).map((Function<List<Object>, Row>) record -> RowFactory.create(record.get(0), record.get(1), record.get(2)));
    Dataset<Row> updateDf = spark.createDataFrame(updateRdd, SCHEMA);
    
    String basePath = workDir.toAbsolutePath() + File.separator + TABLE_NAME;

    Arrays.asList(createDf, updateDf).forEach(df ->
            df.write()
            .format("hudi")
            .option(RECORDKEY_FIELD_OPT_KEY, "id")
            .option(PARTITIONPATH_FIELD_OPT_KEY, "id")
            .option(PRECOMBINE_FIELD_PROP, "id")
            .option(HoodieWriteConfig.INSERT_PARALLELISM, 2)
            .option(HoodieWriteConfig.UPSERT_PARALLELISM, 2)
            .option(HoodieWriteConfig.TABLE_NAME, TABLE_NAME)
            .mode(Append)
            .save(basePath)
    );
  }

  @BeforeEach
  public void createSparkSession() {
    spark = SparkSession
            .builder()
            .appName("Hoodie-snapshot-exporter")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate();
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "parquet", "orc", "hudi"})
  public void testValidateOutputFormatWithValidFormat(String format) {
    assertDoesNotThrow(() -> {
      new OutputFormatValidator().validate(null, format);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "JSON"})
  public void testValidateOutputFormatWithInvalidFormat(String format) {
    assertThrows(ParameterException.class, () -> {
      new OutputFormatValidator().validate(null, format);
    });
  }

  @ParameterizedTest
  @NullSource
  public void testValidateOutputFormatWithNullFormat(String format) {
    assertThrows(ParameterException.class, () -> {
      new OutputFormatValidator().validate(null, format);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "parquet", "orc", "hudi"})
  public void testSnapshotExport(String format) throws IOException {
    HoodieSnapshotExporter.Config cfg = new HoodieSnapshotExporter.Config();
    cfg.sourceBasePath = workDir.toAbsolutePath() + File.separator + TABLE_NAME;
    cfg.targetOutputPath = workDir.toAbsolutePath() + File.separator + format;
    cfg.outputFormat = format;

    HoodieSnapshotExporter exporter = new HoodieSnapshotExporter();
    exporter.export(JavaSparkContext.fromSparkContext(spark.sparkContext()), cfg);

    Dataset<Row> readData = spark.read().schema(SCHEMA).format(format).load(cfg.targetOutputPath);
    assertEquals(2, readData.count());

    List<Row> rows = readData.collectAsList();
    for (Row row: rows) {
      Long id = row.getAs("id");
      String name = row.getAs("name");
      Long age = row.getAs("age");

      Long expectedAge;
      String expectedName;

      if (id.equals(1L)) {
        expectedName = "User1";
        expectedAge = 10L;
      } else {
        expectedName = "User2";
        expectedAge = 30L;
      }

      assertEquals(name, expectedName);
      assertEquals(age, expectedAge);
    }
  }
}
