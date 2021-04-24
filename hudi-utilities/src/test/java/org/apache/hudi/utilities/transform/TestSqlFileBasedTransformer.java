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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class TestSqlFileBasedTransformer extends UtilitiesTestBase {
  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
    UtilitiesTestBase.Helpers.copyToDFS(
        "delta-streamer-config/sql-file-transformer.sql",
        UtilitiesTestBase.dfs,
        UtilitiesTestBase.dfsBasePath + "/sql-file-transformer.sql");
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @Override
  @BeforeEach
  public void setup() throws Exception {
    super.setup();
  }

  @Override
  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testSqlFileBasedTransformer() {
    TypedProperties props = new TypedProperties();
    SqlFileBasedTransformer sqlFileTransformer = new SqlFileBasedTransformer();
    Dataset<Row> inputDatasetRows = getInputDatasetRows();

    // Test if the class throws illegal argument exception when argument not present.
    assertThrows(
        IllegalArgumentException.class,
        () -> sqlFileTransformer.apply(jsc, sparkSession, inputDatasetRows, props));

    // Test if the class throws hoodie IO exception correctly when given a incorrect config.
    props.setProperty(
        "hoodie.deltastreamer.transformer.sql.file",
        UtilitiesTestBase.dfsBasePath + "/non-exist-sql-file.sql");
    assertThrows(
        HoodieIOException.class,
        () -> sqlFileTransformer.apply(jsc, sparkSession, inputDatasetRows, props));

    // Test if the SQL file based transformer works as expected for the correct input.
    props.setProperty(
        "hoodie.deltastreamer.transformer.sql.file",
        UtilitiesTestBase.dfsBasePath + "/sql-file-transformer.sql");
    Dataset<Row> transformedRow =
        sqlFileTransformer.apply(jsc, sparkSession, inputDatasetRows, props);

    // Called distinct() and sort() to match the transformation in this file:
    // hudi-utilities/src/test/resources/delta-streamer-config/sql-file-transformer.sql
    String[] expected =
        inputDatasetRows
            .distinct()
            .sort("col1")
            .as(Encoders.STRING())
            .collectAsList()
            .toArray(new String[0]);
    String[] actual = transformedRow.as(Encoders.STRING()).collectAsList().toArray(new String[0]);
    assertArrayEquals(expected, actual);
  }

  private Dataset<Row> getInputDatasetRows() {
    // Create few rows with duplicate data.
    List<Row> list = new ArrayList<>();
    list.add(RowFactory.create("one"));
    list.add(RowFactory.create("two"));
    list.add(RowFactory.create("three"));
    list.add(RowFactory.create("four"));
    list.add(RowFactory.create("four"));
    // Create the schema struct.
    List<org.apache.spark.sql.types.StructField> listOfStructField = new ArrayList<>();
    listOfStructField.add(DataTypes.createStructField("col1", DataTypes.StringType, true));
    StructType structType = DataTypes.createStructType(listOfStructField);
    SparkSession spark = sparkSession.builder().getOrCreate();
    // Create the data frame with the rows and schema.
    return spark.createDataFrame(list, structType);
  }
}
