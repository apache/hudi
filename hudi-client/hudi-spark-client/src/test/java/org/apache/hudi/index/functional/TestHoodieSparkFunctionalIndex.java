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

package org.apache.hudi.index.functional;

import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieSparkFunctionalIndex extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setup() {
    initSparkContexts("TestHoodieSparkFunctionalIndex");
  }

  @Test
  public void testYearFunction() {
    // Create a test DataFrame
    Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
        RowFactory.create(Timestamp.valueOf("2021-03-15 12:34:56")),
        RowFactory.create(Timestamp.valueOf("2022-07-10 23:45:01"))
    ), DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("timestampColumn", DataTypes.TimestampType, false)
    )));

    // Register the DataFrame as a temp view so we can query it
    df.createOrReplaceTempView("testData");

    // Initialize the HoodieSparkFunctionalIndex with the year function
    HoodieSparkFunctionalIndex index = new HoodieSparkFunctionalIndex(
        "yearIndex",
        "year",
        Arrays.asList("timestampColumn"),
        new HashMap<>()
    );

    // Apply the function using the index
    Column yearColumn = index.apply(Arrays.asList(col("timestampColumn")));

    // Add the result as a new column to the DataFrame
    Dataset<Row> resultDf = df.withColumn("year", yearColumn);

    // Collect and assert the results
    List<Row> results = resultDf.select("year").collectAsList();

    assertEquals("2021", results.get(0).getAs("year").toString());
    assertEquals("2022", results.get(1).getAs("year").toString());
  }

  @Test
  public void testHourFunction() {
    // Create a test DataFrame
    Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
        RowFactory.create(Timestamp.valueOf("2021-03-15 12:34:56")),
        RowFactory.create(Timestamp.valueOf("2022-07-10 23:45:01"))
    ), DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("timestampColumn", DataTypes.TimestampType, false)
    )));

    // Register the DataFrame as a temp view so we can query it
    df.createOrReplaceTempView("testData");

    // Initialize the HoodieSparkFunctionalIndex with the hour function
    HoodieSparkFunctionalIndex index = new HoodieSparkFunctionalIndex(
        "hourIndex",
        "hour",
        Arrays.asList("timestampColumn"),
        new HashMap<>()
    );

    // Apply the function using the index
    Column hourColumn = index.apply(Arrays.asList(col("timestampColumn")));

    // Add the result as a new column to the DataFrame
    Dataset<Row> resultDf = df.withColumn("hour", hourColumn);

    // Collect and assert the results
    List<Row> results = resultDf.select("hour").collectAsList();

    assertEquals("12", results.get(0).getAs("hour").toString());
    assertEquals("23", results.get(1).getAs("hour").toString());
  }

  @Test
  public void testApplyYearFunctionWithWrongNumberOfArguments() {
    // Setup index with the wrong number of source fields
    List<Column> sourceColumns = Arrays.asList(col("timestampColumn"), col("extraColumn"));
    HoodieSparkFunctionalIndex index = new HoodieSparkFunctionalIndex(
        "yearIndex",
        "year",
        Arrays.asList("timestampColumn", "extraColumn"),
        Collections.emptyMap()
    );
    assertThrows(IllegalArgumentException.class, () -> index.apply(sourceColumns));
  }
}
