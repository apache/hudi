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

package org.apache.hudi.index.expression;

import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.hudi.index.expression.HoodieExpressionIndex.BLOOM_FILTER_NUM_ENTRIES;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.BLOOM_FILTER_TYPE;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.DYNAMIC_BLOOM_MAX_ENTRIES;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.FALSE_POSITIVE_RATE;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieSparkExpressionIndex extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setup() {
    initSparkContexts("TestHoodieSparkExpressionIndex");
  }

  @AfterEach
  public void tearDown() {
    cleanupSparkContexts();
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

    // Initialize the HoodieSparkExpressionIndex with the year function
    String mdtPartitionName = "expr_index_yearIndex";
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(mdtPartitionName)
        .withIndexFunction("year")
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), mdtPartitionName))
        .withSourceFields(Arrays.asList("timestampColumn"))
        .withIndexOptions(new HashMap<>())
        .build();
    HoodieSparkExpressionIndex index = new HoodieSparkExpressionIndex(indexDefinition);

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

    // Initialize the HoodieSparkExpressionIndex with the hour function
    String mdtPartitionName = "expr_index_hourIndex";
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(mdtPartitionName)
        .withIndexFunction("hour")
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), mdtPartitionName))
        .withSourceFields(Arrays.asList("timestampColumn"))
        .withIndexOptions(new HashMap<>())
        .build();
    HoodieSparkExpressionIndex index = new HoodieSparkExpressionIndex(indexDefinition);

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
    String mdtPartitionName = "expr_index_yearIndex";
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(mdtPartitionName)
        .withIndexFunction("year")
        .withIndexType(PARTITION_NAME_COLUMN_STATS)
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), mdtPartitionName))
        .withSourceFields(Arrays.asList("timestampColumn", "extraColumn"))
        .withIndexOptions(Collections.emptyMap())
        .build();
    HoodieSparkExpressionIndex index = new HoodieSparkExpressionIndex(indexDefinition);
    assertThrows(IllegalArgumentException.class, () -> index.apply(sourceColumns));
  }

  @Test
  public void testUpperFunctionWithBloomFilters() {
    // Create a test DataFrame with name column
    Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
        RowFactory.create("John Doe"),
        RowFactory.create("Jane Smith")
    ), DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("name", DataTypes.StringType, false)
    )));
    // Register the DataFrame as a temp view so we can query it
    df.createOrReplaceTempView("testData");

    // Initialize the HoodieSparkExpressionIndex with the upper function
    String mdtPartitionName = "expr_index_upperIndex";
    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(mdtPartitionName)
        .withIndexFunction("upper")
        .withIndexType(PARTITION_NAME_BLOOM_FILTERS)
        .withSourceFields(Arrays.asList("name"))
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), mdtPartitionName))
        .withIndexOptions(new HashMap<String, String>() {
          {
            put(BLOOM_FILTER_TYPE, BloomFilterTypeCode.DYNAMIC_V0.name());
            put(BLOOM_FILTER_NUM_ENTRIES, "10000");
            put(FALSE_POSITIVE_RATE, "0.01");
            put(DYNAMIC_BLOOM_MAX_ENTRIES, "100000");
          }
        })
        .build();

    // Apply the function using the index
    HoodieSparkExpressionIndex index = new HoodieSparkExpressionIndex(indexDefinition);
    Column upperColumn = index.apply(Arrays.asList(col("name")));
    assertEquals("upper(name)", upperColumn.toString());

    // validate data
    Dataset<Row> resultDf = df.withColumn("upper(name)", upperColumn);
    List<Row> results = resultDf.select("upper(name)").collectAsList();
    assertEquals("JOHN DOE", results.get(0).getAs("upper(name)").toString());
    assertEquals("JANE SMITH", results.get(1).getAs("upper(name)").toString());
  }
}
