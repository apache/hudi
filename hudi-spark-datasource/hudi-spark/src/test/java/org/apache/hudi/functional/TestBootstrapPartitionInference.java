/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.config.HoodieBootstrapConfig;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;

public class TestBootstrapPartitionInference extends TestBootstrapRead {
  private static Stream<Arguments> testPartitionInferenceArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    String[] bootstrapType = {"metadata", "mixed"};
    Boolean[] dashPartitions = {true};
    String[] tableType = {"COPY_ON_WRITE", "MERGE_ON_READ"};
    Integer[] nPartitions = {1,2};
    for (String tt : tableType) {
      for (Boolean dash : dashPartitions) {
        for (String bt : bootstrapType) {
          for (Integer n : nPartitions) {
            //can't be mixed bootstrap if it's nonpartitioned
            //don't need to test slash partitions if it's nonpartitioned
            if ((!bt.equals("mixed") && dash) || n > 0) {
              b.add(Arguments.of(bt, dash, tt, n));
            }
          }
        }
      }
    }
    return b.build();
  }


  @ParameterizedTest
  @MethodSource("testPartitionInferenceArgs")
  public void testPartitionInference(String bootstrapType, Boolean dashPartitions, String tableType, Integer nPartitions) {
    this.bootstrapType = bootstrapType;
    this.dashPartitions = dashPartitions;
    this.tableType = tableType;
    this.nPartitions = nPartitions;
    setupDirs();

    //do bootstrap
    Map<String, String> options = setBootstrapOptions();
    Dataset<Row> bootstrapDf = sparkSession.emptyDataFrame();
    bootstrapDf.write().format("hudi")
        .options(options)
        .option(HoodieBootstrapConfig.PARTITION_COLUMN_TYPE_INFERENCE.key(), "true")
        .mode(SaveMode.Overwrite)
        .save(bootstrapTargetPath);
    compareTables();

    options = basicOptions();
    doUpdate(options, "001");
    compareTables();

  }

  @Override
  public Dataset<Row> generateTestUpdates(String instantTime) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, nUpdates).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      Dataset<Row> df = sparkSession.read().json(rdd);
      df = df.withColumn("newPartition",  df.col("partition_path").cast("date"))
          .drop("partition_path")
          .withColumnRenamed("newPartition", "partition_path");
      return addPartitionColumns(df, nPartitions);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Dataset<Row> generateTestInserts() {
    List<String> records = dataGen.generateInserts("000", nInserts).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    Dataset<Row> df = sparkSession.read().json(rdd);
    df = df.withColumn("newPartition",  df.col("partition_path").cast("date"))
        .drop("partition_path")
        .withColumnRenamed("newPartition", "partition_path");
    return addPartitionColumns(df, nPartitions);
  }

  @Override
  protected void compareTables() {
    Map<String,String> readOpts = new HashMap<>();
    if (tableType.equals("MERGE_ON_READ")) {
      //Bootstrap MOR currently only has read optimized queries implemented
      readOpts.put(DataSourceReadOptions.QUERY_TYPE().key(),DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL());
    }
    Dataset<Row> hudiDf = sparkSession.read().options(readOpts).format("hudi").load(hudiBasePath);
    Dataset<Row> bootstrapDf = sparkSession.read().format("hudi").load(bootstrapTargetPath);
    if (nPartitions == 0) {
      compareDf(hudiDf.drop(dropColumns), bootstrapDf.drop(dropColumns));
      return;
    }
    compareDf(hudiDf.drop(dropColumns).drop(partitionCols), bootstrapDf.drop(dropColumns).drop(partitionCols));
    compareDf(hudiDf.select("_row_key",partitionCols), bootstrapDf.select("_row_key",partitionCols));
  }
}
