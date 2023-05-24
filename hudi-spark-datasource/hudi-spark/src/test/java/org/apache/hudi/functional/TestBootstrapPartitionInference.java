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

import org.apache.hudi.config.HoodieBootstrapConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

@Tag("functional")
public class TestBootstrapPartitionInference extends TestBootstrapRead {
  private static Stream<Arguments> testPartitionInferenceArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    String[] bootstrapType = {"full", "metadata", "mixed"};
    Integer[] nPartitions = {1,2};
    for (String bt : bootstrapType) {
      for (Integer n : nPartitions) {
        b.add(Arguments.of(bt, n));
      }
    }
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testPartitionInferenceArgs")
  public void testPartitionInference(String bootstrapType,  Integer nPartitions) {
    this.bootstrapType = bootstrapType;
    this.dashPartitions = true;
    this.tableType = "COPY_ON_WRITE";
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
  protected Dataset<Row> makeUpdateDf(String instantTime) {
    Dataset<Row> df = super.makeUpdateDf(instantTime);
    return df.withColumn("newPartition",  df.col("partition_path").cast("date"))
        .drop("partition_path")
        .withColumnRenamed("newPartition", "partition_path");
  }

  @Override
  protected Dataset<Row> makeInsertDf() {
    Dataset<Row> df = super.makeInsertDf();
    return df.withColumn("newPartition",  df.col("partition_path").cast("date"))
        .drop("partition_path")
        .withColumnRenamed("newPartition", "partition_path");
  }

}
