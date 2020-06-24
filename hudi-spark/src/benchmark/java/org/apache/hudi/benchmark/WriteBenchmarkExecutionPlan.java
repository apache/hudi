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

package org.apache.hudi.benchmark;

import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.utils.DataSourceTestUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Execution plan for benchmarking. This has set up methods and acts as arguments to benchmark methods
 */
@State(Scope.Benchmark)
public class WriteBenchmarkExecutionPlan {

  @Param( {"itr1"})
  public String iterationIndex;

  String basePath = "/tmp/hudi_benchmark/";
  int totalRecordsToTest = 1000;
  int parallelism = 1;
  SparkSession spark;
  JavaSparkContext jssc;
  FileSystem fs;
  HoodieTestDataGenerator dataGen;
  Dataset<Row> inputDF;

  @Setup(Level.Iteration)
  public void doSetup() throws Exception {
    try {
      spark = SparkSession.builder().appName("Hoodie Write Benchmark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[2]").getOrCreate();
      jssc = new JavaSparkContext(spark.sparkContext());
      spark.sparkContext().setLogLevel("WARN");
      fs = FileSystem.get(jssc.hadoopConfiguration());
      dataGen = new HoodieTestDataGenerator();
      List<HoodieRecord> recordsSoFar = new ArrayList<>(dataGen.generateInserts("001", totalRecordsToTest));
      List<String> records = DataSourceTestUtils.convertToStringList(recordsSoFar);
      inputDF = spark.read().json(jssc.parallelize(records, 1));
    } catch (IOException e) {
      e.printStackTrace();
      throw new Exception("Exception thrown while generating records to write ", e);
    }
  }
}
