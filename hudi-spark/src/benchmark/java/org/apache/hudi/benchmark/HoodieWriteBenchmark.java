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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks some of the write operations using jmh
 */
public class HoodieWriteBenchmark {

  private static final String pathPrefix = "hudi-benchmark";

  /**
   * Benchmarks insert in Hudi
   */
  @Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkInsert(WriteBenchmarkExecutionPlan plan) throws Exception {
    try {
      String randomPath = UUID.randomUUID().toString();
      org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(plan.basePath + "/" + pathPrefix + "/" + randomPath);
      plan.fs.mkdirs(tablePath);
      doWrites(plan.inputDF, tablePath, plan.parallelism, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL());
    } catch (Throwable e) {
      e.printStackTrace();
      throw new Exception("Exception thrown while running benchmark", e);
    } finally {
      FileUtils.deleteDirectory(new File(plan.basePath + "/" + pathPrefix));
    }
  }

  /**
   * Benchmarks bulk insert in Hudi
   */
  @Fork(value = 1)
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Warmup(iterations = 1)
  @Measurement(iterations = 1)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkBulkInsert(WriteBenchmarkExecutionPlan plan) throws Exception {
    try {
      String randomPath = UUID.randomUUID().toString();
      org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(plan.basePath + "/" + pathPrefix + "/" + randomPath);
      plan.fs.mkdirs(tablePath);
      doWrites(plan.inputDF, tablePath, plan.parallelism, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    } catch (Throwable e) {
      e.printStackTrace();
      throw new Exception("Exception thrown while running benchmark", e);
    } finally {
      FileUtils.deleteDirectory(new File(plan.basePath + "/" + pathPrefix));
    }
  }

  private void doWrites(Dataset<Row> inputDF1, org.apache.hadoop.fs.Path tablePath, int parallelism, String operation) {
    DataFrameWriter<Row> writer = inputDF1.write().format("org.apache.hudi")
        // set all parallelism for now
        .option("hoodie.insert.shuffle.parallelism", parallelism)
        .option("hoodie.bulkinsert.shuffle.parallelism", parallelism)
        .option("hoodie.upsert.shuffle.parallelism", parallelism)
        // Hoodie Table Type
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), HoodieTableType.COPY_ON_WRITE.name())
        // operation type
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), operation)
        // This is the record key
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        // this is the partition to place it into
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        // use to combine duplicate records in input/with disk val
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        // Used by hive sync and queries
        .option(HoodieWriteConfig.TABLE_NAME, "bulk_insert_test_tbl")
        // Add Key Extractor
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), SimpleKeyGenerator.class.getCanonicalName())
        // This will remove any existing data at path below, and create a
        .mode(SaveMode.Overwrite);
    writer.save(tablePath.toString());
  }

}
