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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieDeltaStreamerErrorTableWriteFlow extends TestHoodieDeltaStreamerSchemaEvolutionBase {
  protected void testBase(Tuple3<Integer, Integer, Integer> sourceGenInfo) throws Exception {
    int totalRecords = sourceGenInfo.f0;
    int errorRecords = sourceGenInfo.f1;
    int numFiles = sourceGenInfo.f2;
    boolean shouldCreateMultipleSourceFiles = numFiles > 1;

    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;

    if (totalRecords > 0) {
      if (shouldCreateMultipleSourceFiles) {
        prepareParquetDFSMultiFiles(totalRecords - errorRecords, PARQUET_SOURCE_ROOT, numFiles);
      } else {
        prepareParquetDFSFiles(totalRecords - errorRecords, PARQUET_SOURCE_ROOT);
      }

      // Add error data to the source
      if (errorRecords > 0) {
        String errorDataSourceRoot = basePath + "parquetErrorFilesDfs" + testNum++;
        prepareParquetDFSFiles(errorRecords, errorDataSourceRoot);
        Dataset<Row> df = sparkSession.read().parquet(errorDataSourceRoot);
        df = df.withColumn("_row_key", functions.lit(""));
        // add error records to PARQUET_SOURCE_ROOT
        addParquetData(df, false);
      }
    } else {
      fs.mkdirs(new Path(PARQUET_SOURCE_ROOT));
    }

    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;
    this.deltaStreamer = new HoodieDeltaStreamer(getDeltaStreamerConfig(), jsc);
    this.deltaStreamer.sync();

    // base table validation
    Dataset<Row> baseDf = sparkSession.read().format("hudi").load(tableBasePath);
    assertEquals(totalRecords - errorRecords, baseDf.count());

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(tableBasePath).build();
    assertEquals(1, metaClient.getActiveTimeline().getInstants().size());

    // error table validation
    if (withErrorTable) {
      TestHoodieDeltaStreamerSchemaEvolutionExtensive.validateErrorTable(errorRecords, this.writeErrorTableInParallelWithBaseTable);
    }
  }

  protected static Stream<Arguments> testErrorTableWriteFlowArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    // totalRecords, numErrorRecords, numSourceFiles, WriteOperationType, shouldWriteErrorTableInUnionWithBaseTable

    // empty source, error table union enabled, INSERT
    b.add(Arguments.of(0, 0, 0, WriteOperationType.INSERT, true));
    // empty source, error table union disabled, INSERT
    b.add(Arguments.of(0, 0, 0, WriteOperationType.INSERT, false));
    // non-empty source, error table union enabled, INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.INSERT, true));
    // non-empty source, error table union disabled, INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.INSERT, false));
    // non-empty source, error table union enabled, UPSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.UPSERT, true));
    // non-empty source, error table union disabled, UPSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.UPSERT, false));
    // non-empty source, error table union enabled, BULK_INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.BULK_INSERT, true));
    // non-empty source, error table union disabled, BULK_INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.BULK_INSERT, false));
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testErrorTableWriteFlowArgs")
  void testErrorTableWriteFlow(
      int totalRecords,
      int numErrorRecords,
      int numSourceFiles,
      WriteOperationType wopType,
      boolean writeErrorTableInParallel) throws Exception {
    this.withErrorTable = true;
    this.writeErrorTableInParallelWithBaseTable = writeErrorTableInParallel;
    this.writeOperationType = wopType;
    this.useSchemaProvider = false;
    this.useTransformer = false;
    this.tableType = "COPY_ON_WRITE";
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.rowWriterEnable = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.dfsSourceLimitBytes = 100000000; // set source limit to 100mb
    testBase(Tuple3.of(totalRecords, numErrorRecords, numSourceFiles));
  }
}