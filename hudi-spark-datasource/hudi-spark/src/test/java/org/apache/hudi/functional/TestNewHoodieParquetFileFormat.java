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
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
@Disabled("HUDI-6756")
public class TestNewHoodieParquetFileFormat extends TestBootstrapReadBase {

  private static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    HoodieTableType[] tableType = {COPY_ON_WRITE, MERGE_ON_READ};
    Integer[] nPartitions = {0, 1, 2};
    for (HoodieTableType tt : tableType) {
      for (Integer n : nPartitions) {
        b.add(Arguments.of(tt, n));
      }
    }
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testNewParquetFileFormat(HoodieTableType tableType, Integer nPartitions) {
    this.bootstrapType = nPartitions == 0 ? "metadata" : "mixed";
    this.dashPartitions = true;
    this.tableType = tableType;
    this.nPartitions = nPartitions;
    setupDirs();

    //do bootstrap
    Map<String, String> options = setBootstrapOptions();
    Dataset<Row> bootstrapDf = sparkSession.emptyDataFrame();
    bootstrapDf.write().format("hudi")
        .options(options)
        .mode(SaveMode.Overwrite)
        .save(bootstrapTargetPath);
    runComparisons();

    options = basicOptions();
    doUpdate(options, "001");
    runComparisons();

    doInsert(options, "002");
    runComparisons();

    doDelete(options, "003");
    runComparisons();
  }

  protected void runComparisons() {
    if (tableType.equals(MERGE_ON_READ)) {
      runComparison(hudiBasePath);
    }
    runComparison(bootstrapTargetPath);
  }

  protected void runComparison(String tableBasePath) {
    testCount(tableBasePath);
    runIndividualComparison(tableBasePath);
    runIndividualComparison(tableBasePath, "partition_path");
    runIndividualComparison(tableBasePath, "_hoodie_record_key", "_hoodie_commit_time", "_hoodie_partition_path");
    runIndividualComparison(tableBasePath, "_hoodie_commit_time", "_hoodie_commit_seqno");
    runIndividualComparison(tableBasePath, "_hoodie_commit_time", "_hoodie_commit_seqno", "partition_path");
    runIndividualComparison(tableBasePath, "_row_key", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path");
    runIndividualComparison(tableBasePath, "_row_key", "partition_path", "_hoodie_is_deleted", "begin_lon");
  }

  protected void testCount(String tableBasePath) {
    Dataset<Row> legacyDf = sparkSession.read().format("hudi")
        .option(DataSourceReadOptions.USE_NEW_HUDI_PARQUET_FILE_FORMAT().key(), "false").load(tableBasePath);
    Dataset<Row> fileFormatDf = sparkSession.read().format("hudi")
        .option(DataSourceReadOptions.USE_NEW_HUDI_PARQUET_FILE_FORMAT().key(), "true").load(tableBasePath);
    assertEquals(legacyDf.count(), fileFormatDf.count());
  }

  protected scala.collection.Seq<String> seq(String... a) {
    return scala.collection.JavaConverters.asScalaBufferConverter(Arrays.asList(a)).asScala().toSeq();
  }

  protected void runIndividualComparison(String tableBasePath) {
    runIndividualComparison(tableBasePath, "");
  }

  protected void runIndividualComparison(String tableBasePath, String firstColumn, String... columns) {
    Dataset<Row> legacyDf = sparkSession.read().format("hudi")
        .option(DataSourceReadOptions.USE_NEW_HUDI_PARQUET_FILE_FORMAT().key(), "false").load(tableBasePath);
    Dataset<Row> fileFormatDf = sparkSession.read().format("hudi")
        .option(DataSourceReadOptions.USE_NEW_HUDI_PARQUET_FILE_FORMAT().key(), "true").load(tableBasePath);
    if (firstColumn.isEmpty()) {
      //df.except(df) does not work with map type cols
      legacyDf = legacyDf.drop("city_to_state");
      fileFormatDf = fileFormatDf.drop("city_to_state");
    } else {
      if (columns.length > 0) {
        legacyDf = legacyDf.select(firstColumn, columns);
        fileFormatDf = fileFormatDf.select(firstColumn, columns);
      } else {
        legacyDf = legacyDf.select(firstColumn);
        fileFormatDf = fileFormatDf.select(firstColumn);
      }
    }
    compareDf(legacyDf, fileFormatDf);
  }
}
