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
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional-b")
public class TestNewHoodieParquetFileFormat extends TestBootstrapReadBase {

  private static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    b.add(Arguments.of(MERGE_ON_READ, 0));
    b.add(Arguments.of(COPY_ON_WRITE, 1));
    b.add(Arguments.of(MERGE_ON_READ, 2));
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
        .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "false")
        .load(tableBasePath);
    Dataset<Row> fileFormatDf = sparkSession.read().format("hudi")
        .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true")
        .load(tableBasePath);
    assertEquals(legacyDf.count(), fileFormatDf.count());
  }

  protected scala.collection.Seq<String> seq(String... a) {
    return scala.collection.JavaConverters.asScalaBufferConverter(Arrays.asList(a)).asScala().toSeq();
  }

  protected void runIndividualComparison(String tableBasePath) {
    runIndividualComparison(tableBasePath, "");
  }

  protected void runIndividualComparison(String tableBasePath, String firstColumn, String... columns) {
    List<String> queryTypes = new ArrayList<>();
    queryTypes.add(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL());
    if (tableType.equals(MERGE_ON_READ)) {
      queryTypes.add(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL());
    }
    for (String queryType : queryTypes) {
      Dataset<Row> legacyDf = sparkSession.read().format("hudi")
          .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "false")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), queryType)
          .load(tableBasePath);
      Dataset<Row> fileFormatDf = sparkSession.read().format("hudi")
          .option(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key(), "true")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), queryType)
          .load(tableBasePath);
      if (firstColumn.isEmpty()) {
        //df.except(df) does not work with map type cols
        legacyDf = legacyDf.drop("city_to_state");
        fileFormatDf = fileFormatDf.drop("city_to_state");

        //TODO: [HUDI-3204] for toHadoopFs in BaseFileOnlyRelation, the partition columns will be at the end
        //so just drop column that is out of order here for now
        if (queryType.equals(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL())
            && tableType.equals(MERGE_ON_READ) && nPartitions > 0) {
          legacyDf = legacyDf.drop("partition_path");
          fileFormatDf = fileFormatDf.drop("partition_path");
        }
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
}