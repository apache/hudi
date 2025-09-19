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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestIncrementalQueries extends SparkClientFunctionalTestHarness {
  @ParameterizedTest
  @CsvSource({
      "6,COPY_ON_WRITE", "8,COPY_ON_WRITE", "9,COPY_ON_WRITE",
      "6,MERGE_ON_READ", "8,MERGE_ON_READ", "9,MERGE_ON_READ"})
  void testIncrementalQueryWithMultiCommitsInSameFile(String tableVersion, String tableType) {
    String path = basePath();
    Map<String, String> hudiOptions = createWriteOption(tableVersion, tableType);
    StructType schema = createTableSchema();
    // Write three commits.
    writeData(hudiOptions, path, tableVersion, tableType, schema);
    // Get the commits and their 'commit time' based on table version.
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    List<Pair<HoodieInstant, String>> sortedInstants = metaClient.getActiveTimeline().getInstants()
        .stream()
        .map(instant -> Pair.of(
            instant, tableVersion.equals("6") ? instant.requestedTime() : instant.getCompletionTime()))
        .collect(Collectors.toList());

    // Run incremental query for CASE 1: last two commits are within the range.
    // Make sure the records from the second commit are included.
    // This avoids the differences between different versions. That is,
    // the range type of table version 6 is open_close, but that of > 6 is close_close by default.
    String startTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(1).getRight()) - 1);
    Dataset<Row> result = spark().read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startTimestamp)
        .option(DataSourceReadOptions.END_COMMIT().key(), sortedInstants.get(2).getRight()).load(path);
    // Only records from the last two commits should be returned.
    assertEquals(3,
        result.filter(new Column("_hoodie_commit_time")
                .isin(
                    sortedInstants.get(1).getLeft().requestedTime(),
                    sortedInstants.get(2).getLeft().requestedTime()))
            .count());
    assertFalse(
        result.filter(new Column("_hoodie_commit_time")
                .isin(
                    sortedInstants.get(1).getLeft().requestedTime(),
                    sortedInstants.get(2).getLeft().requestedTime()))
            .isEmpty());

    // Run incremental query for CASE 2: start time is larger than the newest instant.
    // That is, no instances would fall into this range.
    startTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(2).getRight()) + 100);
    String endTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(2).getRight()) + 200);
    result = spark().read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startTimestamp)
        .option(DataSourceReadOptions.END_COMMIT().key(), endTimestamp).load(path);
    assertTrue(result.collectAsList().isEmpty());

    // Run incremental query for CASE 3: start time is larger than the newest instant.
    // That is, no instances would fall into this range.
    startTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(0).getRight()) - 200);
    endTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(0).getRight()) - 100);
    result = spark().read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startTimestamp)
        .option(DataSourceReadOptions.END_COMMIT().key(), endTimestamp).load(path);
    assertTrue(result.collectAsList().isEmpty());

    // Run incremental query for CASE 4: start time is the second instant + 1, end time is the last instant - 1.
    // That is, no instances would fall into this range.
    startTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(1).getRight()) + 1);
    endTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(2).getRight()) - 1);
    result = spark().read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startTimestamp)
        .option(DataSourceReadOptions.END_COMMIT().key(), endTimestamp).load(path);
    assertTrue(result.collectAsList().isEmpty());

    // Run incremental query for CASE 5: start time is the second instant + 1, end time is the last instant.
    // That is, the last instant would fall into this range.
    startTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(1).getRight()) + 1);
    endTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(2).getRight()));
    result = spark().read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startTimestamp)
        .option(DataSourceReadOptions.END_COMMIT().key(), endTimestamp).load(path);
    assertEquals(1, result.filter(new Column("_hoodie_commit_time")
        .isin(sortedInstants.get(2).getLeft().requestedTime())).count());
  }

  private Map<String, String> createWriteOption(String tableVersion, String tableType) {
    Map<String, String> hudiOptions = new HashMap<>();
    hudiOptions.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "column1");
    hudiOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "column1,column2");
    hudiOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "");
    hudiOptions.put(HoodieTableConfig.NAME.key(), "test");
    hudiOptions.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion);
    hudiOptions.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    hudiOptions.put(HoodieMetadataConfig.ENABLE.key(), "false");
    hudiOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);
    return hudiOptions;
  }

  private StructType createTableSchema() {
    return DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("column1", DataTypes.StringType, true),
        DataTypes.createStructField("column2", DataTypes.StringType, true),
        DataTypes.createStructField("column3", DataTypes.StringType, true)
    }).asNullable();
  }

  // Write three commits and validate the table type and version.
  private void writeData(Map<String, String> hudiOptions,
                         String path,
                         String tableVersion,
                         String tableType,
                         StructType schema) {
    // First commit.
    Row rowForFirstCommit = RowFactory.create(
        "commit1.rec1.col1", "commit1.rec1.col2", "commit1.rec1.col3");
    writeData(Collections.singletonList(rowForFirstCommit), schema, hudiOptions, path);
    // Validate table version and type.
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertEquals(
        Integer.valueOf(tableVersion),
        metaClient.getTableConfig().getTableVersion().versionCode());
    assertEquals(tableType, metaClient.getTableConfig().getTableType().name());
    // Second commit.
    Row firstRowForSecondCommit = RowFactory.create(
        "commit2.rec1.col1", "commit2.rec1.col2", "commit2.rec1.col3");
    Row secondRowForSecondCommit = RowFactory.create(
        "commit2.rec2.col1", "commit2.rec2.col2", "commit2.rec2.col3");
    writeData(Arrays.asList(firstRowForSecondCommit, secondRowForSecondCommit), schema, hudiOptions, path);
    // Third commit.
    Row firstRowForThirdCommit = RowFactory.create(
        "commit3.rec1.col1", "commit3.rec1.col2", "commit3.rec1.col3");
    writeData(Collections.singletonList(firstRowForThirdCommit), schema, hudiOptions, path);
  }

  private void writeData(List<Row> records, StructType schema, Map<String, String> hudiOptions, String basePath) {
    spark().createDataset(
            records,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(hudiOptions)
        .mode(SaveMode.Append)
        .save(basePath);
  }
}
