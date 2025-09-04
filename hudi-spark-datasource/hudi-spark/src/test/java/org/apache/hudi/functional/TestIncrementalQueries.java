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
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestIncrementalQueries extends SparkClientFunctionalTestHarness {
  @ParameterizedTest
  @CsvSource({
      "6,COPY_ON_WRITE", "8,COPY_ON_WRITE", "9,COPY_ON_WRITE",
      "6,MERGE_ON_READ", "8,MERGE_ON_READ", "9,MERGE_ON_READ"})
  void testIncrementalQueryWithMultiCommitsInSameFile(String tableVersion, String tableType) {
    // Prepare options.
    String path = basePath();
    Map<String, String> hudiOptions = new HashMap<>();
    hudiOptions.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "column1");
    hudiOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "column1,column2");
    hudiOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "");
    hudiOptions.put(HoodieTableConfig.NAME.key(), "test");
    hudiOptions.put(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion);
    hudiOptions.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");
    hudiOptions.put(HoodieMetadataConfig.ENABLE.key(), "false");
    hudiOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);
    // Record schema.
    StructType schema = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("column1", DataTypes.StringType, true),
        DataTypes.createStructField("column2", DataTypes.StringType, true),
        DataTypes.createStructField("column3", DataTypes.StringType, true)
    }).asNullable();
    // First commit.
    Row rowForFirstCommit = RowFactory.create("commit1.rec1.col1", "commit1.rec1.col2", "commit1.rec1.col3");
    writeData(Collections.singletonList(rowForFirstCommit), schema, hudiOptions, path);
    // Validate table version and type.
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertEquals(Integer.valueOf(tableVersion), metaClient.getTableConfig().getTableVersion().versionCode());
    assertEquals(tableType, metaClient.getTableConfig().getTableType().name());
    // Second commit.
    Row firstRowForSecondCommit = RowFactory.create("commit2.rec1.col1", "commit2.rec1.col2", "commit2.rec1.col3");
    Row secondRowForSecondCommit = RowFactory.create("commit2.rec2.col1", "commit2.rec2.col2", "commit2.rec2.col3");
    writeData(Arrays.asList(firstRowForSecondCommit, secondRowForSecondCommit), schema, hudiOptions, path);
    // Third commit.
    Row firstRowForThirdCommit = RowFactory.create("commit3.rec1.col1", "commit3.rec1.col2", "commit3.rec1.col3");
    writeData(Collections.singletonList(firstRowForThirdCommit), schema, hudiOptions, path);
    // Get the last two commits.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    List<Pair<HoodieInstant, String>> sortedInstants = metaClient.getActiveTimeline().getInstants()
        .stream().skip(1)
        .map(instant -> Pair.of(
            instant, tableVersion.equals("6") ? instant.requestedTime() : instant.getCompletionTime()))
        .collect(Collectors.toList());
    // Make sure the records from the second commit are included.
    // This avoids the differences between different versions.
    String startTimestamp = String.valueOf(Long.valueOf(sortedInstants.get(0).getRight()) - 1);
    // Run incremental query.
    Dataset<Row> result = spark().read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startTimestamp)
        .option(DataSourceReadOptions.END_COMMIT().key(), sortedInstants.get(1).getRight()).load(path);
    java.util.List<Row> rows = result.collectAsList();
    // Only records from the last two commits should be returned.
    assertEquals(3, rows.size());
    rows.forEach(r -> {
      String commitTime = r.getString(0);
      assertTrue(
          commitTime.equals(sortedInstants.get(0).getLeft().requestedTime())
              || commitTime.equals(sortedInstants.get(1).getLeft().requestedTime()));
    });
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

