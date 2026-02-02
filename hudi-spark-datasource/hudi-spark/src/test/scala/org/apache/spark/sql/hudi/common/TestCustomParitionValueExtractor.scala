/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.common

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.sync.common.HoodieSyncConfig

import org.junit.jupiter.api.Assertions.assertTrue

class TestCustomParitionValueExtractor extends HoodieSparkSqlTestBase {
  test("Test custom partition value extractor interface") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `datestr` string,
           |  `country` string,
           |  `state` string,
           |  `city` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.datasource.write.hive_style_partitioning'='false',
           |  '${HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key()}'='org.apache.spark.sql.hudi.common.TestCustomSlashPartitionValueExtractor'
           | )
           | partitioned by (`datestr`, `country`, `state`, `city`)
           | location '$tablePath'
        """.stripMargin)
      // yyyy/mm/dd/country/state/city
      spark.sql(
        s"""
           | insert into $targetTable values
           | (1, 'a1', 1000, '2024-01-01', "USA", "CA", "SFO"),
           | (2, 'a2', 2000, '2024-01-01', "USA", "TX", "AU"),
           | (3, 'a3', 3000, '2024-01-02', "USA", "CA", "LA"),
           | (4, 'a4', 4000, '2024-01-02', "USA", "WA", "SEA"),
           | (5, 'a5', 5000, '2024-01-03', "USA", "CA", "SFO")
        """.stripMargin)

      // check result after insert and merge data into target table
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable"
        + s" where state = 'CA'")(
        Seq(1, "a1", 1000, "2024-01-01", "USA", "CA", "SFO"),
        Seq(3, "a3", 3000, "2024-01-02", "USA", "CA", "LA"),
        Seq(5, "a5", 5000, "2024-01-03", "USA", "CA", "SFO")
      )

      // Verify table config has custom partition value extractor class set
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()
      val tableConfig = metaClient.getTableConfig
      val partitionExtractorClass = tableConfig.getProps.getProperty(
        HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key())
      assertTrue(partitionExtractorClass == "org.apache.spark.sql.hudi.common.TestCustomSlashPartitionValueExtractor",
        s"Table config should have custom partition value extractor class set to TestCustomSlashPartitionValueExtractor, but got $partitionExtractorClass")

      // Verify that partition paths are created with slash separated format (yyyy/MM/dd/country/state/city)
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2024/01/01/USA/CA/SFO")),
        s"Partition path 2024/01/01/USA/CA/SFO should exist")
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2024/01/01/USA/TX/AU")),
        s"Partition path 2024/01/01/USA/TX/AU should exist")
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2024/01/02/USA/CA/LA")),
        s"Partition path 2024/01/02/USA/CA/LA should exist")
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2024/01/02/USA/WA/SEA")),
        s"Partition path 2024/01/02/USA/WA/SEA should exist")
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2024/01/03/USA/CA/SFO")),
        s"Partition path 2024/01/03/USA/CA/SFO should exist")

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage()
      val metadataConfig = HoodieMetadataConfig.newBuilder().build()
      val metadataTable = new HoodieBackedTableMetadata(engine, storage, metadataConfig, tablePath)
      val partitionPaths = metadataTable.getAllPartitionPaths
      assertTrue(partitionPaths.contains("2024/01/01/USA/CA/SFO"))
      assertTrue(partitionPaths.contains("2024/01/01/USA/TX/AU"))
      assertTrue(partitionPaths.contains("2024/01/02/USA/CA/LA"))
      assertTrue(partitionPaths.contains("2024/01/02/USA/WA/SEA"))
      assertTrue(partitionPaths.contains("2024/01/03/USA/CA/SFO"))
      metadataTable.close()
    }
  }

  test("Test custom partition value extractor with partition pruning and filtering") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `datestr` string,
           |  `country` string,
           |  `state` string,
           |  `city` string
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.datasource.write.hive_style_partitioning'='false',
           |  '${HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key()}'='org.apache.spark.sql.hudi.common.TestCustomSlashPartitionValueExtractor'
           | )
           | partitioned by (`datestr`, `country`, `state`, `city`)
           | location '$tablePath'
        """.stripMargin)

      // Insert data across multiple partitions
      spark.sql(
        s"""
           | insert into $targetTable values
           | (1, 'a1', 1000, '2024-01-01', "USA", "CA", "SFO"),
           | (2, 'a2', 2000, '2024-01-01', "USA", "CA", "LA"),
           | (3, 'a3', 3000, '2024-01-01', "USA", "TX", "AU"),
           | (4, 'a4', 4000, '2024-01-02', "USA", "CA", "SFO"),
           | (5, 'a5', 5000, '2024-01-02', "USA", "WA", "SEA"),
           | (6, 'a6', 6000, '2024-01-03', "USA", "CA", "LA"),
           | (7, 'a7', 7000, '2024-01-03', "CAN", "ON", "TOR"),
           | (8, 'a8', 8000, '2024-01-04', "USA", "NY", "NYC")
        """.stripMargin)

      // Test partition pruning with single partition column filter (state)
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable where state = 'CA' order by id")(
        Seq(1, "a1", 1000, "2024-01-01", "USA", "CA", "SFO"),
        Seq(2, "a2", 2000, "2024-01-01", "USA", "CA", "LA"),
        Seq(4, "a4", 4000, "2024-01-02", "USA", "CA", "SFO"),
        Seq(6, "a6", 6000, "2024-01-03", "USA", "CA", "LA")
      )

      // Test partition pruning with multiple partition column filters
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable where state = 'CA' and city = 'SFO' order by id")(
        Seq(1, "a1", 1000, "2024-01-01", "USA", "CA", "SFO"),
        Seq(4, "a4", 4000, "2024-01-02", "USA", "CA", "SFO")
      )

      // Test partition pruning with date filter
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable where datestr = '2024-01-01' order by id")(
        Seq(1, "a1", 1000, "2024-01-01", "USA", "CA", "SFO"),
        Seq(2, "a2", 2000, "2024-01-01", "USA", "CA", "LA"),
        Seq(3, "a3", 3000, "2024-01-01", "USA", "TX", "AU")
      )

      // Test partition pruning with country filter
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable where country = 'CAN' order by id")(
        Seq(7, "a7", 7000, "2024-01-03", "CAN", "ON", "TOR")
      )

      // Test partition pruning with combined date and state filter
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable where datestr = '2024-01-02' and state = 'CA' order by id")(
        Seq(4, "a4", 4000, "2024-01-02", "USA", "CA", "SFO")
      )

      // Test partition pruning with IN clause
      checkAnswer(s"select id, name, ts, datestr, country, state, city from $targetTable where state IN ('CA', 'NY') order by id")(
        Seq(1, "a1", 1000, "2024-01-01", "USA", "CA", "SFO"),
        Seq(2, "a2", 2000, "2024-01-01", "USA", "CA", "LA"),
        Seq(4, "a4", 4000, "2024-01-02", "USA", "CA", "SFO"),
        Seq(6, "a6", 6000, "2024-01-03", "USA", "CA", "LA"),
        Seq(8, "a8", 8000, "2024-01-04", "USA", "NY", "NYC")
      )

      // Test reading with _hoodie_partition_path to verify custom partition format
      checkAnswer(s"select id, _hoodie_partition_path from $targetTable where state = 'CA' order by id")(
        Seq("1", "2024/01/01/USA/CA/SFO"),
        Seq("2", "2024/01/01/USA/CA/LA"),
        Seq("4", "2024/01/02/USA/CA/SFO"),
        Seq("6", "2024/01/03/USA/CA/LA")
      )

      // Create DataFrame and analyze query plan to verify partition pruning
      val dfWithStateFilter = spark.sql(s"select * from $targetTable where state = 'CA'")
      val planWithStateFilter = dfWithStateFilter.queryExecution.executedPlan.toString()
      // Verify partition filters are pushed down
      assertTrue(planWithStateFilter.contains("PartitionFilters") || planWithStateFilter.contains("PushedFilters"),
        s"Query plan should contain partition filters for state column")

      // Test DataFrame API with multiple partition filters
      val dfWithMultipleFilters = spark.table(targetTable)
        .filter("state = 'CA' and datestr = '2024-01-01'")
      val planWithMultipleFilters = dfWithMultipleFilters.queryExecution.executedPlan.toString()
      assertTrue(planWithMultipleFilters.contains("PartitionFilters") || planWithMultipleFilters.contains("PushedFilters"),
        s"Query plan should contain partition filters for multiple columns")

      // Verify the filtered results
      val multiFilterResults = dfWithMultipleFilters.select("id", "name", "state", "datestr").orderBy("id").collect()
      assertTrue(multiFilterResults.length == 2, s"Expected 2 rows but got ${multiFilterResults.length}")
      assertTrue(multiFilterResults(0).getString(0) == "1", s"First row id should be 1")
      assertTrue(multiFilterResults(0).getString(1) == "a1", s"First row name should be a1")
      assertTrue(multiFilterResults(0).getString(2) == "CA", s"First row state should be CA")
      assertTrue(multiFilterResults(0).getString(3) == "2024-01-01", s"First row datestr should be 2024-01-01")
      assertTrue(multiFilterResults(1).getString(0) == "2", s"Second row id should be 2")
      assertTrue(multiFilterResults(1).getString(1) == "a2", s"Second row name should be a2")

      // Test DataFrame with country filter
      val dfWithCountryFilter = spark.table(targetTable).filter("country = 'CAN'")
      val planWithCountryFilter = dfWithCountryFilter.queryExecution.executedPlan.toString()
      assertTrue(planWithCountryFilter.contains("PartitionFilters") || planWithCountryFilter.contains("PushedFilters"),
        s"Query plan should contain partition filters for country column")

      val countryFilterResults = dfWithCountryFilter.select("id", "country").orderBy("id").collect()
      assertTrue(countryFilterResults.length == 1, s"Expected 1 row but got ${countryFilterResults.length}")
      assertTrue(countryFilterResults(0).getString(0) == "7", s"Row id should be 7")
      assertTrue(countryFilterResults(0).getString(1) == "CAN", s"Row country should be CAN")

      // Verify all partitions exist as expected
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage()
      val metadataConfig = HoodieMetadataConfig.newBuilder().build()
      val metadataTable = new HoodieBackedTableMetadata(engine, storage, metadataConfig, tablePath)
      val partitionPaths = metadataTable.getAllPartitionPaths

      // Verify expected partition paths
      assertTrue(partitionPaths.contains("2024/01/01/USA/CA/SFO"))
      assertTrue(partitionPaths.contains("2024/01/01/USA/CA/LA"))
      assertTrue(partitionPaths.contains("2024/01/01/USA/TX/AU"))
      assertTrue(partitionPaths.contains("2024/01/02/USA/CA/SFO"))
      assertTrue(partitionPaths.contains("2024/01/02/USA/WA/SEA"))
      assertTrue(partitionPaths.contains("2024/01/03/USA/CA/LA"))
      assertTrue(partitionPaths.contains("2024/01/03/CAN/ON/TOR"))
      assertTrue(partitionPaths.contains("2024/01/04/USA/NY/NYC"))

      metadataTable.close()
    }
  }
}
