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
import org.apache.hudi.storage.{HoodieStorage, StoragePath}

import org.junit.jupiter.api.Assertions.assertTrue

class TestSlashSeparatedPartitionValue extends HoodieSparkSqlTestBase {

  test("Test slash separated date partitions") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      spark.sql(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `datestr` STRING
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.datasource.write.slash.separated.date.partitioning'='true'
           | )
           | partitioned by (`datestr`)
           | location '$tablePath'
        """.stripMargin)

      spark.sql(
        s"""
           | insert into $targetTable values
           | (1, 'a1', 1000, "2026-01-05"),
           | (2, 'a2', 2000, "2026-01-06")
        """.stripMargin)

      // check result after insert and merge data into target table
      checkAnswer(s"select id, name, ts, _hoodie_partition_path, datestr from $targetTable limit 10")(
        Seq("1", "a1", 1000, "2026/01/05", "2026-01-05"),
        Seq("2", "a2", 2000, "2026/01/06", "2026-01-06")
      )

      // Verify table config has slash separated date partitioning enabled
      val metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
        .setBasePath(tablePath)
        .build()
      val tableConfig = metaClient.getTableConfig
      assertTrue(tableConfig.getSlashSeparatedDatePartitioning,
        "Table config should have slash separated date partitioning enabled")

      // Verify that partition paths are created with slash separated date format (yyyy/MM/dd)
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2026/01/05")),
        s"Partition path 2026/01/05 should exist")
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2026/01/06")),
        s"Partition path 2026/01/06 should exist")

      val engine = new HoodieSparkEngineContext(spark.sparkContext)
      val storage = metaClient.getStorage()
      val metadataConfig = HoodieMetadataConfig.newBuilder().build()
      val metadataTable = new HoodieBackedTableMetadata(engine, storage, metadataConfig, tablePath)
      val partitionPaths = metadataTable.getAllPartitionPaths
      assertTrue(partitionPaths.contains("2026/01/05"))
      assertTrue(partitionPaths.contains("2026/01/06"))
      metadataTable.close()
    }
  }

  test("Test slash separated date partitions with already formatted input") {
    Seq(true, false).foreach { slashSeparatedPartitioning =>
      withTempDir { tmp =>
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        spark.sql(
          s"""
             |create table $targetTable (
             |  `id` string,
             |  `name` string,
             |  `ts` bigint,
             |  `datestr` STRING
             |) using hudi
             | tblproperties (
             |  'primaryKey' = 'id',
             |  'type' = 'COW',
             |  'preCombineField'='ts',
             |  'hoodie.datasource.write.slash.separated.date.partitioning'='$slashSeparatedPartitioning'
             | )
             | partitioned by (`datestr`)
             | location '$tablePath'
          """.stripMargin)

        spark.sql(
          s"""
             | insert into $targetTable values
             | (1, 'a1', 1000, "2026/01/01"),
             | (2, 'a2', 2000, "2026/01/02")
          """.stripMargin)

        val (firstPartitionValue, secondPartitionValue) = if (slashSeparatedPartitioning) {
          ("2026-01-01", "2026-01-02")
        } else {
          ("2026/01/01", "2026/01/02")
        }
        // check result after insert - already formatted values should remain as is
        checkAnswer(s"select id, name, ts, _hoodie_partition_path, datestr from $targetTable limit 10")(
          Seq("1", "a1", 1000, "2026/01/01", firstPartitionValue),
          Seq("2", "a2", 2000, "2026/01/02", secondPartitionValue)
        )

        // Verify table config
        val metaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
          .setBasePath(tablePath)
          .build()
        val tableConfig = metaClient.getTableConfig
        assertTrue(tableConfig.getSlashSeparatedDatePartitioning == slashSeparatedPartitioning,
          s"Table config should have slash separated date partitioning set to $slashSeparatedPartitioning")

        // Verify that partition paths are created with slash separated date format (yyyy/MM/dd)
        assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2026/01/01")),
          s"Partition path 2026/01/01 should exist")
        assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, "2026/01/02")),
          s"Partition path 2026/01/02 should exist")

        val engine = new HoodieSparkEngineContext(spark.sparkContext)
        val storage = metaClient.getStorage()
        val metadataConfig = HoodieMetadataConfig.newBuilder().build()
        val metadataTable = new HoodieBackedTableMetadata(engine, storage, metadataConfig, tablePath)
        val partitionPaths = metadataTable.getAllPartitionPaths
        assertTrue(partitionPaths.contains("2026/01/01"))
        assertTrue(partitionPaths.contains("2026/01/02"))
        metadataTable.close()
      }
    }
  }
}
