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

import org.junit.jupiter.api.Assertions.assertTrue

class TestSlashSeparatedPartitionValue extends HoodieSparkSqlTestBase {

  private def createSlashPartitionedTable(targetTable: String,
                                          tablePath: String,
                                          partitionColumnType: String = "STRING",
                                          tableType: String = "COW",
                                          slashSeparatedPartitioning: Boolean = true): Unit = {
    spark.sql(
      s"""
         |create table $targetTable (
         |  `id` string,
         |  `name` string,
         |  `ts` bigint,
         |  `datestr` $partitionColumnType
         |) using hudi
         | tblproperties (
         |  'primaryKey' = 'id',
         |  'type' = '$tableType',
         |  'preCombineField'='ts',
         |  'hoodie.datasource.write.slash.separated.date.partitioning'='$slashSeparatedPartitioning'
         | )
         | partitioned by (`datestr`)
         | location '$tablePath'
      """.stripMargin)
  }

  private def buildMetaClient(tablePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(tablePath)
      .build()
  }

  private def assertPartitionDirsExist(metaClient: HoodieTableMetaClient,
                                       tablePath: String,
                                       partitions: String*): Unit = {
    partitions.foreach { partition =>
      assertTrue(metaClient.getStorage.exists(new StoragePath(tablePath, partition)),
        s"Partition path $partition should exist")
    }
  }

  /**
   * Asserts that the metadata table names the very same directories the writer created. The
   * `_hoodie_partition_path` column and an `exists` check on storage both still pass when the
   * writer-recorded partition string and the metadata-table entry disagree, so this is the
   * assertion that actually pins the two together.
   */
  private def assertMetadataTablePartitions(metaClient: HoodieTableMetaClient,
                                            tablePath: String,
                                            partitions: String*): Unit = {
    val engine = new HoodieSparkEngineContext(spark.sparkContext)
    val metadataConfig = HoodieMetadataConfig.newBuilder().build()
    val metadataTable =
      new HoodieBackedTableMetadata(engine, metaClient.getStorage, metadataConfig, tablePath)
    try {
      val partitionPaths = metadataTable.getAllPartitionPaths
      partitions.foreach { partition =>
        assertTrue(partitionPaths.contains(partition),
          s"Metadata table should list partition $partition")
      }
    } finally {
      metadataTable.close()
    }
  }

  test("Test slash separated date partitions") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      createSlashPartitionedTable(targetTable, tablePath)

      spark.sql(
        s"""
           | insert into $targetTable values
           | (1, 'a1', 1000, "2026-01-05"),
           | (2, 'a2', 2000, "2026-01-06")
        """.stripMargin)

      checkAnswer(s"select id, name, ts, _hoodie_partition_path, datestr from $targetTable limit 10")(
        Seq("1", "a1", 1000, "2026/01/05", "2026-01-05"),
        Seq("2", "a2", 2000, "2026/01/06", "2026-01-06")
      )

      val metaClient = buildMetaClient(tablePath)
      assertTrue(metaClient.getTableConfig.getSlashSeparatedDatePartitioning,
        "Table config should have slash separated date partitioning enabled")

      assertPartitionDirsExist(metaClient, tablePath, "2026/01/05", "2026/01/06")
      assertMetadataTablePartitions(metaClient, tablePath, "2026/01/05", "2026/01/06")
    }
  }

  test("Test slash separated date partitions written through the row writer") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
      withTempDir { tmp =>
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        createSlashPartitionedTable(targetTable, tablePath)

        // NOTE: The row writer derives the partition path off of an [[InternalRow]], which used to
        //       blow up with a [[ClassCastException]]; a null partition value used to NPE
        spark.sql(
          s"""
             | insert into $targetTable values
             | (1, 'a1', 1000, "2026-01-05"),
             | (2, 'a2', 2000, "2026-01-06"),
             | (3, 'a3', 3000, null)
          """.stripMargin)

        checkAnswer(s"select id, name, ts, _hoodie_partition_path, datestr from $targetTable order by id")(
          Seq("1", "a1", 1000, "2026/01/05", "2026-01-05"),
          Seq("2", "a2", 2000, "2026/01/06", "2026-01-06"),
          Seq("3", "a3", 3000, "__HIVE_DEFAULT_PARTITION__", null)
        )

        val metaClient = buildMetaClient(tablePath)
        assertPartitionDirsExist(metaClient, tablePath,
          "2026/01/05", "2026/01/06", "__HIVE_DEFAULT_PARTITION__")
        assertMetadataTablePartitions(metaClient, tablePath,
          "2026/01/05", "2026/01/06", "__HIVE_DEFAULT_PARTITION__")
      }
    }
  }

  test("Test slash separated date partitions on a DATE typed partition column") {
    // NOTE: Only bulk_insert exercises the row-writer rendering this fixes -- the insert path on a
    //       DATE column is already covered by [[TestTypedPartitionValues]]
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
      withTempDir { tmp =>
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        createSlashPartitionedTable(targetTable, tablePath, partitionColumnType = "DATE")

        // NOTE: A DATE partition value is rendered through [[BuiltinKeyGenerator#convertToLogicalDataType]]
        //       on the row-writer paths and [[HoodieAvroUtils#convertValueForAvroLogicalTypes]] on the
        //       Avro one, so all three have to land in the very same directory
        spark.sql(
          s"""
             | insert into $targetTable values
             | (1, 'a1', 1000, date'2026-01-05'),
             | (2, 'a2', 2000, date'2026-01-06')
          """.stripMargin)

        checkAnswer(s"select id, name, ts, _hoodie_partition_path, datestr from $targetTable order by id")(
          Seq("1", "a1", 1000, "2026/01/05", java.sql.Date.valueOf("2026-01-05")),
          Seq("2", "a2", 2000, "2026/01/06", java.sql.Date.valueOf("2026-01-06"))
        )

        val metaClient = buildMetaClient(tablePath)
        assertPartitionDirsExist(metaClient, tablePath, "2026/01/05", "2026/01/06")
        assertMetadataTablePartitions(metaClient, tablePath, "2026/01/05", "2026/01/06")
      }
    }
  }

  test("Test upsert into a slash separated date partition") {
    Seq("COW", "MOR").foreach { tableType =>
      withTempDir { tmp =>
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        createSlashPartitionedTable(targetTable, tablePath, tableType = tableType)

        spark.sql(
          s"""
             | insert into $targetTable values
             | (1, 'a1', 1000, "2026-01-05")
          """.stripMargin)

        // NOTE: The index and the file-system view have to agree on the slash partition path for
        //       the second write to be recognised as an update. When they disagree the record is
        //       inserted a second time instead, which surfaces as a duplicate rather than an error
        spark.sql(
          s"""
             | merge into $targetTable as t
             | using (select '1' as id, 'a1_updated' as name, 2000L as ts, cast("2026-01-05" as string) as datestr) as s
             | on t.id = s.id
             | when matched then update set *
             | when not matched then insert *
          """.stripMargin)

        checkAnswer(s"select id, name, ts, _hoodie_partition_path, datestr from $targetTable")(
          Seq("1", "a1_updated", 2000, "2026/01/05", "2026-01-05")
        )

        val metaClient = buildMetaClient(tablePath)
        assertTrue(!metaClient.getStorage.exists(new StoragePath(tablePath, "2026-01-05")),
          s"No second directory should be created for table type $tableType")
      }
    }
  }

  test("Test slash separated date partitioning rejects multiple partition fields at create") {
    withTempDir { tmp =>
      val targetTable = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

      // NOTE: A multi-field slash table writes extra path fragments that
      //       [[HoodieSparkUtils#doParsePartitionColumnValues]] cannot line up with the partition
      //       columns, so every read of the table fails under the default lazy listing. Rejecting
      //       the combination up front keeps that layout from ever being written -- HUDI issue #19666
      checkExceptionContain(
        s"""
           |create table $targetTable (
           |  `id` string,
           |  `name` string,
           |  `ts` bigint,
           |  `datestr` STRING,
           |  `city` STRING
           |) using hudi
           | tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'COW',
           |  'preCombineField'='ts',
           |  'hoodie.datasource.write.keygenerator.class'='org.apache.hudi.keygen.CustomKeyGenerator',
           |  'hoodie.datasource.write.partitionpath.field'='datestr:simple,city:simple',
           |  'hoodie.datasource.write.slash.separated.date.partitioning'='true'
           | )
           | partitioned by (`datestr`, `city`)
           | location '$tablePath'
        """.stripMargin)("but found 2: datestr,city")
    }
  }

  test("Test slash separated date partitioning rejects multiple partition fields at write") {
    withTempDir { tmp =>
      val tablePath = s"${tmp.getCanonicalPath}/${generateTableName}"

      // df.write bypasses the catalog-level check, so the rejection has to come from
      // [[HoodieWriterUtils#validateTableConfig]]
      val df = spark.sql(
        "select '1' as id, 'a1' as name, 1000L as ts, '2026-01-05' as datestr, 'NYC' as city")
      checkExceptionContain(() =>
        df.write.format("hudi")
          .option("hoodie.table.name", "rejected_slash_table")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.partitionpath.field", "datestr,city")
          .option("hoodie.datasource.write.slash.separated.date.partitioning", "true")
          .mode("append")
          .save(tablePath)
      )("cannot be read back")
    }
  }

  test("Test slash separated date partitions with already formatted input") {
    Seq(true, false).foreach { slashSeparatedPartitioning =>
      withTempDir { tmp =>
        val targetTable = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$targetTable"

        createSlashPartitionedTable(targetTable, tablePath,
          slashSeparatedPartitioning = slashSeparatedPartitioning)

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

        val metaClient = buildMetaClient(tablePath)
        assertTrue(metaClient.getTableConfig.getSlashSeparatedDatePartitioning == slashSeparatedPartitioning,
          s"Table config should have slash separated date partitioning set to $slashSeparatedPartitioning")

        assertPartitionDirsExist(metaClient, tablePath, "2026/01/01", "2026/01/02")
        assertMetadataTablePartitions(metaClient, tablePath, "2026/01/01", "2026/01/02")
      }
    }
  }
}
