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

package org.apache.spark.sql.hudi.feature

import org.apache.hudi.{ColumnStatsIndexSupport, PartitionStatsIndexSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import scala.collection.JavaConverters._

class TestDataSkippingQuery extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    initQueryIndexConf()
  }

  test("Test the data skipping query involves conditions " +
    "that cover both columns supported by column stats and those that are not supported.") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.enable.data.skipping" -> "true"
        ) {
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  attributes map<string, string>,
               |  price double,
               |  ts long,
               |  dt string
               |) using hudi
               | tblproperties (primaryKey = 'id', type = '$tableType')
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
                  """.stripMargin)
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1', map('color', 'red', 'size', 'M'), 10, 1000, '2021-01-05'),
               | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-06'),
               | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-07')
                  """.stripMargin)
          // Check the case where the WHERE condition only includes columns not supported by column stats
          checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          // Check the case where the WHERE condition only includes columns supported by column stats
          checkAnswer(s"select id, name, price, ts, dt from $tableName where name='a1'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          // Check the case where the WHERE condition includes both columns supported by column stats and those that are not
          checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red' and name='a1'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
        }
      }
    }
  }

  test("Test data skipping when specifying columns with column stats support.") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.enable.data.skipping" -> "true",
          "hoodie.metadata.index.column.stats.column.list" -> "name"
        ) {
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  attributes map<string, string>,
               |  price double,
               |  ts long,
               |  dt string
               |) using hudi
               | tblproperties (primaryKey = 'id', type = '$tableType')
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
                  """.stripMargin)
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1', map('color', 'red', 'size', 'M'), 10, 1000, '2021-01-05'),
               | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-06'),
               | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-07')
                  """.stripMargin)
          // Check the case where the WHERE condition only includes columns not supported by column stats
          checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          // Check the case where the WHERE condition only includes columns supported by column stats
          checkAnswer(s"select id, name, price, ts, dt from $tableName where name='a1'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          // Check the case where the WHERE condition includes both columns supported by column stats and those that are not
          checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red' and name='a1'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          // Check WHERE condition that includes both columns with existing column stats and columns of types
          // that support column stats but for which column stats do not exist
          checkAnswer(s"select id, name, price, ts, dt from $tableName where ts=1000 and name='a1'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
        }
      }
    }
  }

  test("Data skipping retains files with missing column statistics after index expansion") {
    for (tableType <- Seq("cow", "mor"); partitionStats <- Seq(false, true)) {
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.metadata.index.partition.stats.enable" -> partitionStats.toString,
          "hoodie.fileIndex.dataSkippingFailureMode" -> "strict",
          "hoodie.metadata.index.column.stats.column.list" -> "age",
          "hoodie.enable.data.skipping" -> "false"
        ) {
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  age int,
               |  ts long,
               |  dt string
               |) using hudi
               | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = '$tableType')
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
               |""".stripMargin)
          // The data column exists from the first write, but only age is indexed.
          spark.sql(s"insert into $tableName values (1, 'alice', 25, 1, 'par1')")

          withSQLConf("hoodie.metadata.index.column.stats.column.list" -> "age,name") {
            // A separate partition leaves the old file and its statistics untouched.
            spark.sql(s"insert into $tableName values (2, 'bob', 30, 2, 'par2')")
            val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
            val indexedColumns = metaClient.getIndexMetadata.get().getIndexDefinitions.get("column_stats").getSourceFields
            assert(indexedColumns.contains("age") && indexedColumns.contains("name"), indexedColumns.toString)
            val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
            val index = new ColumnStatsIndexSupport(spark, spark.table(tableName).schema,
              new TableSchemaResolver(metaClient).getTableSchema, metadataConfig, metaClient)
            val stats = index.loadColumnStatsIndexRecords(Seq("age", "name"), shouldReadInMemory = true)
              .collectAsList().asScala
            assert(stats.count(_.getColumnName == "age") == 2)
            assert(stats.count(_.getColumnName == "name") == 1)
            assert(stats.groupBy(_.getFileName).values.map(_.map(_.getColumnName).toSet).toSet ==
              Set(Set("age"), Set("age", "name")))
            val query = s"select id, name, age from $tableName where age >= 0 and name = 'alice'"
            checkAnswer(query)(Seq(1, "alice", 25))

            withSQLConf("hoodie.enable.data.skipping" -> "true") {
              spark.sql(s"alter table $tableName set tblproperties ('hoodie.enable.data.skipping' = 'true')")
              spark.catalog.refreshTable(tableName)
              withClue("Name-only predicate with data skipping enabled: ") {
                checkAnswer(s"select id, name, age from $tableName where name = 'alice'")(Seq(1, "alice", 25))
              }
              // Adding a matching age predicate must not discard the partially indexed file.
              withClue("Combined age/name predicate with data skipping enabled: ") {
                checkAnswer(query)(Seq(1, "alice", 25))
              }
            }
          }
        }
      }
    }
  }

  test("Partition statistics invalidate incomplete columns after adding indexed columns") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.metadata.index.partition.stats.enable" -> "true",
          "hoodie.metadata.index.column.stats.column.list" -> "age",
          "hoodie.parquet.small.file.limit" -> "0",
          "hoodie.enable.data.skipping" -> "false"
        ) {
          spark.sql(
            s"""
               |create table $tableName (id int, name string, age int, ts long, dt string) using hudi
               | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = '$tableType')
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
               |""".stripMargin)
          spark.sql(s"insert into $tableName values (1, 'alice', 25, 1, 'par1')")
          withSQLConf("hoodie.metadata.index.column.stats.column.list" -> "age,name") {
            // Keep both files live in the same partition; rewriting the old file would hide missing coverage.
            spark.sql(s"insert into $tableName values (2, 'bob', 30, 2, 'par1')")
            assert(spark.sql(s"select input_file_name() from $tableName").collect().map(_.getString(0)).toSet.size == 2)
            val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
            val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
            val hoodieSchema = new TableSchemaResolver(metaClient).getTableSchema
            val index = new ColumnStatsIndexSupport(spark, spark.table(tableName).schema, hoodieSchema, metadataConfig, metaClient)
            val fileStats = index.loadColumnStatsIndexRecords(Seq("age", "name"), shouldReadInMemory = true).collectAsList().asScala
            assert(fileStats.count(_.getColumnName == "age") == 2)
            assert(fileStats.count(_.getColumnName == "name") == 1)
            assert(fileStats.groupBy(_.getFileName).values.map(_.map(_.getColumnName).toSet).toSet ==
              Set(Set("age"), Set("age", "name")))
            val partitions = new PartitionStatsIndexSupport(spark, spark.table(tableName).schema, hoodieSchema, metadataConfig, metaClient)
            val partitionStats = partitions.loadColumnStatsIndexRecords(Seq("age", "name"), shouldReadInMemory = true)
              .collectAsList().asScala.filterNot(_.getIsDeleted)
            assert(partitionStats.map(_.getColumnName).toSet == Set("age"), partitionStats.toString)
            assert(partitionStats.head.getValueCount == 2)
            checkAnswer(s"select id, name, age from $tableName where name = 'alice'")(Seq(1, "alice", 25))
            withSQLConf("hoodie.enable.data.skipping" -> "true", "hoodie.fileIndex.dataSkippingFailureMode" -> "strict") {
              spark.sql(s"alter table $tableName set tblproperties ('hoodie.enable.data.skipping' = 'true')")
              spark.catalog.refreshTable(tableName)
              checkAnswer(s"select id, name, age from $tableName where name = 'alice'")(Seq(1, "alice", 25))
              checkAnswer(s"select id, name, age from $tableName where age >= 0 and name = 'alice'")(Seq(1, "alice", 25))
            }
          }
        }
      }
    }
  }

  test("Data skipping retains MOR log files with missing column statistics") {
    Seq(false, true).foreach { partitionStats =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.metadata.index.partition.stats.enable" -> partitionStats.toString,
          "hoodie.metadata.index.column.stats.column.list" -> "age",
          "hoodie.compact.inline" -> "false",
          "hoodie.compact.schedule.inline" -> "false",
          "hoodie.enable.data.skipping" -> "false",
          "hoodie.fileIndex.dataSkippingFailureMode" -> "strict"
        ) {
          spark.sql(
            s"""
               |create table $tableName (id int, name string, age int, ts long, dt string) using hudi
               | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = 'mor')
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
               |""".stripMargin)
          spark.sql(s"insert into $tableName values (1, 'before', 20, 1, 'par1')")
          // Updating the existing key writes alice to a log before name is indexed.
          spark.sql(s"update $tableName set name = 'alice', age = 25, ts = 2 where id = 1")
          withSQLConf("hoodie.metadata.index.column.stats.column.list" -> "age,name") {
            spark.sql(s"insert into $tableName values (2, 'bob', 30, 3, 'par2')")
            val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
            val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
            val index = new ColumnStatsIndexSupport(spark, spark.table(tableName).schema,
              new TableSchemaResolver(metaClient).getTableSchema, metadataConfig, metaClient)
            val stats = index.loadColumnStatsIndexRecords(Seq("age", "name"), shouldReadInMemory = true).collectAsList().asScala
            val partialLogs = stats.groupBy(_.getFileName).filter { case (fileName, columns) =>
              FSUtils.isLogFile(fileName) && columns.map(_.getColumnName).toSet == Set("age")
            }
            assert(partialLogs.nonEmpty, stats.toString)
            val query = s"select id, name, age from $tableName where age >= 0 and name = 'alice'"
            // The base file contains before, so this result proves the log contributes the matching row.
            checkAnswer(query)(Seq(1, "alice", 25))
            withSQLConf("hoodie.enable.data.skipping" -> "true") {
              spark.sql(s"alter table $tableName set tblproperties ('hoodie.enable.data.skipping' = 'true')")
              spark.catalog.refreshTable(tableName)
              checkAnswer(s"select id, name, age from $tableName where name = 'alice'")(Seq(1, "alice", 25))
              checkAnswer(query)(Seq(1, "alice", 25))
            }
          }
        }
      }
    }
  }

  test("Partition statistics survive MOR delete-only inline log appends") {
    withTempDir { tmp =>
      val tableName = generateTableName
      withSQLConf(
        "hoodie.metadata.enable" -> "true",
        "hoodie.metadata.index.column.stats.enable" -> "true",
        "hoodie.metadata.index.partition.stats.enable" -> "true",
        "hoodie.metadata.index.column.stats.column.list" -> "age,name",
        // Version 10 uses native logs; this regression must exercise HoodieInlineLogAppendHandle.
        "hoodie.write.table.version" -> "9",
        "hoodie.compact.inline" -> "false",
        "hoodie.compact.schedule.inline" -> "false",
        "hoodie.insert.shuffle.parallelism" -> "1",
        "hoodie.enable.data.skipping" -> "true"
      ) {
        spark.sql(
          s"""
             |create table $tableName (id int, name string, age int, ts long, dt string) using hudi
             | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = 'mor',
             |   hoodie.write.table.version = '9')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
             |""".stripMargin)
        spark.sql(s"insert into $tableName values (1, 'alice', 25, 1, 'par1'), " +
          "(2, 'bob', 30, 1, 'par1'), (3, 'carol', 35, 1, 'par1')")
        val locations = spark.sql(s"select _hoodie_file_name, _hoodie_partition_path from $tableName").collect()
        assert(locations.length == 3 && locations.toSet.size == 1, locations.toSeq.toString)
        val baseFile = locations.head.getString(0)
        val partitionPath = locations.head.getString(1)
        assert(!FSUtils.isLogFile(baseFile))
        val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
        val indexedColumns = Seq("age", "name")
        def loadPartitionStats() = {
          val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
          val partitions = new PartitionStatsIndexSupport(spark, spark.table(tableName).schema,
            new TableSchemaResolver(metaClient).getTableSchema, metadataConfig, metaClient)
          partitions.loadColumnStatsIndexRecords(indexedColumns, shouldReadInMemory = true)
            .collectAsList().asScala.toSeq
        }
        val initialStats = loadPartitionStats()
        assert(initialStats.size == 2 && initialStats.map(_.getColumnName).toSet == indexedColumns.toSet, initialStats.toString)
        initialStats.foreach { stats =>
          assert(!stats.getIsDeleted)
          assert(stats.getValueCount == 3 && stats.getNullCount == 0, stats.toString)
          assert(stats.getMinValue != null && stats.getMaxValue != null, stats.toString)
        }

        spark.sql(s"delete from $tableName where id = 2")
        val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
        val partitionFiles = metaClient.getStorage.listDirectEntries(new StoragePath(tmp.getCanonicalPath, partitionPath)).asScala
        val logs = partitionFiles.filter(info => FSUtils.isLogFile(info.getPath.getName)).map(new HoodieLogFile(_))
        assert(logs.size == 1, partitionFiles.toString)
        val log = logs.head
        assert(!log.isNativeLogFile)
        assert(log.getFileId == FSUtils.getFileId(baseFile))
        val hoodieSchema = new TableSchemaResolver(metaClient).getTableSchema
        val reader = HoodieLogFormat.newReader(metaClient, log, hoodieSchema)
        try {
          assert(reader.hasNext)
          val block = reader.next()
          assert(block.isInstanceOf[HoodieDeleteBlock], block.getBlockType.toString)
          assert(block.asInstanceOf[HoodieDeleteBlock].getRecordsToDelete.map(_.getRecordKey).toSet == Set("2"))
          assert(!reader.hasNext, "A delete-only append must not contain a data block")
        } finally {
          reader.close()
        }

        // Empty stats must cover every indexed column, so the log does not invalidate partition coverage.
        val index = new ColumnStatsIndexSupport(spark, spark.table(tableName).schema, hoodieSchema, metadataConfig, metaClient)
        val logStats = index.loadColumnStatsIndexRecords(indexedColumns, shouldReadInMemory = true)
          .collectAsList().asScala.filter(_.getFileName == log.getFileName)
        assert(logStats.size == 2 && logStats.map(_.getColumnName).toSet == indexedColumns.toSet, logStats.toString)
        logStats.foreach { stats =>
          assert(!stats.getIsDeleted)
          assert(stats.getValueCount == 0 && stats.getNullCount == 0, stats.toString)
          assert(stats.getMinValue == null && stats.getMaxValue == null, stats.toString)
        }
        assert(loadPartitionStats().sortBy(_.getColumnName) == initialStats.sortBy(_.getColumnName))
        checkAnswer(s"select id, name, age from $tableName where age >= 0 and name in ('alice', 'bob', 'carol')")(
          Seq(1, "alice", 25), Seq(3, "carol", 35))
      }
    }
  }

  test("bucket index query") {
    Seq("cow", "mor").foreach { tableType =>
      // table bucket prop can not be read in the query sql now, so need to set these configs
      withSQLConf("hoodie.enable.data.skipping" -> "true",
        "hoodie.bucket.index.hash.field" -> "id",
        "hoodie.bucket.index.num.buckets" -> "20",
        "hoodie.index.type" -> "BUCKET") {
        withTempDir { tmp =>
          val tableName = generateTableName
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  dt string,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (
               | primaryKey = 'id,name',
               | orderingFields = 'ts',
               | type = '$tableType',
               | hoodie.index.type = 'BUCKET',
               | hoodie.bucket.index.hash.field = 'id',
               | hoodie.bucket.index.num.buckets = '20')
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}'
       """.stripMargin)

          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1', 10, 1000, "2021-01-05"),
               | (2, 'a2', 20, 2000, "2021-01-06"),
               | (3, 'a3', 30, 3000, "2021-01-07")
              """.stripMargin)

          checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1 and name = 'a1'")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05")
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 2 or id = 5")(
            Seq(2, "a2", 20.0, 2000, "2021-01-06")
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName where id in (2, 3)")(
            Seq(2, "a2", 20.0, 2000, "2021-01-06"),
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )
          checkAnswer(s"select id, name, price, ts, dt from $tableName where id != 4")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a2", 20.0, 2000, "2021-01-06"),
            Seq(3, "a3", 30.0, 3000, "2021-01-07")
          )
          withSQLConf("hoodie.bucket.index.query.pruning" -> "false") {
            checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1")(
              Seq(1, "a1", 10.0, 1000, "2021-01-05")
            )
            checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 1 and name = 'a1'")(
              Seq(1, "a1", 10.0, 1000, "2021-01-05")
            )
            checkAnswer(s"select id, name, price, ts, dt from $tableName where id = 2 or id = 5")(
              Seq(2, "a2", 20.0, 2000, "2021-01-06")
            )
            checkAnswer(s"select id, name, price, ts, dt from $tableName where id in (2, 3)")(
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(3, "a3", 30.0, 3000, "2021-01-07")
            )
            checkAnswer(s"select id, name, price, ts, dt from $tableName where id != 4")(
              Seq(1, "a1", 10.0, 1000, "2021-01-05"),
              Seq(2, "a2", 20.0, 2000, "2021-01-06"),
              Seq(3, "a3", 30.0, 3000, "2021-01-07")
            )
          }
        }
      }
    }
  }

  test("Test column stats data skipping across multiple files with range, IN and equality predicates") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        withSQLConf(
          "hoodie.metadata.enable" -> "true",
          "hoodie.metadata.index.column.stats.enable" -> "true",
          "hoodie.enable.data.skipping" -> "true",
          "hoodie.metadata.index.column.stats.column.list" -> "id,price",
          // keep every commit in its own base file so column-stats pruning has multiple files to skip
          "hoodie.parquet.small.file.limit" -> "0"
        ) {
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | tblproperties (primaryKey = 'id', orderingFields = 'ts', type = '$tableType')
               | location '${tmp.getCanonicalPath}'
                  """.stripMargin)
          // Three separate commits, each landing in its own base file with disjoint id / price ranges.
          spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000), (2, 'a2', 20, 1000)")
          spark.sql(s"insert into $tableName values (11, 'b1', 110, 2000), (12, 'b2', 120, 2000)")
          spark.sql(s"insert into $tableName values (21, 'c1', 210, 3000), (22, 'c2', 220, 3000)")

          // Equality on the indexed key column -> only the first file qualifies.
          checkAnswer(s"select id, name, price from $tableName where id = 1")(
            Seq(1, "a1", 10.0)
          )
          // Range predicate that only the last file can satisfy.
          checkAnswer(s"select id, name, price from $tableName where id > 20")(
            Seq(21, "c1", 210.0),
            Seq(22, "c2", 220.0)
          )
          // IN predicate touching the first and last files, skipping the middle one.
          checkAnswer(s"select id, name, price from $tableName where id in (2, 22)")(
            Seq(2, "a2", 20.0),
            Seq(22, "c2", 220.0)
          )
          // Range on a second indexed column keeps only the middle file.
          checkAnswer(s"select id, name, price from $tableName where price >= 110 and price < 210")(
            Seq(11, "b1", 110.0),
            Seq(12, "b2", 120.0)
          )
          // Predicate that matches nothing -> all files pruned, empty result.
          checkAnswer(s"select id, name, price from $tableName where id = 999")()

          // Data skipping must not change results compared to a full scan.
          withSQLConf("hoodie.enable.data.skipping" -> "false") {
            checkAnswer(s"select id, name, price from $tableName where id in (2, 22)")(
              Seq(2, "a2", 20.0),
              Seq(22, "c2", 220.0)
            )
          }
        }
      }
    }
  }
}
