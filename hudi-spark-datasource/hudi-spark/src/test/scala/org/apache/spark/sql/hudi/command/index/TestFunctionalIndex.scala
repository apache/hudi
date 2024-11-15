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

package org.apache.spark.sql.hudi.command.index

import org.apache.hudi.DataSourceWriteOptions.{INSERT_OPERATION_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.{DataSourceReadOptions, FunctionalIndexSupport, HoodieFileIndex, HoodieSparkUtils}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.HiveSyncConfigHolder._
import org.apache.hudi.hive.testutils.HiveTestUtil
import org.apache.hudi.hive.{HiveSyncTool, HoodieHiveSyncClient}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.metadata.{HoodieMetadataFileSystemView, MetadataPartitionType}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.sync.common.HoodieSyncConfig.{META_SYNC_BASE_PATH, META_SYNC_DATABASE_NAME, META_SYNC_NO_PARTITION_METADATA, META_SYNC_TABLE_NAME}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.hudi.util.JFunction
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, FromUnixTime, Literal}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{BinaryType, ByteType, DateType, DecimalType, IntegerType, ShortType, StringType, StructType, TimestampType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.scalatest.Ignore

import java.util.stream.Collectors
import scala.collection.JavaConverters

@Ignore
class TestFunctionalIndex extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    initQueryIndexConf()
  }

  test("Test Functional Index With Hive Sync Non Partitioned Table") {
    // There is a big difference between Java class loader architecture of versions 1.8 and 17.
    // Hive 2.3.7 is compiled with Java 1.8, and the class loader used there throws error when Hive APIs are run on Java 17.
    // So we special case this test only for Java 8.
    if (HoodieSparkUtils.gteqSpark3_3 && HoodieTestUtils.getJavaVersion == 8) {
      withTempDir { tmp =>
        Seq("mor").foreach { tableType =>
          val databaseName = "testdb"
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          // ts=1000 and from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-01'
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          // ts=100000 and from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-02'
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 100000)")
          // ts=10000000 and from_unixtime(ts, 'yyyy-MM-dd') = '1970-04-26'
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 10000000)")
          // create functional index
          spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
          val metaClient = createMetaClient(spark, basePath)
          assertTrue(metaClient.getIndexMetadata.isPresent)
          val functionalIndexMetadata = metaClient.getIndexMetadata.get()
          assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
          assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)

          // sync to hive without partition metadata
          val hiveSyncProps = new TypedProperties()
          hiveSyncProps.setProperty(HIVE_USER.key, "")
          hiveSyncProps.setProperty(HIVE_PASS.key, "")
          hiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key, databaseName)
          hiveSyncProps.setProperty(META_SYNC_TABLE_NAME.key, tableName)
          hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key, basePath)
          hiveSyncProps.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key, "false")
          hiveSyncProps.setProperty(META_SYNC_NO_PARTITION_METADATA.key, "true")
          HiveTestUtil.setUp(Option.of(hiveSyncProps), false)
          val tool = new HiveSyncTool(hiveSyncProps, HiveTestUtil.getHiveConf)
          tool.syncHoodieTable()

          // assert table created and no partition metadata
          val hiveClient = new HoodieHiveSyncClient(HiveTestUtil.getHiveSyncConfig, HoodieTableMetaClient.reload(metaClient))
          val roTable = tableName + "_ro"
          val rtTable = tableName + "_rt"
          assertTrue(hiveClient.tableExists(roTable))
          assertTrue(hiveClient.tableExists(rtTable))
          assertEquals(0, hiveClient.getAllPartitions(roTable).size())
          assertEquals(0, hiveClient.getAllPartitions(rtTable).size())

          // check query result
          checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-01'")(
            Seq(1, "a1")
          )

          // teardown Hive
          hiveClient.close()
          tool.close()
          HiveTestUtil.shutdown()
        }
      }
    }
  }

  test("Test Create Functional Index Syntax") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val databaseName = "default"
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          val sqlParser: ParserInterface = spark.sessionState.sqlParser
          val analyzer: Analyzer = spark.sessionState.analyzer

          var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
          var resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

          logicalPlan = sqlParser.parsePlan(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
          resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
          assertResult("idx_datestr")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
          assertResult("column_stats")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
          assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
          assertResult(Map("func" -> "from_unixtime", "format" -> "yyyy-MM-dd"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)

          logicalPlan = sqlParser.parsePlan(s"create index idx_name on $tableName using bloom_filters(name) options(func='lower')")
          resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
          assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
          assertResult("bloom_filters")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
          assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
          assertResult(Map("func" -> "lower"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)
        }
      }
    }
  }

  test("Test Create Functional Index") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val databaseName = "default"
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts',
               |  hoodie.metadata.record.index.enable = 'true',
               |  hoodie.datasource.write.recordkey.field = 'id'
               | )
               | partitioned by(ts)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

          var metaClient = createMetaClient(spark, basePath)

          assert(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))

          val sqlParser: ParserInterface = spark.sessionState.sqlParser
          val analyzer: Analyzer = spark.sessionState.analyzer

          var logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
          var resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

          var createIndexSql = s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')"
          logicalPlan = sqlParser.parsePlan(createIndexSql)

          resolvedLogicalPlan = analyzer.execute(logicalPlan)
          assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
          assertResult("idx_datestr")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
          assertResult("column_stats")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
          assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)

          spark.sql(createIndexSql)
          metaClient = createMetaClient(spark, basePath)
          assertTrue(metaClient.getIndexMetadata.isPresent)
          var functionalIndexMetadata = metaClient.getIndexMetadata.get()
          assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
          assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)

          // Verify one can create more than one functional index
          createIndexSql = s"create index name_lower on $tableName using column_stats(ts) options(func='identity')"
          spark.sql(createIndexSql)
          metaClient = createMetaClient(spark, basePath)
          functionalIndexMetadata = metaClient.getIndexMetadata.get()
          assertEquals(2, functionalIndexMetadata.getIndexDefinitions.size())
          assertEquals("func_index_name_lower", functionalIndexMetadata.getIndexDefinitions.get("func_index_name_lower").getIndexName)

          // Ensure that both the indexes are tracked correctly in metadata partition config
          val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
          assert(mdtPartitions.contains("func_index_name_lower") && mdtPartitions.contains("func_index_idx_datestr"))

          // [HUDI-7472] After creating functional index, the existing MDT partitions should still be available
          assert(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))
        }
      }
    }
  }

  test("Test Drop Functional Index") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        val databaseName = "default"
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = 'mor',
             |  preCombineField = 'ts',
             |  hoodie.metadata.record.index.enable = 'true',
             |  hoodie.datasource.write.recordkey.field = 'id'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

        var metaClient = createMetaClient(spark, basePath)

        assert(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))

        val sqlParser: ParserInterface = spark.sessionState.sqlParser
        val analyzer: Analyzer = spark.sessionState.analyzer

        val logicalPlan = sqlParser.parsePlan(s"show indexes from default.$tableName")
        val resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[ShowIndexesCommand].table, databaseName, tableName)

        // create functional index
        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
        metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getIndexMetadata.isPresent)
        var functionalIndexMetadata = metaClient.getIndexMetadata.get()
        assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
        assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)

        // Verify one can create more than one functional index
        spark.sql(s"create index name_lower on $tableName using column_stats(ts) options(func='identity')")
        metaClient = createMetaClient(spark, basePath)
        functionalIndexMetadata = metaClient.getIndexMetadata.get()
        assertEquals(2, functionalIndexMetadata.getIndexDefinitions.size())
        assertEquals("func_index_name_lower", functionalIndexMetadata.getIndexDefinitions.get("func_index_name_lower").getIndexName)

        // Ensure that both the indexes are tracked correctly in metadata partition config
        val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
        assert(mdtPartitions.contains("func_index_name_lower") && mdtPartitions.contains("func_index_idx_datestr"))

        // drop functional index
        spark.sql(s"drop index func_index_idx_datestr on $tableName")
        // validate table config
        metaClient = HoodieTableMetaClient.reload(metaClient)
        assert(!metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        // assert that the lower(name) index is still present
        assert(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_name_lower"))
      }
    }
  }

  test("Test functional index update after initialization") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir(tmp => {
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""create table $tableName (
            id int,
            name string,
            price double,
            ts long
            ) using hudi
            options (
            primaryKey ='id',
            type = 'mor',
            preCombineField = 'ts',
            hoodie.metadata.record.index.enable = 'true',
            hoodie.datasource.write.recordkey.field = 'id'
            )
            partitioned by(ts)
            location '$basePath'""".stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

        checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-01'")(
          Seq(1, "a1"),
          Seq(2, "a2"),
          Seq(3, "a3")
        )
        // create functional index
        var createIndexSql = s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')"
        spark.sql(createIndexSql)
        var metaClient = createMetaClient(spark, basePath)
        var functionalIndexMetadata = metaClient.getIndexMetadata.get()
        assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
        assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        assertTrue(metaClient.getIndexMetadata.isPresent)

        // do another insert after initializing the index
        spark.sql(s"insert into $tableName values(4, 'a4', 10, 10000000)")
        // check query result
        checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') = '1970-04-26'")(
          Seq(4, "a4")
        )

        // Verify one can create more than one functional index
        createIndexSql = s"create index name_lower on $tableName using column_stats(ts) options(func='identity')"
        spark.sql(createIndexSql)
        metaClient = createMetaClient(spark, basePath)
        functionalIndexMetadata = metaClient.getIndexMetadata.get()
        assertEquals(2, functionalIndexMetadata.getIndexDefinitions.size())
        assertEquals("func_index_name_lower", functionalIndexMetadata.getIndexDefinitions.get("func_index_name_lower").getIndexName)

        // Ensure that both the indexes are tracked correctly in metadata partition config
        val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
        assertTrue(mdtPartitions.contains("func_index_name_lower") && mdtPartitions.contains("func_index_idx_datestr"))
      })
    }
  }

  test("Test Create Functional Index With Data Skipping") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        Seq("cow").foreach { tableType =>
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql("set hoodie.metadata.enable=true")
          spark.sql("set hoodie.enable.data.skipping=true")
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  ts long,
               |  price int
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | partitioned by(price)
               | location '$basePath'
       """.stripMargin)
          spark.sql(s"insert into $tableName (id, name, ts, price) values(1, 'a1', 1000, 10)")
          spark.sql(s"insert into $tableName (id, name, ts, price) values(2, 'a2', 200000, 100)")
          spark.sql(s"insert into $tableName (id, name, ts, price) values(3, 'a3', 2000000000, 1000)")
          // create functional index
          spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
          // validate index created successfully
          val metaClient = createMetaClient(spark, basePath)
          assertTrue(metaClient.getIndexMetadata.isPresent)
          val functionalIndexMetadata = metaClient.getIndexMetadata.get()
          assertEquals(1, functionalIndexMetadata.getIndexDefinitions.size())
          assertEquals("func_index_idx_datestr", functionalIndexMetadata.getIndexDefinitions.get("func_index_idx_datestr").getIndexName)

          checkAnswer(s"select id, name, price, ts, from_unixtime(ts, 'yyyy-MM-dd') from $tableName where from_unixtime(ts, 'yyyy-MM-dd') < '1970-01-03'")(
            Seq(1, "a1", 10, 1000, "1970-01-01")
          )
        }
      }
    }
  }

  test("Test Functional Index File-level Stats Update") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        // create a simple partitioned mor table and insert some records
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  price double,
             |  ts long,
             |  name string
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = 'mor',
             |  preCombineField = 'ts'
             | )
             | partitioned by(name)
             | location '$basePath'
       """.stripMargin)
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
        spark.sql(s"insert into $tableName values(1, 10, 1601098924, 'a1')")
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
        spark.sql(s"insert into $tableName values(2, 10, 1632634924, 'a1')")
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
        spark.sql(s"insert into $tableName values(3, 10, 1664170924, 'a2')")
        // create functional index and verify
        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
        val metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        assertTrue(metaClient.getIndexMetadata.isPresent)
        assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

        // verify functional index records by querying metadata table
        val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') where type=3"
        checkAnswer(metadataSql)(
          Seq("2020-09-26", "2021-09-26"), // for file in name=a1
          Seq("2022-09-26", "2022-09-26") // for file in name
        )

        // do another insert after initializing the index
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2024-09-26
        spark.sql(s"insert into $tableName values(5, 10, 1727329324, 'a3')")
        // check query result for predicates including values when functional index was disabled
        checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') IN ('2024-09-26', '2022-09-26')")(
          Seq(3, "a2"),
          Seq(5, "a3")
        )
        // verify there are new updates to functional index
        checkAnswer(metadataSql)(
          Seq("2020-09-26", "2021-09-26"),
          Seq("2022-09-26", "2022-09-26"),
          Seq("2024-09-26", "2024-09-26") // for file in name=a3
        )
      }
    }
  }

  test("Test Multiple Functional Index Update") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        // create a simple partitioned mor table and insert some records
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  price double,
             |  ts long,
             |  name string
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = 'mor',
             |  preCombineField = 'ts'
             | )
             | partitioned by(name)
             | location '$basePath'
       """.stripMargin)
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
        spark.sql(s"insert into $tableName values(1, 10, 1601098924, 'a1')")
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
        spark.sql(s"insert into $tableName values(2, 10, 1632634924, 'a1')")
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
        spark.sql(s"insert into $tableName values(3, 10, 1664170924, 'a2')")
        // create functional index and verify
        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
        var metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        assertTrue(metaClient.getIndexMetadata.isPresent)
        assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

        // create functional index and verify
        spark.sql(s"create index idx_price on $tableName using column_stats(price) options(func='identity')")
        metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_price"))
        assertEquals(2, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

        // verify functional index records by querying metadata table
        val metadataSql = s"select ColumnStatsMetadata.columnName, ColumnStatsMetadata.minValue.member4.value, ColumnStatsMetadata.maxValue.member4.value, " +
          s"ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value, ColumnStatsMetadata.isDeleted from hudi_metadata('$tableName') where type=3"
        checkAnswer(metadataSql)(
          Seq("ts", null, null, "2020-09-26", "2021-09-26", false), // for file in name=a1
          Seq("ts", null, null, "2022-09-26", "2022-09-26", false), // for file in name=a2
          Seq("price", 10.0, 10.0, null, null, false), // for file in name=a1
          Seq("price", 10.0, 10.0, null, null, false) // for file in name=a2
        )

        // do an update after initializing the index
        // set price as 5.0 for id=1
        spark.sql(s"update $tableName set price = 5.0 where id = 1")

        // check query result for predicates including both the functional index columns
        checkAnswer(s"select id, price from $tableName where price <= 8")(
          Seq(1, 5.0)
        )
        checkAnswer(s"select id, price from $tableName where price > 8")(
          Seq(2, 10.0),
          Seq(3, 10.0)
        )
        checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') >= '2022-09-26'")(
          Seq(3, "a2")
        )

        // verify there are new updates to functional index
        checkAnswer(metadataSql)(
          Seq("ts", null, null, "2020-09-26", "2021-09-26", false), // for file in name=a1
          Seq("ts", null, null, "2020-09-26", "2020-09-26", false), // for update of id=1
          Seq("ts", null, null, "2022-09-26", "2022-09-26", false), // for file in name=a2
          Seq("price", 10.0, 10.0, null, null, false), // for file in name=a1
          Seq("price", 5.0, 5.0, null, null, false), // for update of id=1
          Seq("price", 10.0, 10.0, null, null, false) // for file in name=a2
        )
      }
    }
  }

  test("Test Enable and Disable Functional Index") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        // create a simple partitioned mor table and insert some records
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  price double,
             |  ts long,
             |  name string
             |) using hudi
             | options (
             |  primaryKey ='id',
             |  type = 'mor',
             |  preCombineField = 'ts'
             | )
             | partitioned by(name)
             | location '$basePath'
       """.stripMargin)
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
        spark.sql(s"insert into $tableName values(1, 10, 1601098924, 'a1')")
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
        spark.sql(s"insert into $tableName values(2, 10, 1632634924, 'a1')")
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
        spark.sql(s"insert into $tableName values(3, 10, 1664170924, 'a2')")
        // create functional index and verify
        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
        val metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        assertTrue(metaClient.getIndexMetadata.isPresent)
        assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

        // verify functional index records by querying metadata table
        val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') where type=3"
        checkAnswer(metadataSql)(
          Seq("2020-09-26", "2021-09-26"), // for file in name=a1
          Seq("2022-09-26", "2022-09-26") // for file in name=a2
        )

        // disable functional index
        spark.sql(s"set ${HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP.key}=false")
        // do another insert after disabling the index
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
        spark.sql(s"insert into $tableName values(4, 10, 1664170924, 'a2')")
        // check query result
        checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') = '2022-09-26'")(
          Seq(3, "a2"),
          Seq(4, "a2")
        )

        // enable functional index
        spark.sql(s"set ${HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP.key}=true")
        // do another insert after initializing the index
        // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2024-09-26
        spark.sql(s"insert into $tableName values(5, 10, 1727329324, 'a3')")
        // check query result for predicates including values when functional index was disabled
        checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') IN ('2024-09-26', '2022-09-26')")(
          Seq(3, "a2"),
          Seq(4, "a2"),
          Seq(5, "a3")
        )
        // verify there are new updates to functional index
        checkAnswer(metadataSql)(
          Seq("2020-09-26", "2021-09-26"),
          Seq("2022-09-26", "2022-09-26"),
          Seq("2022-09-26", "2022-09-26"), // for file in name=a2
          Seq("2024-09-26", "2024-09-26") // for file in name=a3
        )
      }
    }
  }

  // Test functional index using column stats and bloom filters, and then clean older version, and check index is correct.
  test("Test Functional Index With Cleaning") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          Seq(true, false).foreach { isPartitioned =>
            val tableName = generateTableName + s"_clean_$tableType$isPartitioned"
            val partitionByClause = if (isPartitioned) "partitioned by(price)" else ""
            val basePath = s"${tmp.getCanonicalPath}/$tableName"
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  ts long,
                 |  price int
                 |) using hudi
                 | options (
                 |  primaryKey ='id',
                 |  type = '$tableType',
                 |  preCombineField = 'ts',
                 |  hoodie.clean.policy = 'KEEP_LATEST_COMMITS',
                 |  ${HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key} = '1'
                 | )
                 | $partitionByClause
                 | location '$basePath'
       """.stripMargin)
            if (tableType == "mor") {
              spark.sql("set hoodie.compact.inline=true")
              spark.sql("set hoodie.compact.inline.max.delta.commits=2")
            }
            if (!isPartitioned) {
              // setting this for non-partitioned table to ensure multiple file groups are created
              spark.sql(s"set ${HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key()}=0")
            }
            // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
            spark.sql(s"insert into $tableName values(1, 'a1', 1601098924, 10)")
            // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
            spark.sql(s"insert into $tableName values(2, 'a2', 1632634924, 100)")
            // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
            spark.sql(s"insert into $tableName values(3, 'a3', 1664170924, 1000)")
            // create functional index
            spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
            // validate index created successfully
            val metaClient = createMetaClient(spark, basePath)
            assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
            assertTrue(metaClient.getIndexMetadata.isPresent)
            assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

            // verify functional index records by querying metadata table
            val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') where type=3"
            checkAnswer(metadataSql)(
              Seq("2020-09-26", "2020-09-26"), // for file in price=10
              Seq("2021-09-26", "2021-09-26"), // for file in price=100
              Seq("2022-09-26", "2022-09-26") // for file in price=1000
            )

            // get file name for price=1000
            val fileNames = spark.sql(s"select ColumnStatsMetadata.fileName from hudi_metadata('$tableName') where type=3 and ColumnStatsMetadata.minValue.member6.value='2022-09-26'").collect()
            assertEquals(1, fileNames.length)
            val fileName = fileNames(0).getString(0)

            // update the record with id=3
            // produce two versions so that the older version can be cleaned
            spark.sql(s"update $tableName set ts=1695706924 where id=3")
            spark.sql(s"update $tableName set ts=1727329324 where id=3")

            // check cleaning completed
            val lastCleanInstant = metaClient.reloadActiveTimeline().getCleanerTimeline.lastInstant()
            assertTrue(lastCleanInstant.isPresent)
            // verify that file for price=1000 is cleaned
            assertTrue(HoodieTestUtils.getCleanedFiles(metaClient, lastCleanInstant.get()).get(0).getValue.equals(fileName))

            // verify there are new updates to functional index with isDeleted true for cleaned file
            checkAnswer(s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value, ColumnStatsMetadata.isDeleted from hudi_metadata('$tableName') where type=3 and ColumnStatsMetadata.fileName='$fileName'")(
              Seq("2022-09-26", "2022-09-26", false) // for cleaned file, there won't be any stats produced.
            )
          }
        }
      }
    }
  }

  private def assertTableIdentifier(catalogTable: CatalogTable,
                                    expectedDatabaseName: String,
                                    expectedTableName: String): Unit = {
    assertResult(Some(expectedDatabaseName))(catalogTable.identifier.database)
    assertResult(expectedTableName)(catalogTable.identifier.table)
  }

  test("Test Functional Index Insert after Initialization") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val isPartitioned = true
          val tableName = generateTableName + s"_init_$tableType$isPartitioned"
          val partitionByClause = if (isPartitioned) "partitioned by(price)" else ""
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  ts long,
               |  price int
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | $partitionByClause
               | location '$basePath'
       """.stripMargin)

          writeRecordsAndValidateFunctionalIndex(tableName, basePath, "update", isDelete = false, shouldCompact = false, shouldCluster = false, shouldRollback = false)
        }
      }
    }
  }

  test("Test Functional Index Rollback") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val isPartitioned = true
          val tableName = generateTableName + s"_rollback_$tableType$isPartitioned"
          val partitionByClause = if (isPartitioned) "partitioned by(price)" else ""
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  ts long,
               |  price int
               |) using hudi
               | options (
               |  primaryKey ='id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
               | $partitionByClause
               | location '$basePath'
       """.stripMargin)

          writeRecordsAndValidateFunctionalIndex(tableName, basePath, "update", isDelete = false, shouldCompact = false, shouldCluster = false, shouldRollback = true)
        }
      }
    }
  }

  /**
   * Write records to the table with the given operation type and do updates or deletes, and then validate functional index.
   */
  private def writeRecordsAndValidateFunctionalIndex(tableName: String,
                                                     basePath: String,
                                                     operationType: String,
                                                     isDelete: Boolean,
                                                     shouldCompact: Boolean,
                                                     shouldCluster: Boolean,
                                                     shouldRollback: Boolean,
                                                     shouldValidate: Boolean = true): Unit = {
    // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
    spark.sql(s"insert into $tableName values(1, 'a1', 1601098924, 10)")
    // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
    spark.sql(s"insert into $tableName values(2, 'a2', 1632634924, 100)")
    // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
    spark.sql(s"insert into $tableName values(3, 'a3', 1664170924, 1000)")
    // create functional index
    spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(func='from_unixtime', format='yyyy-MM-dd')")
    val metaClient = createMetaClient(spark, basePath)
    // verify file pruning with filter on from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
    val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")
    val dataFilter = {
      val tsColumn = UnresolvedAttribute("ts")

      // Define the format "yyyy-MM-dd" as a literal
      val format = Literal("yyyy-MM-dd")

      // Create the from_unixtime(ts, 'yyyy-MM-dd') expression
      val fromUnixTimeExpr = FromUnixTime(tsColumn, format)

      // Define the date to compare against as a literal
      val targetDate = Literal("2024-03-26")

      // Create the equality expression from_unixtime(ts, 'yyyy-MM-dd') = '2024-03-26'
      EqualTo(fromUnixTimeExpr, targetDate)
    }
    verifyFilePruning(opts, dataFilter, metaClient)

    // do the operation
    if (isDelete) {
      spark.sql(s"delete from $tableName where id=1")
    } else {
      spark.sql(s"insert into $tableName values(4, 'a4', 1727329324, 10000)")
    }

    // validate the functional index
    val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value, ColumnStatsMetadata.isDeleted from hudi_metadata('$tableName') where type=3"
    // validate the functional index
    checkAnswer(metadataSql)(
      Seq("2020-09-26", "2020-09-26", false),
      Seq("2021-09-26", "2021-09-26", false),
      Seq("2022-09-26", "2022-09-26", false),
      Seq("2024-09-26", "2024-09-26", false)
    )

    if (shouldRollback) {
      // rollback the operation
      val lastCompletedInstant = metaClient.reloadActiveTimeline().getCommitsTimeline.filterCompletedInstants().lastInstant()
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)), getWriteConfig(Map.empty, metaClient.getBasePath.toString))
      writeClient.rollback(lastCompletedInstant.get().getTimestamp)
      // validate the functional index
      checkAnswer(metadataSql)(
        // the last commit is rolledback so no records for that
        Seq("2020-09-26", "2020-09-26", false),
        Seq("2021-09-26", "2021-09-26", false),
        Seq("2022-09-26", "2022-09-26", false)
      )
    }
  }

  test("testFunctionalIndexUsingColumnStatsWithPartitionAndFilesFilter") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        val metadataOpts = Map(
          HoodieMetadataConfig.ENABLE.key -> "true",
          HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP.key -> "true"
        )
        val opts = Map(
          "hoodie.insert.shuffle.parallelism" -> "4",
          "hoodie.upsert.shuffle.parallelism" -> "4",
          HoodieWriteConfig.TBL_NAME.key -> tableName,
          RECORDKEY_FIELD.key -> "c1",
          PRECOMBINE_FIELD.key -> "c1",
          PARTITIONPATH_FIELD.key() -> "c8"
        )
        val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json-partition-pruning").toString

        // NOTE: Schema here is provided for validation that the input date is in the appropriate format
        val sourceTableSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
          .add("c3", DecimalType(9, 3))
          .add("c4", TimestampType)
          .add("c5", ShortType)
          .add("c6", DateType)
          .add("c7", BinaryType)
          .add("c8", ByteType)
        val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)
        inputDF
          .sort("c1")
          .repartition(4, new Column("c1"))
          .write
          .format("hudi")
          .options(opts)
          .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
          .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Overwrite)
          .save(basePath)
        // Create a functional index on column c6
        spark.sql(s"create table $tableName using hudi location '$basePath'")
        spark.sql(s"create index idx_datestr on $tableName using column_stats(c6) options(func='identity')")
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        assertTrue(metaClient.getIndexMetadata.isPresent)
        assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

        // check functional index records
        val metadataConfig = HoodieMetadataConfig.newBuilder()
          .fromProperties(toProperties(metadataOpts))
          .build()
        val functionalIndexSupport = new FunctionalIndexSupport(spark, metadataConfig, metaClient)
        val prunedPartitions = Set("9")
        var indexDf = functionalIndexSupport.loadFunctionalIndexDataFrame("func_index_idx_datestr", prunedPartitions, shouldReadInMemory = true)
        // check only one record returned corresponding to the pruned partition
        assertTrue(indexDf.count() == 1)
        // select fileName from indexDf
        val fileName = indexDf.select("fileName").collect().map(_.getString(0)).head
        val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(spark.sparkContext), metaClient,
          HoodieMetadataConfig.newBuilder().enable(false).build())
        fsv.loadAllPartitions()
        val partitionPaths = fsv.getPartitionPaths
        val partitionToBaseFiles: java.util.Map[String, java.util.List[StoragePath]] = new java.util.HashMap[String, java.util.List[StoragePath]]
        // select file names for each partition from file system view
        partitionPaths.forEach(partitionPath =>
          partitionToBaseFiles.put(partitionPath.getName, fsv.getLatestBaseFiles(partitionPath.getName)
            .map[StoragePath](baseFile => baseFile.getStoragePath).collect(Collectors.toList[StoragePath]))
        )
        fsv.close()
        val expectedFileNames = partitionToBaseFiles.get(prunedPartitions.head).stream().map[String](baseFile => baseFile.getName).collect(Collectors.toSet[String])
        assertTrue(expectedFileNames.size() == 1)
        // verify the file names match
        assertTrue(expectedFileNames.contains(fileName))

        // check more records returned if no partition filter provided
        indexDf = functionalIndexSupport.loadFunctionalIndexDataFrame("func_index_idx_datestr", Set(), shouldReadInMemory = true)
        assertTrue(indexDf.count() > 1)
      }
    }
  }

  test("testComputeCandidateFileNames") {
    if (HoodieSparkUtils.gteqSpark3_3) {
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        // in this test, we will create a table with inserts going to log file so that there is a file slice with only log file and no base file
        val metadataOpts = Map(
          HoodieMetadataConfig.ENABLE.key -> "true",
          HoodieMetadataConfig.FUNCTIONAL_INDEX_ENABLE_PROP.key -> "true"
        )
        val opts = Map(
          "hoodie.insert.shuffle.parallelism" -> "4",
          "hoodie.upsert.shuffle.parallelism" -> "4",
          HoodieWriteConfig.TBL_NAME.key -> tableName,
          TABLE_TYPE.key -> "MERGE_ON_READ",
          RECORDKEY_FIELD.key -> "c1",
          PRECOMBINE_FIELD.key -> "c1",
          PARTITIONPATH_FIELD.key() -> "c8",
          // setting IndexType to be INMEMORY to simulate Global Index nature
          HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.INMEMORY.name()
        )
        val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json-partition-pruning").toString

        // NOTE: Schema here is provided for validation that the input date is in the appropriate format
        val sourceTableSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
          .add("c3", DecimalType(9, 3))
          .add("c4", TimestampType)
          .add("c5", ShortType)
          .add("c6", DateType)
          .add("c7", BinaryType)
          .add("c8", ByteType)
        val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)
        inputDF
          .sort("c1")
          .repartition(4, new Column("c1"))
          .write
          .format("hudi")
          .options(opts)
          .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
          .option(OPERATION.key, INSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Overwrite)
          .save(basePath)
        // Create a functional index on column c6
        spark.sql(s"create table $tableName using hudi location '$basePath'")
        spark.sql(s"create index idx_datestr on $tableName using column_stats(c6) options(func='identity')")
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("func_index_idx_datestr"))
        assertTrue(metaClient.getIndexMetadata.isPresent)
        assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())
        // check functional index records
        val metadataConfig = HoodieMetadataConfig.newBuilder()
          .fromProperties(toProperties(metadataOpts))
          .build()
        val fileIndex = new HoodieFileIndex(spark, metaClient, None,
          opts ++ metadataOpts ++ Map("glob.paths" -> s"$basePath/9", DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"), includeLogFiles = true)
        val functionalIndexSupport = new FunctionalIndexSupport(spark, metadataConfig, metaClient)
        val partitionFilter: Expression = EqualTo(AttributeReference("c8", IntegerType)(), Literal(9))
        val (isPruned, prunedPaths) = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq(partitionFilter))
        assertTrue(isPruned)
        val prunedPartitionAndFileNames = functionalIndexSupport.getPrunedPartitionsAndFileNames(prunedPaths, includeLogFiles = true)
        assertTrue(prunedPartitionAndFileNames._1.size == 1) // partition
        assertTrue(prunedPartitionAndFileNames._2.size == 1) // log file
        assertTrue(FSUtils.isLogFile(prunedPartitionAndFileNames._2.head))
      }
    }
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression, metaClient: HoodieTableMetaClient, isDataSkippingExpected: Boolean = false, isNoScanExpected: Boolean = false): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> metaClient.getBasePath.toString)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    try {
      val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
      val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
      val latestDataFilesCount = getLatestDataFilesCount(metaClient = metaClient)
      if (isDataSkippingExpected) {
        assertTrue(filteredFilesCount < latestDataFilesCount)
        if (isNoScanExpected) {
          assertTrue(filteredFilesCount == 0)
        } else {
          assertTrue(filteredFilesCount > 0)
        }
      } else {
        assertTrue(filteredFilesCount == latestDataFilesCount)
      }

      // with no data skipping
      fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
      val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
      assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
    } finally {
      fileIndex.close()
    }
  }

  private def getLatestDataFilesCount(includeLogFiles: Boolean = true, metaClient: HoodieTableMetaClient) = {
    var totalLatestDataFiles = 0L
    val fsView: HoodieMetadataFileSystemView = getTableFileSystemView(metaClient)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    } finally {
      fsView.close()
    }
    totalLatestDataFiles
  }

  private def getTableFileSystemView(metaClient: HoodieTableMetaClient): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(
      new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)),
      metaClient,
      metaClient.getActiveTimeline,
      HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexPartitionStats(true).build())
  }

  private def getWriteConfig(hudiOpts: Map[String, String], basePath: String): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }
}
