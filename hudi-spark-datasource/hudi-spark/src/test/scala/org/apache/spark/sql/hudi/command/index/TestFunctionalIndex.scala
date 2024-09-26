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

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.hive.HiveSyncConfigHolder._
import org.apache.hudi.hive.testutils.HiveTestUtil
import org.apache.hudi.hive.{HiveSyncTool, HoodieHiveSyncClient}
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.sync.common.HoodieSyncConfig.{META_SYNC_BASE_PATH, META_SYNC_DATABASE_NAME, META_SYNC_NO_PARTITION_METADATA, META_SYNC_TABLE_NAME}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.scalatest.Ignore

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
          val hiveClient = new HoodieHiveSyncClient(HiveTestUtil.getHiveSyncConfig, metaClient)
          assertTrue(hiveClient.tableExists("h0_ro"))
          assertTrue(hiveClient.tableExists("h0_rt"))
          assertEquals(0, hiveClient.getAllPartitions("h0_ro").size())
          assertEquals(0, hiveClient.getAllPartitions("h0_rt").size())

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
        // verify there are no new updates to functional index
        checkAnswer(metadataSql)(
          Seq("2020-09-26", "2021-09-26"),
          Seq("2022-09-26", "2022-09-26")
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
          Seq("2024-09-26", "2024-09-26") // for file in name=a3
        )
      }
    }
  }

  private def assertTableIdentifier(catalogTable: CatalogTable,
                                    expectedDatabaseName: String,
                                    expectedTableName: String): Unit = {
    assertResult(Some(expectedDatabaseName))(catalogTable.identifier.database)
    assertResult(expectedTableName)(catalogTable.identifier.table)
  }
}
