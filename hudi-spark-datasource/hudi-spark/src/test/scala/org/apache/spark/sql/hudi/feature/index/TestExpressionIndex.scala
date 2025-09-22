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

package org.apache.spark.sql.hudi.feature.index

import org.apache.hudi.{DataSourceReadOptions, ExpressionIndexSupport, HoodieFileIndex, HoodieSparkUtils, SparkAdapterSupport}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.avro.model.HoodieMetadataBloomFilter
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.utils.SparkMetadataWriterUtils
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.view.{FileSystemViewManager, HoodieTableFileSystemView}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{HiveSyncTool, HoodieHiveSyncClient}
import org.apache.hudi.hive.testutils.HiveTestUtil
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.index.expression.HoodieExpressionIndex
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieIndexVersion, HoodieMetadataPayload, MetadataPartitionType}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.sync.common.HoodieSyncConfig.{META_SYNC_BASE_PATH, META_SYNC_DATABASE_NAME, META_SYNC_NO_PARTITION_METADATA, META_SYNC_TABLE_NAME}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.hudi.util.JFunction

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{functions, Column, SaveMode}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.resolveExpr
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, FromUnixTime, Literal, Upper}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import java.util.stream.Collectors

import scala.collection.JavaConverters

class TestExpressionIndex extends HoodieSparkSqlTestBase with SparkAdapterSupport {

  override protected def beforeAll(): Unit = {
    spark.sql("set hoodie.metadata.index.column.stats.enable=false")
    spark.sparkContext.persistentRdds.foreach(rdd => rdd._2.unpersist())
    initQueryIndexConf()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  test("Test Expression Index With Hive Sync Non Partitioned External Table") {
    // There is a big difference between Java class loader architecture of versions 1.8 and 17.
    // Hive 2.3.7 is compiled with Java 1.8, and the class loader used there throws error when Hive APIs are run on Java 17.
    // So we special case this test only for Java 8.
    if (HoodieTestUtils.getJavaVersion == 8) {
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

          spark.sql(s"""DROP TABLE if exists $tableName""")
          // Use the same base path as above
          spark.sql(s"""CREATE TABLE $tableName USING hudi LOCATION '$basePath'""")
          // create expression index
          spark.sql(s"""create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd HH:mm')""")
          // ts=100000 and from_unixtime(ts, 'yyyy-MM-dd') = '1970-01-02'
          spark.sql(s"insert into $tableName values(2, 'a2', 10, 100000)")
          // ts=10000000 and from_unixtime(ts, 'yyyy-MM-dd') = '1970-04-26'
          spark.sql(s"insert into $tableName values(3, 'a3', 10, 10000000)")
          val metaClient = createMetaClient(spark, basePath)
          assertTrue(metaClient.getIndexMetadata.isPresent)
          val expressionIndexMetadata = metaClient.getIndexMetadata.get()
          assertEquals(1, expressionIndexMetadata.getIndexDefinitions.size())
          assertEquals("expr_index_idx_datestr", expressionIndexMetadata.getIndexDefinitions.get("expr_index_idx_datestr").getIndexName)

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

          spark.sql(s"""DROP TABLE if exists $tableName""")
        }
      }
    }
  }

  test("Test Create Expression Index Syntax") {
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

        logicalPlan = sqlParser.parsePlan(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_datestr")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("column_stats")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
        assertResult(Map(HoodieExpressionIndex.EXPRESSION_OPTION -> "from_unixtime", "format" -> "yyyy-MM-dd"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)

        logicalPlan = sqlParser.parsePlan(s"create index idx_name on $tableName using bloom_filters(name) options(expr='lower')")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("bloom_filters")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
        assertResult(Map(HoodieExpressionIndex.EXPRESSION_OPTION -> "lower"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)
      }
    }
  }

  test("Test Create Expression Index") {
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

        var createIndexSql = s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')"
        logicalPlan = sqlParser.parsePlan(createIndexSql)

        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_datestr")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("column_stats")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)

        spark.sql(createIndexSql)
        metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getIndexMetadata.isPresent)
        var expressionIndexMetadata = metaClient.getIndexMetadata.get()
        // RLI and expression index
        assertEquals(2, expressionIndexMetadata.getIndexDefinitions.size())
        assertEquals("expr_index_idx_datestr", expressionIndexMetadata.getIndexDefinitions.get("expr_index_idx_datestr").getIndexName)

        // Verify one can create more than one expression index. When function is not provided,
        // default identity function is used
        createIndexSql = s"create index name_lower on $tableName using column_stat(ts)"
        checkException(createIndexSql)("column_stat is not supported")
        createIndexSql = s"create index name_lower on $tableName using column_stats(ts) options(expr='random')"
        checkNestedException(createIndexSql) ("Unsupported Spark function: random")
        createIndexSql = s"create index name_lower on $tableName using column_stats(ts)"
        checkException(createIndexSql) ("Column stats index without expression on any column can be created using datasource configs. " +
          "Please refer https://hudi.apache.org/docs/metadata for more info")
        createIndexSql = s"create index name_lower on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')"
        spark.sql(createIndexSql)
        metaClient = createMetaClient(spark, basePath)
        expressionIndexMetadata = metaClient.getIndexMetadata.get()
        // RLI and 2 expression indexes
        assertEquals(3, expressionIndexMetadata.getIndexDefinitions.size())
        assertEquals("expr_index_name_lower", expressionIndexMetadata.getIndexDefinitions.get("expr_index_name_lower").getIndexName)

        // Ensure that both the indexes are tracked correctly in metadata partition config
        val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
        assert(mdtPartitions.contains("expr_index_name_lower") && mdtPartitions.contains("expr_index_idx_datestr"))

        // [HUDI-7472] After creating expression index, the existing MDT partitions should still be available
        assert(metaClient.getTableConfig.isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX))
      }
    }
  }

  test("Test Drop Expression Index") {
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

      // create expression index
      spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
      metaClient = createMetaClient(spark, basePath)
      assertTrue(metaClient.getIndexMetadata.isPresent)
      var expressionIndexMetadata = metaClient.getIndexMetadata.get()
      // RLI and expression index
      assertEquals(2, expressionIndexMetadata.getIndexDefinitions.size())
      assertEquals("expr_index_idx_datestr", expressionIndexMetadata.getIndexDefinitions.get("expr_index_idx_datestr").getIndexName)

      // Verify one can create more than one expression index
      spark.sql(s"create index name_lower on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy')")
      metaClient = createMetaClient(spark, basePath)
      expressionIndexMetadata = metaClient.getIndexMetadata.get()
      // RLI and 2 expression indexes
      assertEquals(3, expressionIndexMetadata.getIndexDefinitions.size())
      assertEquals("expr_index_name_lower", expressionIndexMetadata.getIndexDefinitions.get("expr_index_name_lower").getIndexName)

      // Ensure that both the indexes are tracked correctly in metadata partition config
      val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
      assert(mdtPartitions.contains("expr_index_name_lower") && mdtPartitions.contains("expr_index_idx_datestr"))

      // drop expression index
      spark.sql(s"drop index idx_datestr on $tableName")
      // validate table config
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(!metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      // assert that the lower(name) index is still present
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_name_lower"))
    }
  }

  test("Test expression index update after initialization") {
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
      // create expression index
      var createIndexSql = s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')"
      spark.sql(createIndexSql)
      var metaClient = createMetaClient(spark, basePath)
      var expressionIndexMetadata = metaClient.getIndexMetadata.get()
      // RLI and expression indexes
      assertEquals(2, expressionIndexMetadata.getIndexDefinitions.size())
      assertEquals("expr_index_idx_datestr", expressionIndexMetadata.getIndexDefinitions.get("expr_index_idx_datestr").getIndexName)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      assertTrue(metaClient.getIndexMetadata.isPresent)

      // do another insert after initializing the index
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 10000000)")
      // check query result
      checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') = '1970-04-26'")(
        Seq(4, "a4")
      )

      // Verify one can create more than one expression index
      createIndexSql = s"create index name_lower on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy')"
      spark.sql(createIndexSql)
      metaClient = createMetaClient(spark, basePath)
      expressionIndexMetadata = metaClient.getIndexMetadata.get()
      // RLI and 2 expression indexes
      assertEquals(3, expressionIndexMetadata.getIndexDefinitions.size())
      assertEquals("expr_index_name_lower", expressionIndexMetadata.getIndexDefinitions.get("expr_index_name_lower").getIndexName)

      // Ensure that both the indexes are tracked correctly in metadata partition config
      val mdtPartitions = metaClient.getTableConfig.getMetadataPartitions
      assertTrue(mdtPartitions.contains("expr_index_name_lower") && mdtPartitions.contains("expr_index_idx_datestr"))
    })
  }

  test("Test Create Expression Index With Data Skipping") {
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
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }
        spark.sql(s"insert into $tableName (id, name, ts, price) values(1, 'a1', 1000, 10)")
        spark.sql(s"insert into $tableName (id, name, ts, price) values(2, 'a2', 200000, 100)")
        spark.sql(s"insert into $tableName (id, name, ts, price) values(3, 'a3', 2000000000, 1000)")
        // create expression index
        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
        // validate index created successfully
        val metaClient = createMetaClient(spark, basePath)
        assertTrue(metaClient.getIndexMetadata.isPresent)
        val expressionIndexMetadata = metaClient.getIndexMetadata.get()
        assertEquals(1, expressionIndexMetadata.getIndexDefinitions.size())
        assertEquals("expr_index_idx_datestr", expressionIndexMetadata.getIndexDefinitions.get("expr_index_idx_datestr").getIndexName)

        checkAnswer(s"select id, name, price, ts, from_unixtime(ts, 'yyyy-MM-dd') from $tableName where from_unixtime(ts, 'yyyy-MM-dd') < '1970-01-03'")(
          Seq(1, "a1", 10, 1000, "1970-01-01")
        )
      }
    }
  }

  test("Test Expression Index File-level Stats Update") {
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
      // create expression index and verify
      spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
      val metaClient = createMetaClient(spark, basePath)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      assertTrue(metaClient.getIndexMetadata.isPresent)
      assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

      // verify expression index records by querying metadata table
      val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') where type=3"
      checkAnswer(metadataSql)(
        Seq("2020-09-26", "2021-09-26"), // for file in name=a1
        Seq("2022-09-26", "2022-09-26") // for file in name
      )

      // do another insert after initializing the index
      // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2024-09-26
      spark.sql(s"insert into $tableName values(5, 10, 1727329324, 'a3')")
      // check query result for predicates including values when expression index was disabled
      checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') IN ('2024-09-26', '2022-09-26')")(
        Seq(3, "a2"),
        Seq(5, "a3")
      )
      // verify there are new updates to expression index
      checkAnswer(metadataSql)(
        Seq("2020-09-26", "2021-09-26"),
        Seq("2022-09-26", "2022-09-26"),
        Seq("2024-09-26", "2024-09-26") // for file in name=a3
      )
    }
  }

  test("Test Multiple Expression Index Update") {
    withTempDir { tmp =>
      // create a simple partitioned mor table and insert some records
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  ts long,
           |  price string
           |) using hudi
           | options (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts'
           | )
           | partitioned by(price)
           | location '$basePath'
       """.stripMargin)
      // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
      spark.sql(s"insert into $tableName values(1, 'a1', 1601098924, '10')")
      // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
      spark.sql(s"insert into $tableName values(2, 'a1', 1632634924, '10')")
      // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
      spark.sql(s"insert into $tableName values(3, 'a2', 1664170924, '20')")
      // create expression index and verify
      spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
      var metaClient = createMetaClient(spark, basePath)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      assertTrue(metaClient.getIndexMetadata.isPresent)
      assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

      // create expression index and verify
      spark.sql(s"create index idx_name on $tableName using column_stats(name) options(expr='upper')")
      metaClient = createMetaClient(spark, basePath)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_name"))
      assertEquals(2, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

      // verify expression index records by querying metadata table
      val metadataSql = s"select ColumnStatsMetadata.columnName, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value, " +
        s"ColumnStatsMetadata.isDeleted from hudi_metadata('$tableName') where type=3"
      checkAnswer(metadataSql)(
        Seq("ts", "2020-09-26", "2021-09-26", false), // for file in name=a1
        Seq("ts", "2022-09-26", "2022-09-26", false), // for file in name=a2
        Seq("name", "A1", "A1", false), // for file in name=a1
        Seq("name", "A2", "A2", false) // for file in name=a2
      )

      // do an update after initializing the index
      // set name as a3 for id=1
      spark.sql(s"update $tableName set name = 'a3' where id = 1")

      // check query result for predicates including both the expression index columns
      checkAnswer(s"select id, name from $tableName where upper(name) = 'A3'")(
        Seq(1, "a3")
      )
      checkAnswer(s"select id, name from $tableName where upper(name) < 'A3'")(
        Seq(2, "a1"),
        Seq(3, "a2")
      )
      checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') >= '2022-09-26'")(
        Seq(3, "a2")
      )

      // verify there are new updates to expression index
      checkAnswer(metadataSql)(
        Seq("ts", "2020-09-26", "2021-09-26", false), // for file in name=a1
        Seq("ts", "2020-09-26", "2020-09-26", false), // for update of id=1
        Seq("ts", "2022-09-26", "2022-09-26", false), // for file in name=a2
        Seq("name", "A1", "A1", false), // for file in name=a1
        Seq("name", "A3", "A3", false), // for update of id=1
        Seq("name", "A2", "A2", false) // for file in name=a2
      )
    }
  }

  test("Test Enable and Disable Expression Index") {
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
      // create expression index and verify
      spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
      val metaClient = createMetaClient(spark, basePath)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      assertTrue(metaClient.getIndexMetadata.isPresent)
      assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

      // verify expression index records by querying metadata table
      val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') where type=3"
      checkAnswer(metadataSql)(
        Seq("2020-09-26", "2021-09-26"), // for file in name=a1
        Seq("2022-09-26", "2022-09-26") // for file in name=a2
      )

      // disable expression index
      spark.sql(s"set ${HoodieMetadataConfig.EXPRESSION_INDEX_ENABLE_PROP.key}=false")
      // do another insert after disabling the index
      // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
      spark.sql(s"insert into $tableName values(4, 10, 1664170924, 'a2')")
      // check query result
      checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') = '2022-09-26'")(
        Seq(3, "a2"),
        Seq(4, "a2")
      )

      // enable expression index
      spark.sql(s"set ${HoodieMetadataConfig.EXPRESSION_INDEX_ENABLE_PROP.key}=true")
      // do another insert after initializing the index
      // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2024-09-26
      spark.sql(s"insert into $tableName values(5, 10, 1727329324, 'a3')")
      // check query result for predicates including values when expression index was disabled
      checkAnswer(s"select id, name from $tableName where from_unixtime(ts, 'yyyy-MM-dd') IN ('2024-09-26', '2022-09-26')")(
        Seq(3, "a2"),
        Seq(4, "a2"),
        Seq(5, "a3")
      )
      // verify there are new updates to expression index
      checkAnswer(metadataSql)(
        Seq("2020-09-26", "2021-09-26"),
        Seq("2022-09-26", "2022-09-26"),
        Seq("2022-09-26", "2022-09-26"), // for file in name=a2
        Seq("2024-09-26", "2024-09-26") // for file in name=a3
      )
    }
  }

  // Test expression index using column stats and bloom filters, and then clean older version, and check index is correct.
  test("Test Expression Index With Cleaning") {
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

          setCompactionConfigs(tableType)
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
          // create expression index
          spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
          // validate index created successfully
          val metaClient = createMetaClient(spark, basePath)
          assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
          assertTrue(metaClient.getIndexMetadata.isPresent)
          assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

          // verify expression index records by querying metadata table
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

          // verify there are new updates to expression index with isDeleted true for cleaned file
          checkAnswer(s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value, ColumnStatsMetadata.isDeleted from hudi_metadata('$tableName') where type=3 and ColumnStatsMetadata.fileName='$fileName'")(
            Seq("2022-09-26", "2022-09-26", false) // for cleaned file, there won't be any stats produced.
          )
        }
      }
    }
  }

  /**
   * Test expression index with auto generation of record keys
   */
  test("Test Expression Index With AutoGen") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_stats_pruning_binary_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        setCompactionConfigs(tableType)
        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)
        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )

        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime')")
        checkAnswer(s"select id, rider, from_unixtime(ts) from $tableName where from_unixtime(ts) > '1970-01-03'")(
          Seq("trip2", "rider-C", "2023-09-22 20:28:40"),
          Seq("trip5", "rider-A", "2023-11-07 09:34:09")
        )

        // validate pruning
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")
        val metaClient = createMetaClient(spark, basePath)
        val fromUnixTime = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.from_unixtime(functions.col("ts"))), tableSchema)
        val literal = Literal.create("2023-09-22 20:28:40")
        val dataFilter = EqualTo(fromUnixTime, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_datestr on $tableName")
      }
    }
  }

  /**
   * Test expression index with invalid options
   */
  test("Test Expression Index Creation With Invalid Options") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_stats_pruning_binary_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        setCompactionConfigs(tableType)
        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        // With invalid options
        checkNestedExceptionContains(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', invalidOp='random')")(
          "Input options [invalidOp] are not valid for spark function"
        )
      }
    }
  }

  test("Test Prune Partitions") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_prune_partitions_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
        setCompactionConfigs(tableType)
        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )

        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")
        var metaClient = createMetaClient(spark, basePath)
        val fromUnixTimeExpr = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.from_unixtime(functions.col("ts"), "yyyy-MM-dd")), tableSchema)
        val literal = Literal.create("2023-11-07")
        val dataFilter = EqualTo(fromUnixTimeExpr, literal)
        val commonOpts = opts + ("path" -> metaClient.getBasePath.toString)
        var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
        val filterReferencedColumns = HoodieFileIndex.collectReferencedColumns(spark, Seq(dataFilter), tableSchema)
        val metadataConfig = HoodieMetadataConfig.newBuilder
          .enable(true)
          .build()
        var exprIndexSupport = new ExpressionIndexSupport(spark, tableSchema, metadataConfig, metaClient)
        // Validate no partition pruning when index is not yet defined
        assertTrue(exprIndexSupport.prunePartitions(fileIndex, Seq(dataFilter), filterReferencedColumns).isEmpty)

        spark.sql(s"create index idx_ts on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
        metaClient = createMetaClient(spark, basePath)
        // validate partition pruning after index is created
        fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
        exprIndexSupport = new ExpressionIndexSupport(spark, tableSchema, metadataConfig, metaClient)
        assertEquals(Set("state=texas"), exprIndexSupport.prunePartitions(fileIndex, Seq(dataFilter), filterReferencedColumns).get)
        spark.sql(s"drop index idx_ts on $tableName")

        spark.sql(s"create index idx_ts on $tableName using bloom_filters(ts)")
        metaClient = createMetaClient(spark, basePath)
        // validate partition pruning after index is created
        fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
        exprIndexSupport = new ExpressionIndexSupport(spark, tableSchema, metadataConfig, metaClient)
        assertTrue(exprIndexSupport.prunePartitions(fileIndex, Seq(dataFilter), filterReferencedColumns).isEmpty)
        spark.sql(s"drop index idx_ts on $tableName")
      }
    }
  }

  test("Test expression index partition pruning with unpartitioned table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_partition_pruning_with_unpartitioned_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |location '$basePath'
             |""".stripMargin)

        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")

        spark.sql(s"create index idx_rider on $tableName using column_stats(rider) options(expr='upper')")
        val metaClient = createMetaClient(spark, basePath)
        // validate skipping with both types of expression
        val riderExpr = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.upper(functions.col("rider"))), tableSchema)
        val literal = Literal.create("RIDER-D")
        val dataFilter = EqualTo(riderExpr, literal)
        // Partition pruning should not kick in for unpartitioned table
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = false)

        // Validate partition stat records
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}")(
          Seq(getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "", "rider"), "RIDER-A", "RIDER-F")
        )

        spark.sql(s"update $tableName set rider = 'rider-G' where id = 'trip5'")
        // Validate partition stat records after update
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}")(
          Seq(getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "", "rider"), "RIDER-A", "RIDER-G")
        )

        spark.sql(s"drop index idx_rider on $tableName")
      }
    }
  }

  test("Test expression index partition pruning with partition stats") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_partition_pruning_with_partition_stats_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")

        spark.sql(s"create index idx_ts on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
        var metaClient = createMetaClient(spark, basePath)
        // validate skipping with both types of expression
        val fromUnixTimeExpr = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.from_unixtime(functions.col("ts"), "yyyy-MM-dd")), tableSchema)
        var literal = Literal.create("2023-11-07")
        var dataFilter = EqualTo(fromUnixTimeExpr, literal)
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_ts on $tableName")

        spark.sql(s"create index idx_unix on $tableName using column_stats(date) options(expr='unix_timestamp', format='yyyy-MM-dd')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val unixTimestamp = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.unix_timestamp(functions.col("date"), "yyyy-MM-dd")), tableSchema)
        literal = Literal.create(1732924800L)
        dataFilter = EqualTo(unixTimestamp, literal)
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_unix on $tableName")

        spark.sql(s"create index idx_to_date on $tableName using column_stats(date) options(expr='to_date', format='yyyy-MM-dd')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val toDate = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.to_date(functions.col("date"), "yyyy-MM-dd")), tableSchema)
        dataFilter = EqualTo(toDate, sparkAdapter.getExpressionFromColumn(lit(18230)))
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_to_date on $tableName")
      }
    }
  }

  test("Test expression index pruning after update with partition stats") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val isTableMOR = tableType.equals("mor")
        val tableName = generateTableName + s"_partition_pruning_after_update_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql("set hoodie.compact.inline=false") // initially disable compaction to avoid merging log files
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-B','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-D','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")

        spark.sql(s"create index idx_rider on $tableName using column_stats(rider) options(expr='upper')")
        var metaClient = createMetaClient(spark, basePath)
        // validate skipping with both types of expression
        val riderExpr = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.upper(functions.col("rider"))), tableSchema)
        var literal = Literal.create("RIDER-D")
        var dataFilter = EqualTo(riderExpr, literal)
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true)

        // Validate partition stat records
        // first form the keys to validate, because partition stats gets built for all columns
        val riderCalifornia = getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "state=california", "rider")
        val riderTexas = getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "state=texas", "rider")
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} AND key IN ('$riderCalifornia', '$riderTexas')")(
          Seq(riderCalifornia, "RIDER-A", "RIDER-C"),
          Seq(riderTexas, "RIDER-D", "RIDER-F")
        )

        spark.sql(s"update $tableName set rider = 'rider-G' where id = 'trip5'")
        metaClient = createMetaClient(spark, basePath)
        literal = Literal.create("RIDER-D")
        dataFilter = EqualTo(riderExpr, literal)
        // RIDER-D filter should return partitions only with MOR table
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true, isNoScanExpected = !isTableMOR)

        // Validate partition stat records after update
        // For MOR table, min value would still be RIDER-D since the update is in log file and old parquet file with value RIDER-D is still present
        val partitionMinRiderValue = if (isTableMOR) "RIDER-D" else "RIDER-E"
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} AND key IN ('$riderCalifornia', '$riderTexas')")(
          Seq(riderCalifornia, "RIDER-A", "RIDER-C"),
          Seq(riderTexas, partitionMinRiderValue, "RIDER-G")
        )

        if (isTableMOR) {
          spark.sql("set hoodie.compact.inline=true")
          spark.sql("set hoodie.compact.inline.max.delta.commits=1")
        }
        spark.sql(s"update $tableName set rider = 'rider-H' where id = 'trip5'")
        metaClient = createMetaClient(spark, basePath)
        literal = Literal.create("RIDER-D")
        dataFilter = EqualTo(riderExpr, literal)
        // RIDER-D filter should not return any partitions now since MOR is compacted
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true, isNoScanExpected = true)

        // Validate partition stat records after update
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} AND key IN ('$riderCalifornia', '$riderTexas')")(
          Seq(riderCalifornia, "RIDER-A", "RIDER-C"),
          Seq(riderTexas, "RIDER-E", "RIDER-H")
        )
        spark.sql(s"drop index idx_rider on $tableName")
      }
    }
  }

  test("Test expression index pruning after delete with partition stats") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val isTableMOR = tableType.equals("mor")
        val tableName = generateTableName + s"_partition_pruning_after_delete_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql("set hoodie.compact.inline=false") // initially disable compaction to avoid merging log files
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-B','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-D','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")

        spark.sql(s"create index idx_rider on $tableName using column_stats(rider) options(expr='upper')")
        var metaClient = createMetaClient(spark, basePath)
        // validate skipping with both types of expression
        val riderExpr = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.upper(functions.col("rider"))), tableSchema)
        var literal = Literal.create("RIDER-D")
        var dataFilter = EqualTo(riderExpr, literal)
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true)

        // Validate partition stat records
        // first form the keys to validate, because partition stats gets built for all columns
        val riderCalifornia = getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "state=california", "rider")
        val riderTexas = getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "state=texas", "rider")
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} AND key IN ('$riderCalifornia', '$riderTexas')")(
          Seq(riderCalifornia, "RIDER-A", "RIDER-C"),
          Seq(riderTexas, "RIDER-D", "RIDER-F")
        )

        spark.sql(s"delete from $tableName where id = 'trip5'")
        metaClient = createMetaClient(spark, basePath)
        literal = Literal.create("RIDER-D")
        dataFilter = EqualTo(riderExpr, literal)
        // RIDER-D filter should prune all partitions only for COW since MOR is not yet compacted
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true, isNoScanExpected = !isTableMOR)

        // Validate partition stat records after delete
        // For MOR table, min value would still be RIDER-D since the delete is in log file and old parquet file with value RIDER-D is still present
        val partitionMinRiderValue = if (isTableMOR) "RIDER-D" else "RIDER-E"
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} AND key IN ('$riderCalifornia', '$riderTexas')")(
          Seq(riderCalifornia, "RIDER-A", "RIDER-C"),
          Seq(riderTexas, partitionMinRiderValue, "RIDER-F")
        )

        if (isTableMOR) {
          spark.sql("set hoodie.compact.inline=true")
          spark.sql("set hoodie.compact.inline.max.delta.commits=1")
        }
        // delete entry with rider-E
        spark.sql(s"delete from $tableName where id = 'trip3'")
        metaClient = createMetaClient(spark, basePath)
        literal = Literal.create("RIDER-E")
        dataFilter = EqualTo(riderExpr, literal)
        // RIDER-D filter should prune all partitions since MOR is now compacted
        verifyPartitionPruning(opts, Seq(), Seq(dataFilter), metaClient, isDataSkippingExpected = true, isNoScanExpected = true)

        // Validate partition stat records after update
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} AND key IN ('$riderCalifornia', '$riderTexas')")(
          Seq(riderCalifornia, "RIDER-A", "RIDER-C"),
          Seq(riderTexas, "RIDER-F", "RIDER-F")
        )

        spark.sql(s"drop index idx_rider on $tableName")
      }
    }
  }

  /**
   * Test expression index with data skipping for date and timestamp based expressions.
   */
  test("Test Expression Index Column Stat Pruning With Timestamp Expressions") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_stats_pruning_date_expr_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")

        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        setCompactionConfigs(tableType)
        spark.sql("set hoodie.parquet.small.file.limit=0")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")

        // With binary expression
        spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
        // validate index created successfully
        var metaClient = createMetaClient(spark, basePath)
        val fromUnixTime = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.from_unixtime(functions.col("ts"), "yyyy-MM-dd")), tableSchema)
        var literal = Literal.create("2023-11-07")
        var dataFilter = EqualTo(fromUnixTime, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_datestr on $tableName")

        spark.sql(s"create index idx_unix_default on $tableName using column_stats(dateDefault) options(expr='unix_timestamp')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val unixTimestampDefault = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.unix_timestamp(functions.col("dateDefault"))), tableSchema)
        literal = Literal.create(1606699840L)
        dataFilter = EqualTo(unixTimestampDefault, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_unix_default on $tableName")

        spark.sql(s"create index idx_unix on $tableName using column_stats(date) options(expr='unix_timestamp', format='yyyy-MM-dd')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val unixTimestamp = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.unix_timestamp(functions.col("date"), "yyyy-MM-dd")), tableSchema)
        literal = Literal.create(1606694400L)
        dataFilter = EqualTo(unixTimestamp, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_unix on $tableName")

        spark.sql(s"create index idx_to_date on $tableName using column_stats(date) options(expr='to_date', format='yyyy-MM-dd')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val toDate = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.to_date(functions.col("date"), "yyyy-MM-dd")), tableSchema)
        dataFilter = EqualTo(toDate, sparkAdapter.getExpressionFromColumn(lit(18596)))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_to_date on $tableName")

        spark.sql(s"create index idx_to_date_default on $tableName using column_stats(date) options(expr='to_date')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val toDateDefault = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.to_date(functions.col("date"))), tableSchema)
        dataFilter = EqualTo(toDateDefault, sparkAdapter.getExpressionFromColumn(lit(18596)))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_to_date_default on $tableName")

        spark.sql(s"create index idx_date_format on $tableName using column_stats(date) options(expr='date_format', format='yyyy')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val dateFormatDefault = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.date_format(functions.col("date"), "yyyy")), tableSchema)
        dataFilter = EqualTo(dateFormatDefault, sparkAdapter.getExpressionFromColumn(lit("2020")))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_date_format on $tableName")

        spark.sql(s"create index idx_to_timestamp_default on $tableName using column_stats(date) options(expr='to_timestamp')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val toTimestampDefault = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.to_timestamp(functions.col("date"))), tableSchema)
        dataFilter = EqualTo(toTimestampDefault, sparkAdapter.getExpressionFromColumn(lit(1732924800000000L)))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_to_timestamp_default on $tableName")

        spark.sql(s"create index idx_to_timestamp on $tableName using column_stats(date) options(expr='to_timestamp', format='yyyy-MM-dd')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val toTimestamp = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.to_timestamp(functions.col("date"), "yyyy-MM-dd")), tableSchema)
        dataFilter = EqualTo(toTimestamp, sparkAdapter.getExpressionFromColumn(lit(1732924800000000L)))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_to_timestamp on $tableName")

        spark.sql(s"create index idx_date_add on $tableName using column_stats(date) options(expr='date_add', days='10')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val dateAdd = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.date_add(functions.col("date"), 10)), tableSchema)
        dataFilter = EqualTo(dateAdd, sparkAdapter.getExpressionFromColumn(lit(18606)))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_date_add on $tableName")

        spark.sql(s"create index idx_date_sub on $tableName using column_stats(date) options(expr='date_sub', days='10')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val dateSub = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.date_sub(functions.col("date"), 10)), tableSchema)
        dataFilter = EqualTo(dateSub, sparkAdapter.getExpressionFromColumn(lit(18586)))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_date_sub on $tableName")
      }
    }
  }

  /**
   * Test expression index with data skipping for string expressions.
   */
  test("Test Expression Index Column Stat Pruning With String Expression") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_stats_pruning_string_expr_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts LONG,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    dateDefault STRING,
             |    date STRING,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
             |""".stripMargin)

        setCompactionConfigs(tableType)
        spark.sql("set hoodie.parquet.small.file.limit=0")
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414527,'trip1','rider-A','driver-K',19.10, '2020-11-30 01:30:40', '2020-11-30', 'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14, '2021-11-30 01:30:40', '2021-11-30', 'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50, '2022-11-30 01:30:40', '2022-11-30', 'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15, '2023-11-30 01:30:40', '2023-11-30', 'houston','texas')
             |""".stripMargin)
        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, dateDefault, date, city, state) VALUES
             |  (1695414520,'trip2','rider-C','driver-M',27.70,'2024-11-30 01:30:40', '2024-11-30', 'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32, '2019-11-30 01:30:40', '2019-11-30', 'san_diego','texas')
             |""".stripMargin)

        val tableSchema: StructType =
          StructType(
            Seq(
              StructField("ts", LongType),
              StructField("id", StringType),
              StructField("rider", StringType),
              StructField("driver", StringType),
              StructField("fare", DoubleType),
              StructField("dateDefault", StringType),
              StructField("date", StringType),
              StructField("city", StringType),
              StructField("state", StringType)
            )
          )
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")

        // With unary expression
        spark.sql(s"create index idx_lower on $tableName using column_stats(rider) options(expr='lower')")
        var metaClient = createMetaClient(spark, basePath)
        // validate skipping with both types of expression
        val lowerExpr = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.lower(functions.col("rider"))), tableSchema)
        var literal = Literal.create("rider-c")
        var dataFilter = EqualTo(lowerExpr, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_lower on $tableName")

        spark.sql(s"create index idx_substring on $tableName using column_stats(driver) options(expr='substring', pos='8', len='1')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val substring = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.substring(functions.col("driver"), 8, 1)), tableSchema)
        dataFilter = EqualTo(substring, sparkAdapter.getExpressionFromColumn(lit("K")))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_substring on $tableName")

        spark.sql(s"create index idx_trim on $tableName using column_stats(driver) options(expr='trim', trimString='-K')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val trim = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.trim(functions.col("driver"), "-K")), tableSchema)
        dataFilter = EqualTo(trim, sparkAdapter.getExpressionFromColumn(lit("driver")))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_trim on $tableName")

        spark.sql(s"create index idx_rtrim on $tableName using column_stats(driver) options(expr='rtrim', trimString='-K')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val rtrim = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.rtrim(functions.col("driver"), "-K")), tableSchema)
        dataFilter = EqualTo(rtrim, sparkAdapter.getExpressionFromColumn(lit("driver")))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_rtrim on $tableName")

        spark.sql(s"create index idx_ltrim on $tableName using column_stats(driver) options(expr='ltrim', trimString='driver-')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val ltrim = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.ltrim(functions.col("driver"), "driver-")), tableSchema)
        dataFilter = EqualTo(ltrim, sparkAdapter.getExpressionFromColumn(lit("K")))
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_ltrim on $tableName")

        spark.sql(s"create index idx_regexp on $tableName using column_stats(rider) options(expr='regexp_replace', pattern='rider', replacement='passenger')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val regExpReplace = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.regexp_replace(functions.col("rider"), "rider", "passenger")), tableSchema)
        literal = Literal.create("passenger-F")
        dataFilter = EqualTo(regExpReplace, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_regexp on $tableName")

        spark.sql(s"create index idx_regexp_extract on $tableName using column_stats(driver) options(expr='regexp_extract', pattern='driver-(\\\\w+)', idx='1')")
        metaClient = HoodieTableMetaClient.reload(metaClient)
        val regExpExtract = resolveExpr(spark, sparkAdapter.getExpressionFromColumn(functions.regexp_extract(functions.col("driver"), "driver-(\\w+)", 1)), tableSchema)
        literal = Literal.create("K")
        dataFilter = EqualTo(regExpExtract, literal)
        verifyFilePruning(opts, dataFilter, metaClient, isDataSkippingExpected = true)
        spark.sql(s"drop index idx_regexp_extract on $tableName")
      }
    }
  }

  test("Test bloom filters index pruning") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName + s"_bloom_pruning_$tableType"
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
        spark.sql(
          s"""
           CREATE TABLE $tableName (
             |    ts BIGINT,
             |    id STRING,
             |    rider STRING,
             |    driver STRING,
             |    fare DOUBLE,
             |    city STRING,
             |    state STRING
             |) USING HUDI
             |options(
             |    primaryKey ='id',
             |    type = '$tableType',
             |    hoodie.metadata.enable = 'true',
             |    hoodie.datasource.write.recordkey.field = 'id',
             |    hoodie.enable.data.skipping = 'true'
             |)
             |PARTITIONED BY (state)
             |location '$basePath'
       """.stripMargin)

        spark.sql("set hoodie.parquet.small.file.limit=0")
        spark.sql("set hoodie.enable.data.skipping=true")
        spark.sql("set hoodie.metadata.enable=true")
        if (HoodieSparkUtils.gteqSpark3_4) {
          spark.sql("set spark.sql.defaultColumn.enabled=false")
        }

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, city, state) VALUES
             |  (1695159649,'trip1','rider-A','driver-K',19.10,'san_francisco','california'),
             |  (1695414531,'trip6','rider-C','driver-K',17.14,'san_diego','california'),
             |  (1695332066,'trip3','rider-E','driver-O',93.50,'austin','texas'),
             |  (1695516137,'trip4','rider-F','driver-P',34.15,'houston','texas')
             |""".stripMargin)

        spark.sql(
          s"""
             |insert into $tableName(ts, id, rider, driver, fare, city, state) VALUES
             |  (1695091554,'trip2','rider-C','driver-M',27.70,'sunnyvale','california'),
             |  (1699349649,'trip5','rider-A','driver-Q',3.32,'san_diego','texas')
             |""".stripMargin)

        // create index using bloom filters on city column with upper() function
        spark.sql(s"create index idx_bloom_$tableName on $tableName using bloom_filters(city) options(expr='upper', " +
          s"${HoodieExpressionIndex.FALSE_POSITIVE_RATE}='0.01', ${HoodieExpressionIndex.BLOOM_FILTER_TYPE}='SIMPLE', ${HoodieExpressionIndex.BLOOM_FILTER_NUM_ENTRIES}='1000')")
        var metaClient = createMetaClient(spark, basePath)
        val expectedInstantTime = metaClient.getCommitsTimeline
          .filterCompletedInstants().lastInstant().get().requestedTime
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(s"expr_index_idx_bloom_$tableName"))
        assertTrue(metaClient.getIndexMetadata.isPresent)
        assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())
        val indexDefinition: HoodieIndexDefinition = metaClient.getIndexMetadata.get.getIndexDefinitions.get(s"expr_index_idx_bloom_$tableName")
        // validate index options
        assertEquals("0.01", indexDefinition.getIndexOptions.get(HoodieExpressionIndex.FALSE_POSITIVE_RATE))
        assertEquals("SIMPLE", indexDefinition.getIndexOptions.get(HoodieExpressionIndex.BLOOM_FILTER_TYPE))
        assertEquals("1000", indexDefinition.getIndexOptions.get(HoodieExpressionIndex.BLOOM_FILTER_NUM_ENTRIES))

        // validate index metadata
        val indexMetadataDf = spark.sql(s"select key, BloomFilterMetadata from hudi_metadata('$tableName') where BloomFilterMetadata is not null")
        assertEquals(4, indexMetadataDf.count()) // corresponding to 4 files
        val indexMetadata = indexMetadataDf.collect()
        indexMetadata.foreach(row => {
          val bloomFilterMetadata = row.getStruct(1)
          assertEquals("SIMPLE", bloomFilterMetadata.getString(0))
          assertEquals(expectedInstantTime, bloomFilterMetadata.getString(1))
        })

        // Pruning takes place only if query uses upper function on city
        checkAnswer(s"select id, rider from $tableName where upper(city) in ('sunnyvale', 'sg')")()
        checkAnswer(s"select id, rider from $tableName where lower(city) = 'sunny'")()
        checkAnswer(s"select id, rider from $tableName where upper(city) = 'SUNNYVALE'")(
          Seq("trip2", "rider-C")
        )
        // verify file pruning
        val opts = Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")
        val cityColumn = AttributeReference("city", StringType)()
        val upperCityExpr = Upper(cityColumn) // Apply the `upper` function to the city column
        val targetCityUpper = Literal.create("SUNNYVALE")
        val dataFilterUpperCityEquals = EqualTo(upperCityExpr, targetCityUpper)
        verifyFilePruning(opts, dataFilterUpperCityEquals, metaClient, isDataSkippingExpected = true)

        // drop index and recreate without upper() function
        spark.sql(s"drop index idx_bloom_$tableName on $tableName")
        spark.sql(s"create index idx_bloom_$tableName on $tableName using bloom_filters(city)")
        // Pruning takes place only if query uses no function on city
        checkAnswer(s"select id, rider from $tableName where city = 'sunnyvale'")(
          Seq("trip2", "rider-C")
        )
        metaClient = createMetaClient(spark, basePath)
        // verify file pruning
        val targetCity = Literal.create("sunnyvale")
        val dataFilterCityEquals = EqualTo(cityColumn, targetCity)
        verifyFilePruning(opts, dataFilterCityEquals, metaClient, isDataSkippingExpected = true)
        // validate IN query
        checkAnswer(s"select id, rider from $tableName where city in ('san_diego', 'sunnyvale')")(
          Seq("trip2", "rider-C"),
          Seq("trip5", "rider-A"),
          Seq("trip6", "rider-C")
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

  test("Test Expression Index Insert after Initialization") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val isPartitioned = true
        val tableName = generateTableName + s"_init_$tableType$isPartitioned"
        val partitionByClause = if (isPartitioned) "partitioned by(price)" else ""
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
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

        writeRecordsAndValidateExpressionIndex(tableName, basePath, isDelete = false, shouldCompact = false, shouldCluster = false, shouldRollback = false)
      }
    }
  }

  test("Test Expression Index Rollback") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val isPartitioned = true
        val tableName = generateTableName + s"_rollback_$tableType$isPartitioned"
        val partitionByClause = if (isPartitioned) "partitioned by(price)" else ""
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        setCompactionConfigs(tableType)
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
             |  hoodie.metadata.index.partition.stats.enable = false
             | )
             | $partitionByClause
             | location '$basePath'
       """.stripMargin)

        writeRecordsAndValidateExpressionIndex(tableName, basePath, isDelete = false, shouldCompact = false, shouldCluster = false, shouldRollback = true)
        // Validate partition stat records after rollback do not contain entries from rolled back commit
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') " +
          s"where type=${MetadataPartitionType.PARTITION_STATS.getRecordType}")(
          Seq(getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "price=10", "ts"), "2020-09-26", "2020-09-26"),
          Seq(getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "price=100", "ts"), "2021-09-26", "2021-09-26"),
          Seq(getPartitionStatsIndexKey(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX, "price=1000", "ts"), "2022-09-26", "2022-09-26")
        )
      }
    }
  }

  private def setCompactionConfigs(tableType: String): Unit = {
    spark.sql(s"set hoodie.compact.inline= ${if (tableType == "mor") "true" else "false"}")
    if (tableType == "mor") {
      spark.sql("set hoodie.compact.inline.max.delta.commits=2")
    }
  }

  /**
   * Write records to the table with the given operation type and do updates or deletes, and then validate expression index.
   */
  private def writeRecordsAndValidateExpressionIndex(tableName: String,
                                                     basePath: String,
                                                     isDelete: Boolean,
                                                     shouldCompact: Boolean,
                                                     shouldCluster: Boolean,
                                                     shouldRollback: Boolean): Unit = {
    // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2020-09-26
    spark.sql(s"insert into $tableName values(1, 'a1', 1601098924, 10)")
    // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2021-09-26
    spark.sql(s"insert into $tableName values(2, 'a2', 1632634924, 100)")
    // a record with from_unixtime(ts, 'yyyy-MM-dd') = 2022-09-26
    spark.sql(s"insert into $tableName values(3, 'a3', 1664170924, 1000)")
    // create expression index
    spark.sql(s"create index idx_datestr on $tableName using column_stats(ts) options(expr='from_unixtime', format='yyyy-MM-dd')")
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

    // validate the expression index
    val metadataSql = s"select ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value, ColumnStatsMetadata.isDeleted from hudi_metadata('$tableName') where type=3"
    // validate the expression index
    checkAnswer(metadataSql)(
      Seq("2020-09-26", "2020-09-26", false),
      Seq("2021-09-26", "2021-09-26", false),
      Seq("2022-09-26", "2022-09-26", false),
      Seq("2024-09-26", "2024-09-26", false)
    )

    if (shouldRollback) {
      // rollback the operation
      val lastCompletedInstant = metaClient.reloadActiveTimeline().getCommitsTimeline.filterCompletedInstants().lastInstant()
      val configBuilder = getWriteConfigBuilder(Map.empty, metaClient.getBasePath.toString)
      configBuilder.withMetadataConfig(HoodieMetadataConfig.newBuilder()
        .withMetadataIndexColumnStats(false).withMetadataIndexPartitionStats(false).build())
      val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)), configBuilder.build())
      writeClient.rollback(lastCompletedInstant.get().requestedTime)
      writeClient.close()
      // validate the expression index
      checkAnswer(metadataSql)(
        // the last commit is rolledback so no records for that
        Seq("2020-09-26", "2020-09-26", false),
        Seq("2021-09-26", "2021-09-26", false),
        Seq("2022-09-26", "2022-09-26", false)
      )
    }
  }

  test("testExpressionIndexUsingColumnStatsWithPartitionAndFilesFilter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      val metadataOpts = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.EXPRESSION_INDEX_ENABLE_PROP.key -> "true"
      )
      val opts = Map(
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4",
        HoodieWriteConfig.TBL_NAME.key -> tableName,
        RECORDKEY_FIELD.key -> "c1",
        HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
        PARTITIONPATH_FIELD.key() -> "c8",
        "hoodie.metadata.index.column.stats.enable" -> "false"
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
      // Create a expression index on column c6
      spark.sql(s"create table $tableName using hudi location '$basePath'")
      spark.sql(s"create index idx_datestr on $tableName using column_stats(c6) options(expr='year')")
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      assertTrue(metaClient.getIndexMetadata.isPresent)
      assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())

      // check expression index records
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(toProperties(metadataOpts))
        .build()
      val expressionIndexSupport = new ExpressionIndexSupport(spark, null, metadataConfig, metaClient)
      val prunedPartitions = Set("9")
      var indexDf = expressionIndexSupport.loadExpressionIndexDataFrame("expr_index_idx_datestr", prunedPartitions, shouldReadInMemory = true)
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
      indexDf = expressionIndexSupport.loadExpressionIndexDataFrame("expr_index_idx_datestr", Set(), shouldReadInMemory = true)
      assertTrue(indexDf.count() > 1)
    }
  }

  test("testComputeCandidateFileNames") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      // in this test, we will create a table with inserts going to log file so that there is a file slice with only log file and no base file
      val metadataOpts = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.EXPRESSION_INDEX_ENABLE_PROP.key -> "true"
      )
      val opts = Map(
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4",
        HoodieWriteConfig.TBL_NAME.key -> tableName,
        TABLE_TYPE.key -> "MERGE_ON_READ",
        RECORDKEY_FIELD.key -> "c1",
        HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
        PARTITIONPATH_FIELD.key() -> "c8",
        // setting IndexType to be INMEMORY to simulate Global Index nature
        HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.INMEMORY.name(),
        "hoodie.metadata.index.column.stats.enable" -> "false"
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
      // Create a expression index on column c6
      spark.sql(s"create table $tableName using hudi location '$basePath'")
      spark.sql(s"create index idx_datestr on $tableName using column_stats(c6) options(expr='year')")
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains("expr_index_idx_datestr"))
      assertTrue(metaClient.getIndexMetadata.isPresent)
      assertEquals(1, metaClient.getIndexMetadata.get.getIndexDefinitions.size())
      // check expression index records
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(toProperties(metadataOpts))
        .build()
      val fileIndex = new HoodieFileIndex(spark, metaClient, None,
        opts ++ metadataOpts ++ Map("glob.paths" -> s"$basePath/9", DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"), includeLogFiles = true)
      val expressionIndexSupport = new ExpressionIndexSupport(spark, null, metadataConfig, metaClient)
      val partitionFilter: Expression = EqualTo(AttributeReference("c8", IntegerType)(), Literal(9))
      val (isPruned, prunedPaths) = fileIndex.prunePartitionsAndGetFileSlices(Seq.empty, Seq(partitionFilter))
      assertTrue(isPruned)
      val prunedPartitionAndFileNames = expressionIndexSupport.getPrunedPartitionsAndFileNames(fileIndex, prunedPaths)
      assertTrue(prunedPartitionAndFileNames._1.size == 1) // partition
      assertTrue(prunedPartitionAndFileNames._2.size == 1) // log file
      assertTrue(FSUtils.isLogFile(prunedPartitionAndFileNames._2.head))

      val prunedPartitionAndFileNamesMap = expressionIndexSupport.getPrunedPartitionsAndFileNamesMap(prunedPaths, includeLogFiles = true)
      assertTrue(prunedPartitionAndFileNamesMap.keySet.size == 1) // partition
      assertTrue(prunedPartitionAndFileNamesMap.values.head.size == 1) // log file
      assertTrue(FSUtils.isLogFile(prunedPartitionAndFileNamesMap.values.head.head))
    }
  }

  test("testGetExpressionIndexRecordsUsingBloomFilter") {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.EXPRESSION_INDEX_ENABLE_PROP.key -> "true"
    )
    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "testGetExpressionIndexRecordsUsingBloomFilter",
      TABLE_TYPE.key -> "MERGE_ON_READ",
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
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
    var df = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)
    df = df.withColumn(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_PARTITION, lit("c/d"))
      .withColumn(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_RELATIVE_FILE_PATH, lit("c/d/123141ab-701b-4ba4-b60b-e6acd9e9103e-0_329-224134-258390_2131313124.parquet"))
      .withColumn(HoodieExpressionIndex.HOODIE_EXPRESSION_INDEX_FILE_SIZE, lit(100))
    val indexOptions = Map(
      HoodieExpressionIndex.BLOOM_FILTER_TYPE -> "DYNAMIC_V0",
      HoodieExpressionIndex.FALSE_POSITIVE_RATE -> "0.01",
      HoodieExpressionIndex.BLOOM_FILTER_NUM_ENTRIES -> "1000",
      HoodieExpressionIndex.DYNAMIC_BLOOM_MAX_ENTRIES -> "1000"
    )
    val mdtPartitionName = "expr_index_random"
    val bloomFilterRecords = SparkMetadataWriterUtils.getExpressionIndexRecordsUsingBloomFilter(df, "c5",
        HoodieStorageConfig.newBuilder().build(), "",
        HoodieIndexDefinition.newBuilder()
          .withIndexName(mdtPartitionName)
          .withIndexType(MetadataPartitionType.COLUMN_STATS.name())
          .withVersion(HoodieIndexVersion.getCurrentVersion(
            HoodieTableVersion.current(), mdtPartitionName))
          .withIndexOptions(JavaConverters.mapAsJavaMapConverter(indexOptions).asJava).build())
      .getExpressionIndexRecords
    // Since there is only one partition file pair there is only one bloom filter record
    assertEquals(1, bloomFilterRecords.count())
    assertFalse(bloomFilterRecords.isEmpty)
    val bloomFilter: HoodieMetadataBloomFilter = bloomFilterRecords.collectAsList().get(0).getData.asInstanceOf[HoodieMetadataPayload].getBloomFilterMetadata.get()
    assertTrue(bloomFilter.getType.equals("DYNAMIC_V0"))
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression, metaClient: HoodieTableMetaClient,
                                isDataSkippingExpected: Boolean = false, isNoScanExpected: Boolean = false): Unit = {
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
      val filesCountWithNoSkipping = fileIndex.listFiles(Seq.empty, Seq(dataFilter)).flatMap(s => s.files).size
      assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
    } finally {
      fileIndex.close()
    }
  }

  private def verifyPartitionPruning(opts: Map[String, String], partitionFilter: Seq[Expression], dataFilter: Seq[Expression],
                                     metaClient: HoodieTableMetaClient, isDataSkippingExpected: Boolean = false, isNoScanExpected: Boolean = false): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> metaClient.getBasePath.toString)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    try {
      val (isPruned, filteredPartitionDirectoriesAndFileSlices) = fileIndex.prunePartitionsAndGetFileSlices(dataFilter, partitionFilter)
      if (isDataSkippingExpected) {
        assertTrue(isPruned)
      }
      val filteredFilesCount = filteredPartitionDirectoriesAndFileSlices
        .map(o => o._2)
        .flatMap(s => s.iterator)
        .map(f1 => {
          var totalLatestDataFiles = 0L
          totalLatestDataFiles = totalLatestDataFiles + (f1.getLogFiles.count() + (if (f1.getBaseFile.isPresent) 1 else 0))
          totalLatestDataFiles
        }).sum
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
      val filesCountWithNoSkipping = fileIndex.prunePartitionsAndGetFileSlices(partitionFilter, dataFilter)._2
        .map(o => o._2)
        .flatMap(s => s.iterator)
        .map(f1 => {
          var totalLatestDataFiles = 0L
          totalLatestDataFiles = totalLatestDataFiles + (f1.getLogFiles.count() + (if (f1.getBaseFile.isPresent) 1 else 0))
          totalLatestDataFiles
        }).sum
      assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
    } finally {
      fileIndex.close()
    }
  }

  private def getLatestDataFilesCount(includeLogFiles: Boolean = true, metaClient: HoodieTableMetaClient) = {
    var totalLatestDataFiles = 0L
    val fsView: HoodieTableFileSystemView = getTableFileSystemView(metaClient)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().requestedTime)
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

  private def getTableFileSystemView(metaClient: HoodieTableMetaClient): HoodieTableFileSystemView = {
    val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexPartitionStats(true).build()
    val metadataTable = new HoodieBackedTableMetadata(engineContext, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)
    new HoodieTableFileSystemView(
      metadataTable,
      metaClient,
      metaClient.getActiveTimeline)
  }

  private def getWriteConfigBuilder(hudiOpts: Map[String, String], basePath: String): HoodieWriteConfig.Builder = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
  }
}
