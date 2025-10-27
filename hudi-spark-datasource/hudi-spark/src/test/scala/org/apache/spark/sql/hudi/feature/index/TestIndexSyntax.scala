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

import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.metadata.HoodieTableMetadataUtil

import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.hudi.command.{CreateIndexCommand, DropIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

class TestIndexSyntax extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
  }

  test("Test Create/Drop/Show/Refresh Index") {
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

        logicalPlan = sqlParser.parsePlan(s"create index idx_name on $tableName using lucene (name) options(block_size=1024)")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("lucene")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(false)(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].ignoreIfExists)
        assertResult(Map("block_size" -> "1024"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)

        logicalPlan = sqlParser.parsePlan(s"create index if not exists idx_price on $tableName using lucene (price options(`order`='desc')) options(block_size=512)")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].table, databaseName, tableName)
        assertResult("idx_price")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexName)
        assertResult("lucene")(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].indexType)
        assertResult(Map("order" -> "desc"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].columns.head._2)
        assertResult(Map("block_size" -> "512"))(resolvedLogicalPlan.asInstanceOf[CreateIndexCommand].options)

        logicalPlan = sqlParser.parsePlan(s"drop index if exists idx_name on $tableName")
        resolvedLogicalPlan = analyzer.execute(logicalPlan)
        assertTableIdentifier(resolvedLogicalPlan.asInstanceOf[DropIndexCommand].table, databaseName, tableName)
        assertResult("idx_name")(resolvedLogicalPlan.asInstanceOf[DropIndexCommand].indexName)
        assertResult(true)(resolvedLogicalPlan.asInstanceOf[DropIndexCommand].ignoreIfNotExists)
      }
    }
  }

  test("Test Create and Drop Index Syntax with Simple record key") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
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
             |  hoodie.metadata.enable = 'true'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

        checkException(s"create index idx_ts on $tableName (id)") (
          "Record index should be named as record_index"
        )
        checkException(s"create index record_index on $tableName (ts)") (
          "Input columns should match configured record key columns: id"
        )
        checkException(s"create index record_index on $tableName (id,ts)") (
          "Index can be created either on all record key columns or a non record key column"
        )

        spark.sql(s"create index record_index on $tableName (id)")
        spark.sql(s"drop index record_index on $tableName")

        // Test record index creation with using clause
        spark.sql(s"create index record_index on $tableName using record_index(id)")

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX))
      }
    }
  }

  test("Test Create and Drop Index Syntax with Complex record key") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
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
             |  primaryKey ='id,price',
             |  type = '$tableType',
             |  preCombineField = 'ts',
             |  hoodie.metadata.enable = 'true'
             | )
             | partitioned by(ts)
             | location '$basePath'
       """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")

        checkException(s"create index record_index on $tableName (price)")(
          "Index can be only be created on all record key columns. Configured record key fields Set(id, price). Input columns: Set(price)"
        )
        checkException(s"create index record_index on $tableName (id,ts)") (
          "Index can be created either on all record key columns or a non record key column"
        )
        spark.sql(s"create index record_index on $tableName (id,price)")

        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX))
      }
    }
  }

  test("Test matchesRecordKeys API") {
    val tableConfig = new HoodieTableConfig()

    // Test with simple record key
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "rk1")

    var colNames:Set[String] = Set("rk1")
    assertTrue(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set("col1")
    assertFalse(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set()
    assertFalse(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set("rk1", "col1")
    checkException(() => CreateIndexCommand.matchesRecordKeys(colNames, tableConfig)) (
      "Index can be created either on all record key columns or a non record key column"
    )

    // Test with complex record key
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "rk1,rk2")

    colNames = Set("rk1", "rk2")
    assertTrue(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set("rk1")
    checkException(() => CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))(
      "Index can be only be created on all record key columns. Configured record key fields Set(rk1, rk2). Input columns: Set(rk1)"
    )
    colNames = Set()
    assertFalse(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set("col1")
    assertFalse(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set("col1", "col2")
    assertFalse(CreateIndexCommand.matchesRecordKeys(colNames, tableConfig))
    colNames = Set("rk1", "col1")
    checkException(() => CreateIndexCommand.matchesRecordKeys(colNames, tableConfig)) (
      "Index can be created either on all record key columns or a non record key column"
    )
    colNames = Set("rk1", "rk2", "col1")
    checkException(() => CreateIndexCommand.matchesRecordKeys(colNames, tableConfig)) (
      "Index can be created either on all record key columns or a non record key column"
    )
  }

  private def assertTableIdentifier(catalogTable: CatalogTable, expectedDatabaseName: String, expectedTableName: String): Unit = {
    assertResult(Some(expectedDatabaseName))(catalogTable.identifier.database)
    assertResult(expectedTableName)(catalogTable.identifier.table)
  }
}
