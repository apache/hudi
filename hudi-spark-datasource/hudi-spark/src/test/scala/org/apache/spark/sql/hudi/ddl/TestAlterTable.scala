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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

import scala.collection.JavaConverters._

class TestAlterTable extends HoodieSparkSqlTestBase {

  test("Test Alter Table") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
       """.stripMargin)

        // change column comment
        spark.sql(s"alter table $tableName change column id id int comment 'primary id'")
        var catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        validateTableSchema(tablePath)
        spark.sql(s"alter table $tableName change column name name string comment 'name column'")
        spark.sessionState.catalog.refreshTable(new TableIdentifier(tableName))
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        assertResult("name column") (
          catalogTable.schema(catalogTable.schema.fieldIndex("name")).getComment().get
        )
        validateTableSchema(tablePath)

        // alter table name.
        val newTableName = s"${tableName}_1"
        spark.sql(s"alter table $tableName rename to $newTableName")
        assertResult(false)(
          spark.sessionState.catalog.tableExists(new TableIdentifier(tableName))
        )
        assertResult(true) (
          spark.sessionState.catalog.tableExists(new TableIdentifier(newTableName))
        )

        val metaClient = createMetaClient(spark, tablePath)
        assertResult(newTableName) (metaClient.getTableConfig.getTableName)
        validateTableSchema(tablePath)

        // insert some data
        spark.sql(s"insert into $newTableName values(1, 'a1', 10, 1000)")

        // add column
        spark.sql(s"alter table $newTableName add columns(ext0 string)")
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(newTableName))
        assertResult(Seq("id", "name", "price", "ts", "ext0")) {
          HoodieSqlCommonUtils.removeMetaFields(catalogTable.schema).fields.map(_.name)
        }
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1000, null)
        )
        validateTableSchema(tablePath)

        // change column's data type
        checkExceptionContain(s"alter table $newTableName change column id id bigint") (
          "ALTER TABLE CHANGE COLUMN is not supported for changing column 'id'" +
            " with type 'IntegerType' to 'id' with type 'LongType'"
        )

        // Insert data to the new table.
        spark.sql(s"insert into $newTableName values(2, 'a2', 12, 1000, 'e0')")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1000, null),
          Seq(2, "a2", 12.0, 1000, "e0")
        )

        // Merge data to the new table.
        spark.sql(
          s"""
             |merge into $newTableName t0
             |using (
             |  select 1 as id, 'a1' as name, 12 as price, 1001L as ts, 'e0' as ext0
             |) s0
             |on t0.id = s0.id
             |when matched then update set *
             |when not matched then insert *
           """.stripMargin)
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 12.0, 1001, "e0"),
          Seq(2, "a2", 12.0, 1000, "e0")
        )

        // Update data to the new table.
        spark.sql(s"update $newTableName set price = 10, ext0 = null where id = 1")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1001, null),
          Seq(2, "a2", 12.0, 1000, "e0")
        )
        spark.sql(s"update $newTableName set price = 10, ext0 = null where id = 2")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1001, null),
          Seq(2, "a2", 10.0, 1000, null)
        )

        // Delete data from the new table.
        spark.sql(s"delete from $newTableName where id = 1")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(2, "a2", 10.0, 1000, null)
        )

        val partitionedTable = generateTableName
        spark.sql(
          s"""
             |create table $partitionedTable (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$partitionedTable'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
             | partitioned by (dt)
       """.stripMargin)
        spark.sql(s"insert into $partitionedTable values(1, 'a1', 10, 1000, '2021-07-25')")
        spark.sql(s"alter table $partitionedTable add columns(ext0 double)")
        checkAnswer(s"select id, name, price, ts, dt, ext0 from $partitionedTable")(
          Seq(1, "a1", 10.0, 1000, "2021-07-25", null)
        )

        spark.sql(s"insert into $partitionedTable values(2, 'a2', 10, 1000, 1, '2021-07-25')")
        checkAnswer(s"select id, name, price, ts, dt, ext0 from $partitionedTable order by id")(
          Seq(1, "a1", 10.0, 1000, "2021-07-25", null),
          Seq(2, "a2", 10.0, 1000, "2021-07-25", 1.0)
        )

        val tableName2 = generateTableName
        spark.sql(
          s"""
             |create table $tableName2 (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName2'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  `hoodie.index.type`='BUCKET',
             |  `hoodie.index.bucket.engine`='SIMPLE',
             |  `hoodie.bucket.index.num.buckets`='2',
             |  `hoodie.bucket.index.hash.field`='id',
             |  `hoodie.storage.layout.type`='BUCKET',
             |  `hoodie.storage.layout.partitioner.class`='org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner'
             | )
       """.stripMargin)
        spark.sql(s"insert into $tableName2 values(1, 'a1', 10, 1000)")
        spark.sql(s"alter table $tableName2 add columns(dt string comment 'data time')")
        checkAnswer(s"select id, name, price, ts, dt from $tableName2")(
          Seq(1, "a1", 10.0, 1000, null)
        )

        withSQLConf("hoodie.schema.on.read.enable" -> "true") {
          spark.sql(s"alter table $tableName2 add columns(hh string comment 'hour time')")
          Seq(1, "a1", 10.0, 1000, null, null)
        }
      }
    }
  }

  test("Test Alter table With Disable Clean Rollback And Archive") {
    withTempDir { tmp =>
      Seq("cow").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.metadata.enable = 'false',
             |  hoodie.clean.commits.retained = '100',
             |  hoodie.clustering.inline = 'true',
             |  hoodie.clustering.inline.max.commits = '1',
             |  hoodie.keep.max.commits = '300',
             |  hoodie.keep.min.commits = '200'
             | )
       """.stripMargin)

        // 1. Executing ALTER TABLE commands won't trigger a rollback service
        // Create an inflight commit
        val client = HoodieCLIUtils.createHoodieWriteClient(spark, tablePath, Map.empty, Option(tableName))
        val metaClient = createMetaClient(spark, tablePath)
        val firstInstant = client.startCommit(HoodieTimeline.COMMIT_ACTION)
        val hoodieTable = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
        val timeLine = hoodieTable.getActiveTimeline
        val requested = hoodieTable.getInstantGenerator.createNewInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, firstInstant)
        val metadata = new HoodieCommitMetadata
        metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
        timeLine.transitionRequestedToInflight(requested, org.apache.hudi.common.util.Option.of(metadata))
        // Executing ALTER TABLE
        spark.sql(s"alter table $tableName change column id id int comment 'primary id'")
        var catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        validateTableSchema(tablePath)
        metaClient.reloadActiveTimeline()
        val rollbackCount = metaClient.getActiveTimeline.getRollbackTimeline.countInstants()
        assertTrue(rollbackCount == 0, "Executing ALTER TABLE commands won't trigger a rollback service.")
        // commint the inflight commit
        val jsc = new JavaSparkContext(spark.sparkContext)
        client.commit(firstInstant,jsc.emptyRDD)

        // 2. Executing ALTER TABLE commands won't trigger a clean service
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 20, 2000)")

        spark.sql(s"alter table $tableName set TBLPROPERTIES (hoodie.clean.commits.retained = '1')")
        spark.sql(s"alter table $tableName change column id id int comment 'pk'")
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("pk") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        validateTableSchema(tablePath)
        metaClient.reloadActiveTimeline()
        val cleanCount = metaClient.getActiveTimeline.getCleanerTimeline.countInstants()
        assertTrue(cleanCount == 0, "Executing ALTER TABLE commands won't trigger a clean service.")

        // 3. Executing ALTER TABLE commands won't trigger an archive service
        spark.sql(s"alter table $tableName  set TBLPROPERTIES (hoodie.keep.max.commits = '3')")
        spark.sql(s"alter table $tableName  set TBLPROPERTIES (hoodie.keep.min.commits = '2')")
        spark.sql(s"alter table $tableName change column id id int comment 'primary id'")
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        metaClient.getArchivedTimeline().reload()
        val archiveCount = metaClient.getArchivedTimeline().countInstants()
        assertTrue(archiveCount == 0, "Executing ALTER TABLE commands won't trigger an archive service.")
      }
    }
  }

  def validateTableSchema(tablePath: String): Unit = {
    val metaClient = createMetaClient(spark, tablePath)
    val schema = new TableSchemaResolver(metaClient).getTableSchema(false)
    assertFalse(schema.getFields.asScala.exists(f => HoodieRecord.HOODIE_META_COLUMNS.contains(f.name())),
      "Metadata fields should be excluded from the table schema")
  }

  test("Test Alter Rename Table") {
    Seq("cow", "mor").foreach { tableType =>
      val tableName = generateTableName
      // Create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |  type = '$tableType',
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
     """.stripMargin)

      // alter table name.
      val newTableName = s"${tableName}_1"
      val oldLocation = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName)).properties.get("path")
      spark.sql(s"alter table $tableName rename to $newTableName")
      val newLocation = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(newTableName)).properties.get("path")
      // only hoodieCatalog will set path to tblp
      if (oldLocation.nonEmpty) {
        assertResult(false)(
          newLocation.equals(oldLocation)
        )
      } else {
        assertResult(None)(newLocation)
      }
    }
  }

  test("Test Alter Rename Table With Location") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // Create table with location
        val locTableName = s"${tableName}_loc"
        val tablePath = s"${tmp.getCanonicalPath}/$locTableName"
        spark.sql(
          s"""
             |create table $locTableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
       """.stripMargin)

        // alter table name.
        val newLocTableName = s"${locTableName}_1"
        val oldLocation2 = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(locTableName))
          .properties.get("path")
        spark.sql(s"alter table $locTableName rename to $newLocTableName")
        val newLocation2 = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(newLocTableName))
          .properties.get("path")
        if (oldLocation2.nonEmpty) {
          // the scheme and authority need to match as well
          assertResult(true)(
            oldLocation2.get.equals(newLocation2.get)
          )
        } else {
          assertResult(None) (newLocation2)
        }
      }
    }
  }

  test("Test Alter Table With OCC") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.write.concurrency.mode='optimistic_concurrency_control',
             |  hoodie.clean.failed.writes.policy='LAZY',
             |  hoodie.write.lock.provider='org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider'
             | )
       """.stripMargin)

        // change column comment
        spark.sql(s"alter table $tableName change column id id int comment 'primary id'")
        var catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        spark.sql(s"alter table $tableName change column name name string comment 'name column'")
        spark.sessionState.catalog.refreshTable(new TableIdentifier(tableName))
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
        assertResult("primary id") (
          catalogTable.schema(catalogTable.schema.fieldIndex("id")).getComment().get
        )
        assertResult("name column") (
          catalogTable.schema(catalogTable.schema.fieldIndex("name")).getComment().get
        )

        // alter table name.
        val newTableName = s"${tableName}_1"
        spark.sql(s"alter table $tableName rename to $newTableName")
        assertResult(false)(
          spark.sessionState.catalog.tableExists(new TableIdentifier(tableName))
        )
        assertResult(true) (
          spark.sessionState.catalog.tableExists(new TableIdentifier(newTableName))
        )

        val metaClient = createMetaClient(spark, tablePath)
        assertResult(newTableName) (metaClient.getTableConfig.getTableName)

        // insert some data
        spark.sql(s"insert into $newTableName values(1, 'a1', 10, 1000)")

        // add column
        spark.sql(s"alter table $newTableName add columns(ext0 string)")
        catalogTable = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(newTableName))
        assertResult(Seq("id", "name", "price", "ts", "ext0")) {
          HoodieSqlCommonUtils.removeMetaFields(catalogTable.schema).fields.map(_.name)
        }
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1000, null)
        )

        // change column's data type
        checkExceptionContain(s"alter table $newTableName change column id id bigint") (
          "ALTER TABLE CHANGE COLUMN is not supported for changing column 'id'" +
            " with type 'IntegerType' to 'id' with type 'LongType'"
        )

        // Insert data to the new table.
        spark.sql(s"insert into $newTableName values(2, 'a2', 12, 1000, 'e0')")
        checkAnswer(s"select id, name, price, ts, ext0 from $newTableName")(
          Seq(1, "a1", 10.0, 1000, null),
          Seq(2, "a2", 12.0, 1000, "e0")
        )
      }
    }
  }

  test("Test Alter Table With Spark Sql Conf") {
    withTempDir { tmp =>
      Seq(true, false).foreach { cleanEnable =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long
             |) using hudi
             | location '$tablePath'
             | tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.clean.trigger.strategy = 'NUM_COMMITS',
             |  hoodie.clean.commits.retained = '3'
             | )
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"update $tableName set name = 'a2' where id = 1")
        spark.sql(s"update $tableName set name = 'a3' where id = 1")
        spark.sql(s"update $tableName set name = 'a4' where id = 1")

        withSQLConf("hoodie.clean.automatic" -> cleanEnable.toString) {
          spark.sql(s"alter table $tableName add columns(ext0 string)")
        }

        val metaClient = createMetaClient(spark, tablePath)

        // alter table always disable clean
        val cnt = metaClient.getActiveTimeline.countInstants()
        if (cleanEnable) {
          assertResult(5)(cnt)
        } else {
          assertResult(5)(cnt)
        }
      }
    }
  }
}
