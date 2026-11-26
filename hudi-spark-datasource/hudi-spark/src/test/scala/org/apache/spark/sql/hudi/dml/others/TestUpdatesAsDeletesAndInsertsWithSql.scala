/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.core.io.storage.HoodieIOFactory
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.action.commit.BaseWriteHelper

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getMetaClientAndFileSystemView
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Spark SQL DML on merge-on-read tables with [[HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS]]
 * enabled: every update and delete must reach the log as a positional delete.
 */
class TestUpdatesAsDeletesAndInsertsWithSql extends HoodieSparkSqlTestBase {

  Seq("SIMPLE", "GLOBAL_SIMPLE").foreach { indexType =>
    test(s"Test MERGE INTO writing updates as deletes and inserts with $indexType index") {
      withTable(indexType) { (tableName, tablePath) =>
        // Every source row matches and there is no not-matched clause
        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 11.0 as price, 'p1' as dt
             |  union all select 2, 'a2m', 21.0, 'p1' union all select 3, 'a3m', 31.0, 'p1') s
             |on t.id = s.id
             |when matched then update set *
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1m", 11.0, "p1"), Seq(2, "a2m", 21.0, "p1"), Seq(3, "a3m", 31.0, "p1"),
          Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
        assertEquals(Seq("1", "2", "3"), positionalDeletes(tablePath))

        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 'x' as name, 0.0 as price, 'p1' as dt, 'del' as op
             |  union all select 2, 'a2u', 22.0, 'p1', 'upd'
             |  union all select 4, 'a4u', 44.0, 'p2', 'none'
             |  union all select 6, 'a6', 60.0, 'p2', 'ins'
             |  union all select 7, 'a7', 70.0, 'p2', 'none') s
             |on t.id = s.id
             |when matched and s.op = 'del' then delete
             |when matched and s.op = 'upd' then update set id = s.id, name = s.name, price = s.price, dt = s.dt
             |when not matched and s.op = 'ins' then insert (id, name, price, dt) values (s.id, s.name, s.price, s.dt)
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(2, "a2u", 22.0, "p1"), Seq(3, "a3m", 31.0, "p1"), Seq(4, "a4", 40.0, "p2"),
          Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"))
        assertEquals(Seq("1", "1", "2", "2", "3"), positionalDeletes(tablePath))
      }
    }
  }

  Seq("SIMPLE", "GLOBAL_SIMPLE").foreach { indexType =>
    test(s"Test MERGE INTO with matched clauses on target fields writing updates as deletes and inserts with $indexType index") {
      withTable(indexType) { (tableName, tablePath) =>
        // The matched clauses see the target row, which the write reads before it splits an update
        spark.sql(
          s"""
             |merge into $tableName t
             |using (select 1 as id, 5.0 as delta, 'p1' as dt union all select 2, 5.0, 'p1'
             |  union all select 3, 5.0, 'p1' union all select 4, 5.0, 'p2' union all select 6, 1.0, 'p2') s
             |on t.id = s.id
             |when matched and t.price < 30 then update set id = t.id, name = t.name, price = t.price + s.delta, dt = t.dt
             |when matched and t.price >= 40 then delete
             |when not matched then insert (id, name, price, dt) values (s.id, 'a6', 60.0, s.dt)
           """.stripMargin)
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1", 15.0, "p1"), Seq(2, "a2", 25.0, "p1"), Seq(3, "a3", 30.0, "p1"),
          Seq(5, "a5", 50.0, "p2"), Seq(6, "a6", 60.0, "p2"))
        assertEquals(Seq("1", "2", "4"), positionalDeletes(tablePath))
      }
    }
  }

  test("Test MERGE INTO with a partial update assignment writing updates as deletes and inserts") {
    withTable { (tableName, tablePath) =>
      // An insert into a base file must carry every column, which a partial update record does not
      checkExceptionContain(
        s"""
           |merge into $tableName t
           |using (select 1 as id, 12.0 as price, 'p1' as dt) s
           |on t.id = s.id
           |when matched then update set price = s.price
         """.stripMargin)(HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key)
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
        Seq(4, "a4", 40.0, "p2"), Seq(5, "a5", 50.0, "p2"))
      assertEquals(Seq.empty, positionalDeletes(tablePath))
    }
  }

  Seq(None, Some("false")).foreach { optimizedWrites =>
    test(s"Test UPDATE and DELETE writing updates as deletes and inserts with optimized writes ${optimizedWrites.getOrElse("default")}") {
      withSQLConf(optimizedWrites.map(SPARK_SQL_OPTIMIZED_WRITES.key() -> _).toSeq: _*) {
        withTable { (tableName, tablePath) =>
          spark.sql(s"update $tableName set price = price * 2 where id in (1, 4)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 20.0, "p1"), Seq(2, "a2", 20.0, "p1"), Seq(3, "a3", 30.0, "p1"),
            Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq("1", "4"), positionalDeletes(tablePath))

          // The index also locates the tombstoned copy of key 1 in its old file group and deletes it again
          spark.sql(s"delete from $tableName where id in (1, 3)")
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(2, "a2", 20.0, "p1"), Seq(4, "a4", 80.0, "p2"), Seq(5, "a5", 50.0, "p2"))
          assertEquals(Seq("1", "1", "1", "3", "4"), positionalDeletes(tablePath))
        }
      }
    }
  }

  test("Test UPDATE, DELETE and MERGE INTO refused on a table without a primary key writing updates as deletes and inserts") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  dt string
           |) using hudi
           | partitioned by (dt)
           | location '$tablePath'
           | tblproperties (
           |  type = 'mor',
           |  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'
           | )
         """.stripMargin)
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true") {
        // An insert locates no existing row, so the write mode lets it through
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1')")
        val instants = HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
          .getActiveTimeline.getInstants.asScala.toList
        Seq(
          s"update $tableName set price = 11.0 where id = 1",
          s"delete from $tableName where id = 2",
          s"""merge into $tableName t
             |using (select 1 as id, 'a1m' as name, 12.0 as price, 'p1' as dt) s
             |on t.id = s.id
             |when matched then update set *""".stripMargin
        ).foreach { statement =>
          checkExceptionContain(statement)(BaseWriteHelper.keylessTableNotSupported().getMessage)
        }
        // Each statement is refused before it starts a commit
        assertEquals(instants, HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf, tablePath)
          .getActiveTimeline.getInstants.asScala.toList)
      }
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "p1"), Seq(2, "a2", 20.0, "p1"))
    }
  }

  /**
   * Creates a partitioned merge-on-read table with the write mode enabled, holding ids 1 to 3 in
   * partition p1 and ids 4 and 5 in partition p2.
   */
  private def withTable(f: (String, String) => Unit): Unit = withTable("SIMPLE")(f)

  private def withTable(indexType: String)(f: (String, String) => Unit): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      withSQLConf(HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key() -> "true", "hoodie.index.type" -> indexType) {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt string
             |) using hudi
             | partitioned by (dt)
             | location '$tablePath'
             | tblproperties (
             |  type = 'mor',
             |  primaryKey = 'id',
             |  'hoodie.record.merge.mode' = 'COMMIT_TIME_ORDERING'
             | )
           """.stripMargin)
        spark.sql(s"insert into $tableName values (1, 'a1', 10.0, 'p1'), (2, 'a2', 20.0, 'p1'), (3, 'a3', 30.0, 'p1'),"
          + " (4, 'a4', 40.0, 'p2'), (5, 'a5', 50.0, 'p2')")
        f(tableName, tablePath)
      }
    }
  }

  /**
   * Asserts that every log block of the latest file slices is a delete block whose positions point at
   * the rows of the deleted keys in the slice's base file, and returns the sorted deleted keys.
   */
  private def positionalDeletes(tablePath: String): Seq[String] = {
    val (metaClient, fsView) = getMetaClientAndFileSystemView(tablePath)
    val schema = new TableSchemaResolver(metaClient).getTableSchema
    val deletedKeys = ArrayBuffer[String]()
    try {
      Seq("dt=p1", "dt=p2").flatMap(fsView.getLatestFileSlices(_).iterator().asScala).filter(_.hasLogFiles).foreach { slice =>
        assertTrue(slice.getBaseFile.isPresent, s"a file group with logs must have a base file: $slice")
        val baseFileKeys = ArrayBuffer[String]()
        val fileReader = HoodieIOFactory.getIOFactory(metaClient.getStorage)
          .getReaderFactory(HoodieRecordType.AVRO)
          .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, new StoragePath(slice.getBaseFile.get.getPath))
        try {
          fileReader.getRecordKeyIterator.asScala.foreach(baseFileKeys += _)
        } finally {
          fileReader.close()
        }
        slice.getLogFiles.iterator().asScala.foreach { logFile =>
          val logReader = HoodieLogFormat.newReader(metaClient, logFile, schema)
          try {
            while (logReader.hasNext) {
              val block = logReader.next()
              assertTrue(block.isInstanceOf[HoodieDeleteBlock],
                s"expected only delete blocks but got ${block.getClass.getSimpleName} in ${logFile.getFileName}")
              val deleteBlock = block.asInstanceOf[HoodieDeleteBlock]
              val deleteRecords = deleteBlock.getRecordsToDelete
              val positions = deleteBlock.getRecordPositionList.asScala
              assertEquals(deleteRecords.length, positions.size, "every delete record must carry a position")
              positions.zipWithIndex.foreach { case (position, i) =>
                assertEquals(deleteRecords(i).getRecordKey, baseFileKeys(position.intValue()),
                  "the base file row at a delete position must hold the deleted record key")
              }
              assertEquals(slice.getBaseFile.get.getCommitTime, deleteBlock.getBaseFileInstantTimeOfPositions,
                "positions must reference the file slice's base file")
              deleteRecords.foreach(deletedKeys += _.getRecordKey)
            }
          } finally {
            logReader.close()
          }
        }
      }
    } finally {
      fsView.close()
    }
    deletedKeys.sorted.toList
  }
}
