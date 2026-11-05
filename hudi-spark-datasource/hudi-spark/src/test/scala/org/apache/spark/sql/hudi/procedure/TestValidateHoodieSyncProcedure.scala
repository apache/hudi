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

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.exception.HoodieIOException

import java.sql.SQLException

/**
 * Tests for [[org.apache.spark.sql.hudi.command.procedures.ValidateHoodieSyncProcedure]].
 *
 * The "complete" / "latestPartitions" modes count records over JDBC, but they do not need a live
 * Hive/JDBC endpoint to be exercised negatively: pointed at a hostless URL, both fail fast in the
 * driver's URL parsing, which raises JdbcUriParseException (an SQLException) before any transport
 * class loads or any network connect. The last test pins those two failure shapes, one of which is
 * the connection-failure masking bug of #19635. Failing during parsing, not transport setup, keeps
 * the pins independent of libthrift resolution: the Hive 2.3.10 client jars need libthrift 0.14.1
 * (TConfiguration), but spark-hive pulls 0.12.0 onto the test classpath, so a connect attempt that
 * reaches HiveAuthUtils.getSocketTransport dies with NoClassDefFoundError instead of the
 * SQLException these pins rely on, see #19680.
 *
 * The other tests pass 'noop', which short-circuits the record counting (record counts stay 0)
 * while still exercising the timeline comparison, the catch-up-commit computation and the result
 * formatting, which is the bulk of the procedure.
 */
class TestValidateHoodieSyncProcedure extends HoodieSparkProcedureTestBase {

  /**
   * The throwable and its causes, in order. Null-terminated, and capped so a self-referential
   * cause cannot spin forever.
   */
  private def causeChain(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(16).toSeq

  private def chainTypes(t: Throwable): String = causeChain(t).map(_.getClass.getName).mkString(" <- ")

  private def createTable(tableName: String, path: String, tableType: String = "cow"): Unit = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long
         |) using hudi
         | location '$path'
         | tblproperties (
         |  type = '$tableType',
         |  primaryKey = 'id',
         |  orderingFields = 'ts'
         | )
       """.stripMargin)
  }

  test("Test Call sync_validate when the target table is ahead (catch-up commits)") {
    withTempDir { tmp =>
      // Each insert must land in its own file group: the catch-up count is
      // recordsWritten - updateRecordsWritten summed over the catch-up commits, so without this
      // conf the second insert packs into the first small file, rewrites the copied first record
      // and the count becomes 3 instead of 2.
      withSQLConf("hoodie.parquet.small.file.limit" -> "0") {
        val srcTable = generateTableName
        val dstTable = generateTableName
        createTable(srcTable, s"${tmp.getCanonicalPath}/$srcTable")
        // The source table has a single, earlier commit.
        spark.sql(s"insert into $srcTable select 1, 'a1', 10, 1000")

        createTable(dstTable, s"${tmp.getCanonicalPath}/$dstTable")
        // The destination table receives later commits, so it is ahead of the source.
        spark.sql(s"insert into $dstTable select 1, 'a1', 10, 1000")
        spark.sql(s"insert into $dstTable select 2, 'a2', 20, 2000")

        val result = spark.sql(
          s"""call sync_validate(src_table => '$srcTable', dst_table => '$dstTable',
             | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
            .stripMargin).collect()

        assertResult(1)(result.length)
        // The procedure prints the unqualified table name, while generateTableName is db-qualified.
        val srcName = srcTable.stripPrefix("default.")
        val dstName = dstTable.stripPrefix("default.")
        // The destination is ahead, so the dst-first branch is taken (count(dst) - count(src)) and the
        // two catch-up commits (one insert record each) are counted. Record counts stay 0 in this mode.
        // "Catach up" mirrors the typo in the procedure's output message.
        assertResult(s"Count difference now is count($dstName) - count($srcName) == 0. Catach up count is 2")(
          result.head.getString(0))
      }
    }
  }

  test("Test Call sync_validate when both tables point at the same timeline (no catch-up)") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // Using the same table as both source and target means neither timeline is ahead,
      // so no catch-up commits are found and only the count difference is reported.
      val result = spark.sql(
        s"""call sync_validate(src_table => '$tableName', dst_table => '$tableName',
           | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
          .stripMargin).collect()

      assertResult(1)(result.length)
      // The procedure prints the unqualified table name, while generateTableName is db-qualified.
      val name = tableName.stripPrefix("default.")
      // No catch-up suffix: the exact match pins that no commits are found after the (shared) latest.
      assertResult(s"Count difference now is count($name) - count($name) == 0")(
        result.head.getString(0))
    }
  }

  test("Test Call sync_validate when the source table is ahead (argument order)") {
    withTempDir { tmp =>
      withSQLConf("hoodie.parquet.small.file.limit" -> "0") {
        val srcTable = generateTableName
        val dstTable = generateTableName
        createTable(dstTable, s"${tmp.getCanonicalPath}/$dstTable")
        spark.sql(s"insert into $dstTable select 1, 'a1', 10, 1000")

        createTable(srcTable, s"${tmp.getCanonicalPath}/$srcTable")
        spark.sql(s"insert into $srcTable select 1, 'a1', 10, 1000")
        spark.sql(s"insert into $srcTable select 2, 'a2', 20, 2000")

        val result = spark.sql(
          s"""call sync_validate(src_table => '$srcTable', dst_table => '$dstTable',
             | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
            .stripMargin).collect()

        assertResult(1)(result.length)
        // The procedure prints the unqualified table name, while generateTableName is db-qualified.
        val srcName = srcTable.stripPrefix("default.")
        val dstName = dstTable.stripPrefix("default.")
        // The source is ahead, so the else branch is taken: this pins that it passes the source
        // table first, i.e. count(src) - count(dst), the mirror image of the dst-ahead branch.
        assertResult(s"Count difference now is count($srcName) - count($dstName) == 0. Catach up count is 2")(
          result.head.getString(0))
      }
    }
  }

  test("Test Call sync_validate when the target table has never been written (sentinel commit)") {
    withTempDir { tmp =>
      withSQLConf("hoodie.parquet.small.file.limit" -> "0") {
        val srcTable = generateTableName
        val dstTable = generateTableName
        createTable(srcTable, s"${tmp.getCanonicalPath}/$srcTable")
        spark.sql(s"insert into $srcTable select 1, 'a1', 10, 1000")
        spark.sql(s"insert into $srcTable select 2, 'a2', 20, 2000")

        // The destination is created but never written, so its commits timeline is empty.
        createTable(dstTable, s"${tmp.getCanonicalPath}/$dstTable")

        val result = spark.sql(
          s"""call sync_validate(src_table => '$srcTable', dst_table => '$dstTable',
             | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
            .stripMargin).collect()

        assertResult(1)(result.length)
        // The procedure prints the unqualified table name, while generateTableName is db-qualified.
        val srcName = srcTable.stripPrefix("default.")
        val dstName = dstTable.stripPrefix("default.")
        // An empty destination timeline falls back to the sentinel latest commit "0", the realistic
        // "target not yet synced" shape. The sentinel is never greater than the source's latest, so
        // the else branch runs, and findInstantsAfter("0") makes the whole source timeline the
        // catch-up set: both inserts are counted.
        assertResult(s"Count difference now is count($srcName) - count($dstName) == 0. Catach up count is 2")(
          result.head.getString(0))
      }
    }
  }

  test("Test Call sync_validate when the target table is mor (known limitation)") {
    withTempDir { tmp =>
      val srcTable = generateTableName
      val dstTable = generateTableName
      createTable(srcTable, s"${tmp.getCanonicalPath}/$srcTable")
      spark.sql(s"insert into $srcTable select 1, 'a1', 10, 1000")

      // Writes to a mor table land as deltacommits, so both of these are in the catch-up range.
      createTable(dstTable, s"${tmp.getCanonicalPath}/$dstTable", "mor")
      spark.sql(s"insert into $dstTable select 1, 'a1', 10, 1000")
      spark.sql(s"insert into $dstTable select 2, 'a2', 20, 2000")

      // Known limitation: the catch-up counting hardcodes the commit action when it rebuilds the
      // instants, so it cannot resolve deltacommit instants and the procedure fails on any mor
      // target with catch-up commits. See #19635. A fix flips this test to assert a count.
      val e = intercept[HoodieIOException] {
        spark.sql(
          s"""call sync_validate(src_table => '$srcTable', dst_table => '$dstTable',
             | mode => 'noop', hive_server_url => 'jdbc:hive2://unused', hive_pass => 'x')"""
            .stripMargin).collect()
      }
      // The instant timestamp varies per run, but the synthesized commit action is the whole bug.
      assert(e.getMessage.startsWith("Cannot find the instant["), e.getMessage)
      assert(e.getMessage.endsWith("__commit__COMPLETED]"), e.getMessage)
    }
  }

  test("Test Call sync_validate record-count modes fail fast on an unusable HiveServer2 url") {
    withTempDir { tmp =>
      val tableName = generateTableName
      createTable(tableName, s"${tmp.getCanonicalPath}/$tableName")
      spark.sql(s"insert into $tableName select 1, 'a1', 10, 1000")

      // The mode dispatch counts records before the timelines are compared, so a single table as
      // both source and target is enough here: neither call survives the record counting.
      // Nothing reaches the network either: the url has no host, so Utils.configureConnParams
      // rejects it with JdbcUriParseException (an SQLException) while parsing, before the driver
      // touches any thrift transport class. That last part is load-bearing, see #19680: with
      // spark-hive's libthrift 0.12.0 on the classpath, transport setup dies with a
      // NoClassDefFoundError (the Hive 2.3.10 jars need 0.14.1) instead of an SQLException.

      // mode = 'complete' routes to the countRecords overload that declares its connection as
      // `var conn: Connection = null` and closes it in an unguarded `finally { conn.close() }`.
      // When DriverManager.getConnection throws, conn is still null, so the finally block raises a
      // NullPointerException that replaces the real SQLException. This pins that masking bug, see
      // #19635. A fix flips this test to assert an SQLException is present in the chain.
      val completeFailure = intercept[Throwable] {
        spark.sql(
          s"""call sync_validate(src_table => '$tableName', dst_table => '$tableName',
             | mode => 'complete', hive_server_url => 'jdbc:hive2://:10000', hive_pass => 'x')"""
            .stripMargin).collect()
      }
      // Assert over the cause chain, never the top-level type: on Spark 3.4+ QueryExecution wraps a
      // NullPointerException thrown by an eagerly executed command into a SparkException
      // [INTERNAL_ERROR]. Message text is off limits too, the NPE message is null on JDK 11 and the
      // Hive URI wording moves with hive.version.
      assert(causeChain(completeFailure).exists(_.isInstanceOf[NullPointerException]),
        chainTypes(completeFailure))
      assert(!causeChain(completeFailure).exists(_.isInstanceOf[SQLException]),
        chainTypes(completeFailure))

      // mode = 'latestPartitions' routes to the sibling overload, which obtains the connection
      // before entering its try and closes it under an `if (conn != null)` guard, so the connection
      // failure surfaces as the SQLException it is instead of being masked.
      val latestPartitionsFailure = intercept[Throwable] {
        spark.sql(
          s"""call sync_validate(src_table => '$tableName', dst_table => '$tableName',
             | mode => 'latestPartitions', hive_server_url => 'jdbc:hive2://:10000', hive_pass => 'x')"""
            .stripMargin).collect()
      }
      assert(causeChain(latestPartitionsFailure).exists(_.isInstanceOf[SQLException]),
        chainTypes(latestPartitionsFailure))
    }
  }
}
