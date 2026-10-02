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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.common.table.HoodieTableVersion

import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path, RawLocalFileSystem}
import org.apache.spark.TaskContext
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

/**
 * Reads of schema-evolved tables must resolve each base file's schema without executor tasks touching
 * the table's `.hoodie` folder.
 */
class TestSchemaOnReadExecutorMetaAccess extends HoodieSparkSqlTestBase {

  for (tableType <- Seq("cow", "mor");
       tableVersion <- Seq(HoodieTableVersion.SIX, HoodieTableVersion.current());
       evolveFromCreate <- Seq(true, false)) {
    test(s"Schema-on-read base files resolve schemas without executor .hoodie access (tableType=$tableType, " +
      s"tableVersion=${tableVersion.versionCode()}, evolveFromCreate=$evolveFromCreate)") {
      withTempDir { tmp =>
        val isV6 = tableVersion == HoodieTableVersion.SIX
        withSparkSqlSessionConfigWithCondition(
          ("hoodie.schema.on.read.enable" -> evolveFromCreate.toString, true),
          ("hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true", true),
          // keeps the first commit's base files, which predate the schema history when evolveFromCreate is off
          ("hoodie.parquet.small.file.limit" -> "0", true),
          ("hoodie.metadata.enable" -> "false", isV6)) {
          val tableName = generateTableName
          val writeVersionClause = if (isV6) "hoodie.write.table.version = '6'," else ""
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  cnt int,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}'
               | tblproperties (
               |  $writeVersionClause
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'ts'
               | )
             """.stripMargin)
          spark.sql(s"insert into $tableName values (1, 'a1', 1.0, 10, 1000), (2, 'a2', 2.0, 20, 1000)")
          spark.sql("set hoodie.schema.on.read.enable = true")
          spark.sql(s"alter table $tableName rename column name to fullname")
          spark.sql(s"alter table $tableName alter column cnt type bigint")
          spark.sql(s"alter table $tableName add columns (qty int)")
          spark.sql(s"insert into $tableName values (3, 'a3', 3.0, 30, 1000, 300)")

          val rows = ExecutorMetaFolderAccessFileSystem.recordingTaskAccesses(spark.sparkContext.hadoopConfiguration) {
            spark.sql(s"select id, fullname, price, cnt, qty from $tableName order by id").collect()
          }
          checkAnswer(rows)(
            Seq(1, "a1", 1.0, 10L, null),
            Seq(2, "a2", 2.0, 20L, null),
            Seq(3, "a3", 3.0, 30L, 300))
          ExecutorMetaFolderAccessFileSystem.assertNoTaskAccesses()
        }
      }
    }
  }
}

/**
 * A local file system that records the paths under `.hoodie` opened, listed or looked up from inside Spark tasks.
 */
class ExecutorMetaFolderAccessFileSystem extends RawLocalFileSystem {

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    ExecutorMetaFolderAccessFileSystem.record("open", f)
    super.open(f, bufferSize)
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    ExecutorMetaFolderAccessFileSystem.record("list", f)
    super.listStatus(f)
  }

  override def getFileStatus(f: Path): FileStatus = {
    ExecutorMetaFolderAccessFileSystem.record("status", f)
    super.getFileStatus(f)
  }
}

object ExecutorMetaFolderAccessFileSystem {
  private val accesses = new ConcurrentLinkedQueue[String]()
  @volatile private var firstAccessStack: Option[Throwable] = None

  def assertNoTaskAccesses(): Unit = {
    val recorded = accesses.asScala.toSeq
    val stack = firstAccessStack.map(t => t.getStackTrace.take(40).mkString("\n  at ")).getOrElse("")
    assert(recorded.isEmpty,
      s"Executor tasks accessed ${recorded.size} .hoodie paths:\n${recorded.distinct.mkString("\n")}\nfirst access:\n  at $stack")
  }

  /**
   * Runs `f` with the `file` scheme served by this file system on `conf`, recording task accesses.
   */
  def recordingTaskAccesses[T](conf: org.apache.hadoop.conf.Configuration)(f: => T): T = {
    accesses.clear()
    firstAccessStack = None
    conf.setClass("fs.file.impl", classOf[ExecutorMetaFolderAccessFileSystem], classOf[FileSystem])
    conf.setBoolean("fs.file.impl.disable.cache", true)
    try {
      f
    } finally {
      conf.unset("fs.file.impl")
      conf.unset("fs.file.impl.disable.cache")
    }
  }

  private def record(operation: String, path: Path): Unit = {
    val pathStr = path.toUri.getPath
    if (TaskContext.get() != null && pathStr.contains("/.hoodie/")) {
      if (firstAccessStack.isEmpty) {
        firstAccessStack = Some(new Throwable())
      }
      accesses.add(s"$operation $pathStr")
    }
  }
}
