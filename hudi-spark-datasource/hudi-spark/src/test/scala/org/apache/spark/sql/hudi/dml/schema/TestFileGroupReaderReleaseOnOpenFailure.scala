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

package org.apache.spark.sql.hudi.dml.schema

import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, LocalFileSystem, Path}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Tag

import java.io.IOException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * A merge-on-read snapshot read that fails while the file group reader loads its log files, after it has opened
 * the base file, closes that base file rather than leaving it open with nothing to close it.
 */
@Tag("functional")
class TestFileGroupReaderReleaseOnOpenFailure extends HoodieSparkSqlTestBase {
  private val functional = new org.scalatest.Tag("functional")

  test("A failed log file read releases the opened base file", functional) {
    withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |create table $tableName (id int, name string, ts long) using hudi
           |tblproperties (type = 'mor', primaryKey = 'id', orderingFields = 'ts')
           |location '${tmp.getCanonicalPath}'
           |""".stripMargin)
      spark.sql(s"insert into $tableName values (1, 'a1', 1000)")
      spark.sql(s"update $tableName set name = 'a2', ts = 1001 where id = 1")
      assert(new java.io.File(tmp.getCanonicalPath).listFiles().exists(_.getName.contains(".log.")),
        "the update must leave a log file in the file slice")

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val previousImpl = hadoopConf.get("fs.file.impl")
      val previousCache = hadoopConf.get("fs.file.impl.disable.cache")
      hadoopConf.set("fs.file.impl", classOf[OpenTrackingLocalFileSystem].getName)
      hadoopConf.setBoolean("fs.file.impl.disable.cache", true)
      try {
        // Control: a read that completes leaves no base file stream open, so the count observes every close.
        OpenTrackingLocalFileSystem.reset(failLogs = false)
        assert(spark.sql(s"select name from $tableName").collect().map(_.getString(0)).toSeq == Seq("a2"))
        assert(OpenTrackingLocalFileSystem.baseFileOpens.get() > 0, "the control read must open the base file")
        assert(OpenTrackingLocalFileSystem.openBaseFiles.get() == 0,
          s"base file streams left open after a completed read: ${OpenTrackingLocalFileSystem.openBaseFiles.get()}")

        OpenTrackingLocalFileSystem.reset(failLogs = true)
        val failure = intercept[Exception](spark.sql(s"select id, name from $tableName").collect())
        assert(Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
          .exists(cause => cause.getMessage != null && cause.getMessage.contains(OpenTrackingLocalFileSystem.InjectedMessage)),
          s"the read must fail on the injected log file failure: $failure")
        assert(OpenTrackingLocalFileSystem.baseFileOpens.get() > 0, "the read must open the base file before the log files")
        assert(OpenTrackingLocalFileSystem.openBaseFiles.get() == 0,
          s"base file streams left open after the failed read: ${OpenTrackingLocalFileSystem.openBaseFiles.get()}")
      } finally {
        OpenTrackingLocalFileSystem.failLogOpens.set(false)
        if (previousImpl == null) hadoopConf.unset("fs.file.impl") else hadoopConf.set("fs.file.impl", previousImpl)
        if (previousCache == null) hadoopConf.unset("fs.file.impl.disable.cache") else hadoopConf.set("fs.file.impl.disable.cache", previousCache)
      }
    }
  }
}

/**
 * A local file system that counts open data-table base file streams and can fail to open data-table log files.
 * Closes are counted on the innermost stream: Parquet unwraps nested FSDataInputStreams and closes the inner one.
 */
class OpenTrackingLocalFileSystem extends LocalFileSystem {
  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val name = path.getName
    val dataTableFile = !path.toUri.getPath.contains("/.hoodie/")
    if (dataTableFile && name.contains(".log.") && OpenTrackingLocalFileSystem.failLogOpens.get()) {
      throw new IOException(OpenTrackingLocalFileSystem.InjectedMessage)
    }
    val stream = super.open(path, bufferSize)
    if (dataTableFile && name.endsWith(".parquet") && !name.contains(".log.")) {
      OpenTrackingLocalFileSystem.baseFileOpens.incrementAndGet()
      OpenTrackingLocalFileSystem.openBaseFiles.incrementAndGet()
      new FSDataInputStream(new CloseCountingInputStream(stream.getWrappedStream.asInstanceOf[FSInputStream],
        () => OpenTrackingLocalFileSystem.openBaseFiles.decrementAndGet()))
    } else {
      stream
    }
  }
}

/** Delegates to `in` and runs `onClose` once, on its first close. */
class CloseCountingInputStream(in: FSInputStream, onClose: () => Unit) extends FSInputStream {
  private val closed = new AtomicBoolean(false)

  override def seek(pos: Long): Unit = in.seek(pos)

  override def getPos: Long = in.getPos

  override def seekToNewSource(targetPos: Long): Boolean = in.seekToNewSource(targetPos)

  override def read(): Int = in.read()

  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)

  override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = in.read(position, buffer, offset, length)

  override def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit =
    in.readFully(position, buffer, offset, length)

  override def available(): Int = in.available()

  override def close(): Unit = {
    try {
      in.close()
    } finally {
      if (closed.compareAndSet(false, true)) {
        onClose()
      }
    }
  }
}

object OpenTrackingLocalFileSystem {
  val InjectedMessage = "injected log file open failure"
  val failLogOpens = new AtomicBoolean(false)
  val baseFileOpens = new AtomicInteger()
  val openBaseFiles = new AtomicInteger()

  def reset(failLogs: Boolean): Unit = {
    baseFileOpens.set(0)
    openBaseFiles.set(0)
    failLogOpens.set(failLogs)
  }
}
