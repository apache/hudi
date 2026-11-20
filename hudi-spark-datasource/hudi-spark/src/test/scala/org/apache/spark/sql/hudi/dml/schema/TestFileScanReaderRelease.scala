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

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.jupiter.api.Tag

import java.io.{Closeable, IOException}
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** A Spark file scan closes the reader of a file it skips before it opens the next file. */
@Tag("functional")
class TestFileScanReaderRelease extends HoodieSparkSqlTestBase {
  private val functional = new org.scalatest.Tag("functional")

  for (ioFailure <- Seq(true, false); releasing <- Seq(false, true)) {
    val mode = if (releasing) "Releasing listener" else "Existing listener control"
    val failure = if (ioFailure) "IO failure" else "decoding failure"

    test(s"$mode closes a skipped reader before opening the next file after $failure", functional) {
      val id = UUID.randomUUID().toString
      FileScanReaderReleaseState.closeCounts.put(id, new AtomicInteger())
      try {
        withSQLConf("spark.sql.files.ignoreCorruptFiles" -> "true") {
          val files = Array("first", "second").map { name =>
            sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
              InternalRow.empty, new StoragePath(s"file:/reader-release/$id/$name"), 0, 1)
          }
          val readFile = (file: PartitionedFile) => FileScanReaderReleaseState.open(file, id, releasing, ioFailure)
          // Use the version adapter for Spark 3.3 through 4.x constructor compatibility.
          val scan = sparkAdapter.createHoodieFileScanRDD(spark, readFile, Seq(FilePartition(0, files)),
            StructType(Seq(StructField("value", IntegerType, nullable = false))))
          val rows = scan.map(_.getInt(0)).collect().toSeq
          val immediate = FileScanReaderReleaseState.closedWhenSecondOpened.get(id).intValue()
          val afterTask = FileScanReaderReleaseState.closeCounts.get(id).get()
          info(s"rows=$rows; first close count when second opens=$immediate; after task completion=$afterTask")
          assert(rows == Seq(11, 22))
          assert(immediate == 1, s"first reader close count when healthy second opens: $immediate")
          if (releasing) {
            assert(afterTask == 1, "task completion must not close the released reader again")
          }
        }
      } finally {
        FileScanReaderReleaseState.closeCounts.remove(id)
        FileScanReaderReleaseState.closedWhenSecondOpened.remove(id)
      }
    }
  }
}

private object FileScanReaderReleaseState {
  val closeCounts = new ConcurrentHashMap[String, AtomicInteger]()
  val closedWhenSecondOpened = new ConcurrentHashMap[String, Integer]()

  def open(file: PartitionedFile, id: String, releasing: Boolean, ioFailure: Boolean): Iterator[InternalRow] = {
    val delegate = if (file.toPath.getName == "first") {
      new Iterator[InternalRow] with Closeable {
        private var emitted = false

        override def hasNext: Boolean = {
          if (!emitted) {
            true
          } else if (ioFailure) {
            throw new IOException("invalid data after the first row")
          } else {
            throw new IllegalStateException("decoding failed after the first row")
          }
        }

        override def next(): InternalRow = {
          emitted = true
          InternalRow(11)
        }

        override def close(): Unit = {
          closeCounts.get(id).incrementAndGet()
        }
      }
    } else {
      closedWhenSecondOpened.put(id, Integer.valueOf(closeCounts.get(id).get()))
      Iterator(InternalRow(22))
    }
    if (releasing) {
      CloseableIteratorListener.addListenerReleasedOnExhaustion(delegate)
    } else {
      CloseableIteratorListener.addListener(delegate)
    }
  }
}
