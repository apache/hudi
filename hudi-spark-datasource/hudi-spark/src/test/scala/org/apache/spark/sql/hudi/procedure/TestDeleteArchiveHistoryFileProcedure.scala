/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.procedure

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableVersion
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.testutils.HoodieTestUtils

import org.apache.spark.sql.Row

import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

class TestDeleteArchiveHistoryFileProcedure extends HoodieSparkProcedureTestBase {

  private def withArchive(f: (String, Path, Path) => Unit): Unit = {
    withTempDir { tmp =>
      val base = tmp.getCanonicalPath
      val meta = HoodieTestUtils.init(base, HoodieTableType.MERGE_ON_READ, HoodieTableVersion.SIX)
      val timeline = Paths.get(meta.getTimelinePath.toUri.getPath)
      val archive = Files.createDirectories(Paths.get(meta.getArchivePath.toUri.getPath))
      f(base, timeline, archive)
    }
  }

  private def writeFile(dir: Path, name: String, mtime: Long): Path = {
    val file = Files.write(dir.resolve(name), Array[Byte](1, 2, 3))
    Files.setLastModifiedTime(file, FileTime.fromMillis(mtime))
  }

  Seq(("commit", "deltacommit", 30), ("deltacommit", "commit", 7)).foreach {
    case (first, second, days) =>
      test(s"clean only expired lower versions using earliest $first and $days days") {
        withArchive { (base, timeline, archive) =>
          val requested = "20240131120000000"
          Files.createFile(timeline.resolve("20240101120000000.clean"))
          Files.createFile(timeline.resolve(s"$requested.$first"))
          Files.createFile(timeline.resolve(s"20240205120000000.$second"))
          val cutoff = TimelineUtils.parseDateFromInstantTimeSafely(requested).get().getTime - TimeUnit.DAYS.toMillis(days)
          val expired = writeFile(archive, ".commits_.archive.2", cutoff - 1)
          val retained = Seq(
            writeFile(archive, ".commits_.archive.3", cutoff),
            writeFile(archive, ".commits_.archive.4", cutoff + 1),
            writeFile(archive, ".commits_.archive.10", cutoff - 1),
            writeFile(archive, ".commits_.archive.invalid", cutoff - 1))

          val retention = if (days == 30) "" else s", retain_days => $days"
          val call = s"call delete_archive_history_file(path => '$base'$retention)"
          val result = spark.sql(call).head()
          assert(result.getAs[Long]("cutoff_time") == cutoff)
          val deleted = result.getAs[scala.collection.Seq[Row]]("deleted_files")
          assert(deleted.size == 1)
          assert(deleted.head.getString(0).endsWith("/" + expired.getFileName))
          assert(deleted.head.getLong(1) == 3L)
          assert(deleted.head.getLong(2) == cutoff - 1)
          assert(Files.notExists(expired))
          retained.foreach(file => assert(Files.exists(file), file.toString))
          assert(spark.sql(call).head().getAs[scala.collection.Seq[Row]]("deleted_files").isEmpty)
        }
      }
  }

  test("retain old archives when the timeline contains no commit or deltacommit") {
    withArchive { (base, timeline, archive) =>
      Files.createFile(timeline.resolve("20240131120000000.clean"))
      val files = Seq(1, 2).map(v => writeFile(archive, s".commits_.archive.$v", 1000L))
      val result = spark.sql(s"call delete_archive_history_file(path => '$base')").head()
      assert(result.isNullAt(result.fieldIndex("cutoff_time")))
      assert(result.getAs[scala.collection.Seq[Row]]("deleted_files").isEmpty)
      files.foreach(file => assert(Files.exists(file), file.toString))
    }
  }
}
