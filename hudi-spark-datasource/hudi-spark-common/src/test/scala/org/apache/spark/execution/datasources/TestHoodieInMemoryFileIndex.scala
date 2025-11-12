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

package org.apache.spark.execution.datasources

import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Paths

class TestHoodieInMemoryFileIndex {

  @Test
  def testCreateInMemoryIndex(@TempDir tempDir: File): Unit = {
    val spark = SparkSession.builder
      .config(getSparkConfForTest("Hoodie Datasource test"))
      .getOrCreate

    val folders: Seq[StoragePath] = Seq(
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder2").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    val index = HoodieInMemoryFileIndex.create(spark, Seq(folders(0), folders(1)))
    val indexedFilePaths = index.allFiles().map(fs => fs.getPath)
    assertEquals(files.sortWith(_.toString < _.toString), indexedFilePaths.sortWith(_.toString < _.toString))
    spark.stop()
  }

}
