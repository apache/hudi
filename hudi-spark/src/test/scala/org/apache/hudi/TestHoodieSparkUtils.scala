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

package org.apache.hudi

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class TestHoodieSparkUtils {

  @Test
  def testGlobPaths(@TempDir tempDir: File): Unit = {
    val folders: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    var paths = Seq(tempDir.getAbsolutePath + "/*")
    var globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(folders.sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/*/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(Seq(files(0), files(1)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(Seq(files(2), files(3)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*", tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(paths,
      new Path(paths.head).getFileSystem(new Configuration()))
    assertEquals(files.sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))
  }

  @Test
  def testCreateInMemoryIndex(@TempDir tempDir: File): Unit = {
    val spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    val folders: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2").toUri)
    )

    val files: Seq[Path] = Seq(
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new Path(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    val index = HoodieSparkUtils.createInMemoryFileIndex(spark, Seq(folders(0), folders(1)))
    val indexedFilePaths = index.allFiles().map(fs => fs.getPath)
    assertEquals(files.sortWith(_.toString < _.toString), indexedFilePaths.sortWith(_.toString < _.toString))
    spark.stop()
  }
}
