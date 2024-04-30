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

package org.apache.hudi.util

import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}

import org.apache.hadoop.conf.Configuration
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.File
import java.nio.file.Paths

class TestPathUtils {

  @Test
  def testGlobPaths(@TempDir tempDir: File): Unit = {
    val folders: Seq[StoragePath] = Seq(
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder1").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder2").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, ".hoodie").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata").toUri)
    )

    val files: Seq[StoragePath] = Seq(
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder1", "file1").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder1", "file2").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder2", "file3").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, "folder2", "file4").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata", "file5").toUri),
      new StoragePath(Paths.get(tempDir.getAbsolutePath, ".hoodie", "metadata", "file6").toUri)
    )

    folders.foreach(folder => new File(folder.toUri).mkdir())
    files.foreach(file => new File(file.toUri).createNewFile())

    val storage = HoodieStorageUtils.getStorage(tempDir.getAbsolutePath, HoodieTestUtils.getDefaultStorageConf)
    var paths = Seq(tempDir.getAbsolutePath + "/*")
    var globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths, storage)
    assertEquals(folders.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/*/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths, storage)
    assertEquals(files.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths, storage)
    assertEquals(Seq(files(0), files(1)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths, storage)
    assertEquals(Seq(files(2), files(3)).sortWith(_.toString < _.toString),
      globbedPaths.sortWith(_.toString < _.toString))

    paths = Seq(tempDir.getAbsolutePath + "/folder1/*", tempDir.getAbsolutePath + "/folder2/*")
    globbedPaths = PathUtils.checkAndGlobPathIfNecessary(paths, storage)
    assertEquals(files.filterNot(entry => entry.toString.contains(".hoodie"))
      .sortWith(_.toString < _.toString), globbedPaths.sortWith(_.toString < _.toString))
  }


}
