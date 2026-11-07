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

package org.apache.hudi.functional

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path, RawLocalFileSystem}
import org.apache.spark.TaskContext

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

/**
 * A local file system that records the `.hoodie` accesses made from inside Spark tasks: timeline folder
 * listings, `hoodie.properties` reads and metadata table file reads. Installed for the `file` scheme with
 * [[TaskMetaIOCountingFileSystem.install]].
 */
class TaskMetaIOCountingFileSystem extends RawLocalFileSystem {

  override def listStatus(f: Path): Array[FileStatus] = {
    TaskMetaIOCountingFileSystem.recordListing(f)
    super.listStatus(f)
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    TaskMetaIOCountingFileSystem.recordOpen(f)
    super.open(f, bufferSize)
  }
}

object TaskMetaIOCountingFileSystem {
  private val FS_IMPL = "fs.file.impl"
  private val FS_IMPL_DISABLE_CACHE = "fs.file.impl.disable.cache"

  private val timelineListings = new ConcurrentLinkedQueue[String]()
  private val tablePropertiesReads = new ConcurrentLinkedQueue[String]()
  private val metadataTableFileReads = new ConcurrentLinkedQueue[String]()

  /** Routes the `file` scheme of the given conf to this file system and returns a function restoring it. */
  def install(conf: Configuration): () => Unit = {
    val previousImpl = Option(conf.get(FS_IMPL))
    val previousDisableCache = Option(conf.get(FS_IMPL_DISABLE_CACHE))
    conf.set(FS_IMPL, classOf[TaskMetaIOCountingFileSystem].getName)
    conf.setBoolean(FS_IMPL_DISABLE_CACHE, true)
    reset()
    () => {
      previousImpl.map(conf.set(FS_IMPL, _)).getOrElse(conf.unset(FS_IMPL))
      previousDisableCache.map(conf.set(FS_IMPL_DISABLE_CACHE, _)).getOrElse(conf.unset(FS_IMPL_DISABLE_CACHE))
    }
  }

  def reset(): Unit = {
    timelineListings.clear()
    tablePropertiesReads.clear()
    metadataTableFileReads.clear()
  }

  def taskTimelineListings: Seq[String] = timelineListings.asScala.toSeq

  def taskTablePropertiesReads: Seq[String] = tablePropertiesReads.asScala.toSeq

  def taskMetadataTableFileReads: Seq[String] = metadataTableFileReads.asScala.toSeq

  private def inTask: Boolean = TaskContext.get() != null

  private def recordListing(path: Path): Unit = {
    val p = path.toUri.getPath
    if (inTask && (p.endsWith("/.hoodie/timeline") || p.endsWith("/.hoodie"))) {
      timelineListings.add(p)
    }
  }

  private def recordOpen(path: Path): Unit = {
    val p = path.toUri.getPath
    if (inTask) {
      if (p.endsWith("/hoodie.properties")) {
        tablePropertiesReads.add(p)
      } else if (p.contains("/.hoodie/metadata/") && !p.contains("/.hoodie/metadata/.hoodie/")) {
        metadataTableFileReads.add(p)
      }
    }
  }
}
