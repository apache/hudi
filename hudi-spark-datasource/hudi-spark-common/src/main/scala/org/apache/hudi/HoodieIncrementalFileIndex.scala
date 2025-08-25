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

package org.apache.hudi

import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePathInfo
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util.stream.Collectors

import scala.collection.JavaConverters._

class HoodieIncrementalFileIndex(override val spark: SparkSession,
                                 override val metaClient: HoodieTableMetaClient,
                                 override val schemaSpec: Option[StructType],
                                 override val options: Map[String, String],
                                 @transient override val fileStatusCache: FileStatusCache = NoopCache,
                                 override val includeLogFiles: Boolean,
                                 mergeOnReadIncrementalRelation: MergeOnReadIncrementalRelation)
  extends HoodieFileIndex(
    spark, metaClient, schemaSpec, options, fileStatusCache, includeLogFiles, shouldEmbedFileSlices = true
  ) with FileIndex {

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val fileSlices = mergeOnReadIncrementalRelation.listFileSplits(partitionFilters, dataFilters).toSeq.flatMap(
      {
        case (partitionValues, fileSlices) =>
          fileSlices.filter(!_.isEmpty).map(fs => ( partitionValues, fs.withLogFiles(includeLogFiles)))
      }
    )
    prepareFileSlices(fileSlices)
  }

  override def inputFiles: Array[String] = {
    val fileSlices = mergeOnReadIncrementalRelation.listFileSplits(Seq.empty, Seq.empty)
    if (fileSlices.isEmpty) {
      Array.empty
    }
    fileSlices.values.flatten.flatMap(fs => {
      val baseFileStatusOpt = getBaseFileInfo(Option.apply(fs.getBaseFile.orElse(null)))
      val logFilesStatus = if (includeLogFiles) {
        fs.getLogFiles.map[StoragePathInfo](JFunction.toJavaFunction[HoodieLogFile, StoragePathInfo](lf => lf.getPathInfo))
      } else {
        java.util.stream.Stream.empty()
      }
      val files = logFilesStatus.collect(Collectors.toList[StoragePathInfo]).asScala
      baseFileStatusOpt.foreach(f => files.append(f))
      files
    }).map(fileStatus => fileStatus.getPath.toString).toArray
  }

  def getRequiredFilters: Seq[Filter] = {
    mergeOnReadIncrementalRelation.getRequiredFilters
  }
}
