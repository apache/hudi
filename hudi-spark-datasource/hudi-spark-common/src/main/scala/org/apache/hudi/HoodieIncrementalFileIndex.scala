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

import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util.stream.Collectors
import scala.jdk.CollectionConverters.asScalaBufferConverter

class HoodieIncrementalFileIndex(override val spark: SparkSession,
                                 override val metaClient: HoodieTableMetaClient,
                                 override val schemaSpec: Option[StructType],
                                 override val options: Map[String, String],
                                 @transient override val fileStatusCache: FileStatusCache = NoopCache,
                                 override val includeLogFiles: Boolean,
                                 override val shouldEmbedFileSlices: Boolean)
  extends HoodieFileIndex(
    spark, metaClient, schemaSpec, options, fileStatusCache, includeLogFiles, shouldEmbedFileSlices
  ) with FileIndex {
  val mergeOnReadIncrementalRelation: MergeOnReadIncrementalRelation = MergeOnReadIncrementalRelation(
    spark.sqlContext, options, metaClient, schemaSpec, schemaSpec)

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    hasPushedDownPartitionPredicates = true

    val fileSlices = mergeOnReadIncrementalRelation.listFileSplits(partitionFilters, dataFilters)
    if (fileSlices.isEmpty) {
      Seq.empty
    } else {
      val prunedPartitionsAndFilteredFileSlices = fileSlices.map {
        case (partitionValues, fileSlices) =>
          if (shouldEmbedFileSlices) {
            val baseFileStatusesAndLogFileOnly: Seq[FileStatus] = fileSlices.map(slice => {
              if (slice.getBaseFile.isPresent) {
                slice.getBaseFile.get().getFileStatus
              } else if (includeLogFiles && slice.getLogFiles.findAny().isPresent) {
                slice.getLogFiles.findAny().get().getFileStatus
              } else {
                null
              }
            }).filter(slice => slice != null)
            val c = fileSlices.filter(f => (includeLogFiles && f.getLogFiles.findAny().isPresent)
              || (f.getBaseFile.isPresent && f.getBaseFile.get().getBootstrapBaseFile.isPresent)).
              foldLeft(Map[String, FileSlice]()) { (m, f) => m + (f.getFileId -> f) }
            if (c.nonEmpty) {
              sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
                new HoodiePartitionFileSliceMapping(partitionValues, c), baseFileStatusesAndLogFileOnly)
            } else {
              sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
                partitionValues, baseFileStatusesAndLogFileOnly)
            }
          } else {
            val allCandidateFiles: Seq[FileStatus] = fileSlices.flatMap(fs => {
              val baseFileStatusOpt = getBaseFileStatus(Option.apply(fs.getBaseFile.orElse(null)))
              val logFilesStatus = if (includeLogFiles) {
                fs.getLogFiles.map[FileStatus](JFunction.toJavaFunction[HoodieLogFile, FileStatus](lf => lf.getFileStatus))
              } else {
                java.util.stream.Stream.empty()
              }
              val files = logFilesStatus.collect(Collectors.toList[FileStatus]).asScala
              baseFileStatusOpt.foreach(f => files.append(f))
              files
            })
            sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(
              partitionValues, allCandidateFiles)
          }
      }.toSeq

      if (shouldReadAsPartitionedTable()) {
        prunedPartitionsAndFilteredFileSlices
      } else if (shouldEmbedFileSlices) {
        assert(partitionSchema.isEmpty)
        prunedPartitionsAndFilteredFileSlices
      } else {
        Seq(PartitionDirectory(InternalRow.empty, prunedPartitionsAndFilteredFileSlices.flatMap(_.files)))
      }
    }
  }

  override def inputFiles: Array[String] = {
    val fileSlices = mergeOnReadIncrementalRelation.listFileSplits(Seq.empty, Seq.empty)
    if (fileSlices.isEmpty) {
      Array.empty
    }
    fileSlices.values.flatten.flatMap(fs => {
      val baseFileStatusOpt = getBaseFileStatus(Option.apply(fs.getBaseFile.orElse(null)))
      val logFilesStatus = if (includeLogFiles) {
        fs.getLogFiles.map[FileStatus](JFunction.toJavaFunction[HoodieLogFile, FileStatus](lf => lf.getFileStatus))
      } else {
        java.util.stream.Stream.empty()
      }
      val files = logFilesStatus.collect(Collectors.toList[FileStatus]).asScala
      baseFileStatusOpt.foreach(f => files.append(f))
      files
    }).map(fileStatus => fileStatus.getPath.toString).toArray
  }

  def getRequiredFilters: Seq[Filter] = {
    mergeOnReadIncrementalRelation.getRequiredFilters
  }
}
