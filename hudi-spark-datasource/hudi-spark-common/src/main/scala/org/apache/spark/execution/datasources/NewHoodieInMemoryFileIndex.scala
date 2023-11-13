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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.timeline.TimelineUtils.validateTimestampAsOf
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils.isNullOrEmpty
import org.apache.hudi.hadoop.CachingPath
import org.apache.hudi.{AvroConversionUtils, BaseFileOnlyRelation, DataSourceReadOptions, HoodieFileIndex, HoodieFileIndexTrait, HoodiePartitionFileSliceMapping, SparkAdapterSupport}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.types.StructType

import java.net.URI
import scala.collection.JavaConverters._


class NewHoodieInMemoryFileIndex(sparkSession: SparkSession,
                                 rootPathsSpecified: Seq[Path],
                                 parameters: Map[String, String],
                                 userSpecifiedSchema: Option[StructType],
                                 fileStatusCache: FileStatusCache = NoopCache,
                                 metaClient: HoodieTableMetaClient,
                                 includeLogFiles: Boolean = false,
                                 relation: BaseFileOnlyRelation,
                                 userSpecifiedPartitionSpec: Option[PartitionSpec]) extends HoodieInMemoryFileIndex(sparkSession,
  rootPathsSpecified, parameters, userSpecifiedSchema, fileStatusCache) with SparkAdapterSupport with HoodieFileIndexTrait {

  val schema: StructType = userSpecifiedSchema.getOrElse({
    val schemaUtil = new TableSchemaResolver(metaClient)
    AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
  })

  protected lazy val specifiedQueryTimestamp: Option[String] =
    parameters.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key)
      .map(HoodieSqlCommonUtils.formatQueryInstant)

  private def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.getTimestamp))
  private lazy val basePath: Path = metaClient.getBasePathV2
  private def timeline: HoodieTimeline = metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants
  override def partitionSpec(): PartitionSpec = userSpecifiedPartitionSpec.get

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val partitions = queryTimestamp match {
      case Some(ts) =>
        specifiedQueryTimestamp.foreach(t => validateTimestampAsOf(metaClient, t))
        val partitionDirs = super.listFiles(HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters), dataFilters)
        val fsView = new HoodieTableFileSystemView(metaClient, timeline, partitionDirs.flatMap(_.files).toArray)

        fsView.getPartitionPaths.asScala.map { partitionPath =>
          val relativePath = FSUtils.getRelativePartitionPath(basePath, partitionPath)
          (relativePath, fsView.getLatestMergedFileSlicesBeforeOrOn(relativePath, ts).iterator().asScala.toSeq)
        }
      case _ => Seq.empty
    }

    partitions.map(p => {
      val partitionPath = relation.getPartitionColumnsAsInternalRowWithRelativePath(p._1)
      val baseFileStatusesAndLogFileOnly: Seq[FileStatus] = p._2.map(slice => {
        if (slice.getBaseFile.isPresent) {
          slice.getBaseFile.get().getFileStatus
        } else if (includeLogFiles && slice.getLogFiles.findAny().isPresent) {
          slice.getLogFiles.findAny().get().getFileStatus
        } else {
          null
        }
      }).filter(slice => slice != null)
      val c = p._2.filter(f => (includeLogFiles && f.getLogFiles.findAny().isPresent)
        || (f.getBaseFile.isPresent && f.getBaseFile.get().getBootstrapBaseFile.isPresent)).
        foldLeft(Map[String, FileSlice]()) { (m, f) => m + (f.getFileId -> f) }
      if (c.nonEmpty) {
        PartitionDirectory(new HoodiePartitionFileSliceMapping(partitionPath, c), baseFileStatusesAndLogFileOnly)
      } else {
        PartitionDirectory(partitionPath, baseFileStatusesAndLogFileOnly)
      }

    })
  }

  def dataSchema: StructType = {
    val partitionColumns = partitionSchema.fieldNames
    StructType(schema.fields.filterNot(f => partitionColumns.contains(f.name)))
  }
}


object NewHoodieInMemoryFileIndex {


  def makeNewHoodieInMemoryFileIndex(sparkSession: SparkSession,
                                     rootPathsSpecified: Seq[Path],
                                     parameters: Map[String, String],
                                     userSpecifiedSchema: Option[StructType],
                                     fileStatusCache: FileStatusCache = NoopCache,
                                     metaClient: HoodieTableMetaClient,
                                     includeLogFiles: Boolean = false): NewHoodieInMemoryFileIndex  = {

    val schema: StructType = userSpecifiedSchema.getOrElse({
      val schemaUtil = new TableSchemaResolver(metaClient)
      AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
    })

    val relation: BaseFileOnlyRelation = BaseFileOnlyRelation(
      sparkSession.sqlContext, metaClient, parameters, userSpecifiedSchema, rootPathsSpecified, userSpecifiedSchema)

    val basePath = metaClient.getBasePathV2

    val partitionColumns = metaClient.getTableConfig.getPartitionFields.orElse(Array.empty)

    val tablePathWithoutScheme = CachingPath.getPathWithoutSchemeAndAuthority(basePath)
    val paths = rootPathsSpecified.map(p => {
      val partitionPathWithoutScheme = CachingPath.getPathWithoutSchemeAndAuthority(p.getParent)
      new URI(tablePathWithoutScheme.toString).relativize(new URI(partitionPathWithoutScheme.toString)).toString
    }).filterNot(isNullOrEmpty).distinct.map(r =>
      PartitionPath(relation.getPartitionColumnsAsInternalRowWithRelativePath(r), new Path(basePath, r)))
    val partitionSpec = Some(PartitionSpec(StructType(schema.fields.filter(f => partitionColumns.contains(f.name))), paths))

    new NewHoodieInMemoryFileIndex(sparkSession, rootPathsSpecified, parameters, userSpecifiedSchema, fileStatusCache,
      metaClient, includeLogFiles, relation, partitionSpec)

  }
}
