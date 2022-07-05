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

package org.apache.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobPattern, Path}
import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.HoodieBaseRelation.getPartitionPath
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieFileIndex.getConfigProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.{getCommitMetadata, getWritePartitionPaths, listAffectedFilesForCommits}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class CopyOnWriteIncrementalRelation(sqlContext: SQLContext,
                                     optParams: Map[String, String],
                                     userSchema: Option[StructType],
                                     metaClient: HoodieTableMetaClient)
  extends BaseFileOnlyRelation(sqlContext, metaClient, optParams, userSchema, Seq())
    with HoodieIncrementalRelationTrait {

  override type FileSplit = HoodieBaseFileSplit

  // TODO(HUDI-3204) this is to override behavior (exclusively) for COW tables to always extract
  //                 partition values from partition path
  //                 For more details please check HUDI-4161
  // NOTE: This override has to mirror semantic of whenever this Relation is converted into [[HadoopFsRelation]],
  //       which is currently done for all cases, except when Schema Evolution is enabled
  override protected val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    val enableSchemaOnRead = !internalSchema.isEmptySchema
    !enableSchemaOnRead
  }

  private lazy val sparkParsePartitionUtil = sparkAdapter.createSparkParsePartitionUtil(sqlContext.sparkSession.sessionState.conf)

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }

  override protected def timeline: HoodieTimeline = {
    val startTimestamp = optParams(DataSourceReadOptions.BEGIN_INSTANTTIME.key)
    val endTimestamp = optParams.getOrElse(DataSourceReadOptions.END_INSTANTTIME.key, super.timeline.lastInstant().get.getTimestamp)
    super.timeline.findInstantsInRange(startTimestamp, endTimestamp)
  }

  protected override def composeRDD(fileSplits: Seq[HoodieBaseFileSplit],
                                    partitionSchema: StructType,
                                    dataSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    filters: Array[Filter]): HoodieUnsafeRDD = {
    val baseFileReader = createBaseFileReader(
      spark = sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredSchema = requiredSchema,
      // Adding the filters based on the incremental commit time span so we're only fetching
      // records that fall into incremental span of the timeline being queried
      filters = incrementalSpanRecordFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = HoodieDataSourceHelper.getConfigurationWithInternalSchema(new Configuration(conf), requiredSchema.internalSchema, metaClient.getBasePath, validCommits)
    )

    val serDe = sparkAdapter.createSparkRowSerDe(RowEncoder(requiredSchema.structTypeSchema).resolveAndBind())
    val records = createBaseFileReader(
      spark = sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredSchema = requiredSchema,
      // Adding the filters based on the incremental commit time span so we're only fetching
      // records that fall into incremental span of the timeline being queried
      filters = incrementalSpanRecordFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = HoodieDataSourceHelper.getConfigurationWithInternalSchema(new Configuration(conf), requiredSchema.internalSchema, metaClient.getBasePath, validCommits)
    ).apply(fileSplits.head.filePartition.files(0)).toList.map(
      row => {
        serDe.deserializeRow(row)
      })

    new HoodieFileScanRDD(sparkSession, baseFileReader, fileSplits)
  }

  override protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[HoodieBaseFileSplit] = {
    if (includedCommits.isEmpty) {
      List()
    } else {
      val commitsMetadata = includedCommits.map(getCommitMetadata(_, timeline)).asJava

      val partitionFiles = listAffectedFilesForCommits(
        conf, new Path(metaClient.getBasePath), commitsMetadata).toSeq.groupBy(getPartitionPath)
      val partitionSchema = if (shouldExtractPartitionValuesFromPartitionPath) {
        StructType(partitionColumns.map(StructField(_, StringType)))
      } else {
        StructType(Nil)
      }
      val prunedPartitionList = prunePartition(partitionSchema, partitionFiles.keySet.toSeq, partitionFilters)
      val prunedFiles = partitionFiles.filter(e => prunedPartitionList.contains(e._1))
        .values.flatten.toSeq
      val modifiedFiles = filterFileStatus(prunedFiles, globPattern)
      val fileSplits = modifiedFiles.flatMap { file =>
        // TODO fix, currently assuming parquet as underlying format
        HoodieDataSourceHelper.splitFiles(
          sparkSession = sparkSession,
          file = file,
          partitionValues = getPartitionColumnsAsInternalRow(file)
        )
      }
        .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
      val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
      sparkAdapter.getFilePartitions(sparkSession, fileSplits, maxSplitBytes)
        .map(HoodieBaseFileSplit.apply)
    }
  }

  private def filterFileStatus(files: Seq[FileStatus], pathGlobPattern: String): Seq[FileStatus] = {
    val filteredFiles = if (!StringUtils.isNullOrEmpty(pathGlobPattern)) {
      val globMatcher = new GlobPattern("*" + pathGlobPattern)
      files.filter(file => {
        val path = file.getPath.toString
        globMatcher.matches(path)
      })
    } else {
      files
    }
    filteredFiles
  }

  protected def prunePartition(partitionSchema: StructType,
                               partitionPaths: Seq[Path],
                               predicates: Seq[Expression]): Seq[Path] = {
    val partitionColumnNames = partitionSchema.fields.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = InterpretedPredicate(predicate.transform {
        case a: AttributeReference =>
          val index = partitionSchema.indexWhere(a.name == _.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
      })
      val timeZoneId = getConfigProperties(sqlContext.sparkSession, optParams)
        .getString(DateTimeUtils.TIMEZONE_OPTION, SQLConf.get.sessionLocalTimeZone)
      val partitionDataTypes = partitionSchema.map(f => f.name -> f.dataType).toMap
      val prunedPartitionPaths = partitionPaths.filter {
        partitionPath => {
          val relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), partitionPath)
          boundPredicate.eval(InternalRow.fromSeq(Seq(UTF8String.fromString(relativePartitionPath))))
        }
      }

      logInfo(s"Total partition size is: ${partitionPaths.size}," +
        s" after partition prune size is: ${prunedPartitionPaths.size}")
      prunedPartitionPaths
    } else {
      partitionPaths
    }
  }
}
