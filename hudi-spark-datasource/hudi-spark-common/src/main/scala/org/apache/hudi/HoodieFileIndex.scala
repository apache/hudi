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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.HoodieDatasetUtils.withPersistence
import org.apache.hudi.HoodieFileIndex.{collectReferencedColumns, getConfigProperties}
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper.{getMaxColumnNameFor, getMinColumnNameFor, getNumNullsColumnNameFor}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata, MetadataPartitionType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{AnalysisException, Column, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import java.text.SimpleDateFormat
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * A file index which support partition prune for hoodie snapshot and read-optimized query.
 *
 * Main steps to get the file list for query:
 * 1、Load all files and partition values from the table path.
 * 2、Do the partition prune by the partition filter condition.
 *
 * There are 3 cases for this:
 * 1、If the partition columns size is equal to the actually partition path level, we
 * read it as partitioned table.(e.g partition column is "dt", the partition path is "2021-03-10")
 *
 * 2、If the partition columns size is not equal to the partition path level, but the partition
 * column size is "1" (e.g. partition column is "dt", but the partition path is "2021/03/10"
 * who's directory level is 3).We can still read it as a partitioned table. We will mapping the
 * partition path (e.g. 2021/03/10) to the only partition column (e.g. "dt").
 *
 * 3、Else the the partition columns size is not equal to the partition directory level and the
 * size is great than "1" (e.g. partition column is "dt,hh", the partition path is "2021/03/10/12")
 * , we read it as a Non-Partitioned table because we cannot know how to mapping the partition
 * path with the partition columns in this case.
 *
 * TODO rename to HoodieSparkSqlFileIndex
 */
case class HoodieFileIndex(spark: SparkSession,
                           metaClient: HoodieTableMetaClient,
                           schemaSpec: Option[StructType],
                           options: Map[String, String],
                           @transient fileStatusCache: FileStatusCache = NoopCache)
  extends SparkHoodieTableFileIndex(
    spark = spark,
    metaClient = metaClient,
    schemaSpec = schemaSpec,
    configProperties = getConfigProperties(spark, options),
    queryPaths = Seq(HoodieFileIndex.getQueryPath(options)),
    specifiedQueryInstant = options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key).map(HoodieSqlCommonUtils.formatQueryInstant),
    fileStatusCache = fileStatusCache
  )
    with FileIndex {

  override def rootPaths: Seq[Path] = queryPaths.asScala

  def isDataSkippingEnabled(): Boolean = {
    options.getOrElse(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(),
      spark.sessionState.conf.getConfString(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(), "false")).toBoolean
  }

  /**
   * Returns the FileStatus for all the base files (excluding log files). This should be used only for
   * cases where Spark directly fetches the list of files via HoodieFileIndex or for read optimized query logic
   * implemented internally within Hudi like HoodieBootstrapRelation. This helps avoid the use of path filter
   * to filter out log files within Spark.
   *
   * @return List of FileStatus for base files
   */
  def allFiles: Seq[FileStatus] = {
    cachedAllInputFileSlices.values.asScala.flatMap(_.asScala)
      .filter(_.getBaseFile.isPresent)
      .map(_.getBaseFile.get().getFileStatus)
      .toSeq
  }

  /**
   * Invoked by Spark to fetch list of latest base files per partition.
   *
   * @param partitionFilters partition column filters
   * @param dataFilters      data columns filters
   * @return list of PartitionDirectory containing partition to base files mapping
   */
  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val convertedPartitionFilters =
      HoodieFileIndex.convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)

    // Look up candidate files names in the col-stats index, if all of the following conditions are true
    //    - Data-skipping is enabled
    //    - Col-Stats Index is present
    //    - List of predicates (filters) is present
    val candidateFilesNamesOpt: Option[Set[String]] =
      lookupCandidateFilesInMetadataTable(dataFilters) match {
        case Success(opt) => opt
        case Failure(e) =>
          logError("Failed to lookup candidate files in Z-index", e)
          Option.empty
      }

    logDebug(s"Overlapping candidate files from Column Stats Index: ${candidateFilesNamesOpt.getOrElse(Set.empty)}")

    if (queryAsNonePartitionedTable) {
      // Read as Non-Partitioned table
      // Filter in candidate files based on the col-stats index lookup
      val candidateFiles = allFiles.filter(fileStatus =>
        // NOTE: This predicate is true when {@code Option} is empty
        candidateFilesNamesOpt.forall(_.contains(fileStatus.getPath.getName))
      )

      logInfo(s"Total files : ${allFiles.size}; " +
        s"candidate files after data skipping: ${candidateFiles.size}; " +
        s"skipping percent ${if (allFiles.nonEmpty) (allFiles.size - candidateFiles.size) / allFiles.size.toDouble else 0}")

      Seq(PartitionDirectory(InternalRow.empty, candidateFiles))
    } else {
      // Prune the partition path by the partition filters
      val prunedPartitions = prunePartition(cachedAllInputFileSlices.keySet.asScala.toSeq, convertedPartitionFilters)
      var totalFileSize = 0
      var candidateFileSize = 0

      val result = prunedPartitions.map { partition =>
        val baseFileStatuses: Seq[FileStatus] =
          cachedAllInputFileSlices.get(partition).asScala
            .map(fs => fs.getBaseFile.orElse(null))
            .filter(_ != null)
            .map(_.getFileStatus)

        // Filter in candidate files based on the col-stats index lookup
        val candidateFiles = baseFileStatuses.filter(fs =>
          // NOTE: This predicate is true when {@code Option} is empty
          candidateFilesNamesOpt.forall(_.contains(fs.getPath.getName)))

        totalFileSize += baseFileStatuses.size
        candidateFileSize += candidateFiles.size
        PartitionDirectory(InternalRow.fromSeq(partition.values), candidateFiles)
      }

      logInfo(s"Total base files: ${totalFileSize}; " +
        s"candidate files after data skipping : ${candidateFileSize}; " +
        s"skipping percent ${if (allFiles.nonEmpty) (totalFileSize - candidateFileSize) / totalFileSize.toDouble else 0}")

      result
    }
  }

  private def lookupFileNamesMissingFromIndex(allIndexedFileNames: Set[String]) = {
    val allBaseFileNames = allFiles.map(f => f.getPath.getName).toSet
    allBaseFileNames -- allIndexedFileNames
  }

  /**
   * Computes pruned list of candidate base-files' names based on provided list of {@link dataFilters}
   * conditions, by leveraging Metadata Table's Column Statistics index (hereon referred as ColStats for brevity)
   * bearing "min", "max", "num_nulls" statistics for all columns.
   *
   * NOTE: This method has to return complete set of candidate files, since only provided candidates will
   * ultimately be scanned as part of query execution. Hence, this method has to maintain the
   * invariant of conservatively including every base-file's name, that is NOT referenced in its index.
   *
   * @param queryFilters list of original data filters passed down from querying engine
   * @return list of pruned (data-skipped) candidate base-files' names
   */
  private def lookupCandidateFilesInMetadataTable(queryFilters: Seq[Expression]): Try[Option[Set[String]]] = Try {
    val fs = metaClient.getFs
    val metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(basePath)

    if (!isDataSkippingEnabled() || !fs.exists(new Path(metadataTablePath)) || queryFilters.isEmpty) {
      Option.empty
    } else {
      val targetColStatsIndexColumns = Seq(
        HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
        HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
        HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
        HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT)

      val requiredMetadataIndexColumns =
        (targetColStatsIndexColumns :+ HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME).map(colName =>
          s"${HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS}.${colName}")

      // Read Metadata Table's Column Stats Index into Spark's [[DataFrame]]
      val metadataTableDF = spark.read.format("org.apache.hudi")
        .load(s"$metadataTablePath/${MetadataPartitionType.COLUMN_STATS.getPartitionPath}")

      // TODO filter on (column, partition) prefix
      val colStatsDF = metadataTableDF.where(col(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).isNotNull)
        .select(requiredMetadataIndexColumns.map(col): _*)

      val queryReferencedColumns = collectReferencedColumns(spark, queryFilters, schema)

      // Persist DF to avoid re-computing column statistics unraveling
      withPersistence(colStatsDF) {
        // Metadata Table bears rows in the following format
        //
        //  +---------------------------+------------+------------+------------+-------------+
        //  |        fileName           | columnName |  minValue  |  maxValue  |  num_nulls  |
        //  +---------------------------+------------+------------+------------+-------------+
        //  | one_base_file.parquet     |          A |          1 |         10 |           0 |
        //  | another_base_file.parquet |          A |        -10 |          0 |           5 |
        //  +---------------------------+------------+------------+------------+-------------+
        //
        // While Data Skipping utils are expecting following (transposed) format, where per-column stats are
        // essentially transposed (from rows to columns):
        //
        //  +---------------------------+------------+------------+-------------+
        //  |          file             | A_minValue | A_maxValue | A_num_nulls |
        //  +---------------------------+------------+------------+-------------+
        //  | one_base_file.parquet     |          1 |         10 |           0 |
        //  | another_base_file.parquet |        -10 |          0 |           5 |
        //  +---------------------------+------------+------------+-------------+
        //
        // NOTE: Column Stats Index might potentially contain statistics for many columns (if not all), while
        //       query at hand might only be referencing a handful of those. As such, we collect all the
        //       column references from the filtering expressions, and only transpose records corresponding to the
        //       columns referenced in those
        val transposedColStatsDF =
        queryReferencedColumns.map(colName =>
          colStatsDF.filter(col(HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME).equalTo(colName))
            .select(targetColStatsIndexColumns.map(col): _*)
            .withColumnRenamed(HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT, getNumNullsColumnNameFor(colName))
            .withColumnRenamed(HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE, getMinColumnNameFor(colName))
            .withColumnRenamed(HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE, getMaxColumnNameFor(colName))
        )
          .reduceLeft((left, right) =>
            left.join(right, usingColumn = HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME))

        // Persist DF to avoid re-computing column statistics unraveling
        withPersistence(transposedColStatsDF) {
          val indexSchema = transposedColStatsDF.schema
          val indexFilter =
            queryFilters.map(translateIntoColumnStatsIndexFilterExpr(_, indexSchema))
              .reduce(And)

          val allIndexedFileNames =
            transposedColStatsDF.select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
              .collect()
              .map(_.getString(0))
              .toSet

          val prunedCandidateFileNames =
            transposedColStatsDF.where(new Column(indexFilter))
              .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
              .collect()
              .map(_.getString(0))
              .toSet

          // NOTE: Col-Stats Index isn't guaranteed to have complete set of statistics for every
          //       base-file: since it's bound to clustering, which could occur asynchronously
          //       at arbitrary point in time, and is not likely to be touching all of the base files.
          //
          //       To close that gap, we manually compute the difference b/w all indexed (by col-stats-index)
          //       files and all outstanding base-files, and make sure that all base files not
          //       represented w/in the index are included in the output of this method
          val notIndexedFileNames = lookupFileNamesMissingFromIndex(allIndexedFileNames)

          Some(prunedCandidateFileNames ++ notIndexedFileNames)
        }
      }
    }
  }

  override def refresh(): Unit = super.refresh()

  override def inputFiles: Array[String] = {
    val fileStatusList = allFiles
    fileStatusList.map(_.getPath.toString).toArray
  }

  override def sizeInBytes: Long = {
    cachedFileSize
  }
}

object HoodieFileIndex extends Logging {

  private def collectReferencedColumns(spark: SparkSession, queryFilters: Seq[Expression], schema: StructType): Seq[String] = {
    val resolver = spark.sessionState.analyzer.resolver
    val refs = queryFilters.flatMap(_.references)
    schema.fieldNames.filter { colName => refs.exists(r => resolver.apply(colName, r.name)) }
  }

  def getConfigProperties(spark: SparkSession, options: Map[String, String]) = {
    val sqlConf: SQLConf = spark.sessionState.conf
    val properties = new TypedProperties()

    // To support metadata listing via Spark SQL we allow users to pass the config via SQL Conf in spark session. Users
    // would be able to run SET hoodie.metadata.enable=true in the spark sql session to enable metadata listing.
    properties.setProperty(HoodieMetadataConfig.ENABLE.key(),
      sqlConf.getConfString(HoodieMetadataConfig.ENABLE.key(),
        HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS.toString))
    properties.putAll(options.asJava)
    properties
  }

  def convertFilterForTimestampKeyGenerator(metaClient: HoodieTableMetaClient,
      partitionFilters: Seq[Expression]): Seq[Expression] = {

    val tableConfig = metaClient.getTableConfig
    val keyGenerator = tableConfig.getKeyGeneratorClassName

    if (keyGenerator != null && (keyGenerator.equals(classOf[TimestampBasedKeyGenerator].getCanonicalName) ||
        keyGenerator.equals(classOf[TimestampBasedAvroKeyGenerator].getCanonicalName))) {
      val inputFormat = tableConfig.getString(KeyGeneratorOptions.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP)
      val outputFormat = tableConfig.getString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP)
      if (StringUtils.isNullOrEmpty(inputFormat) || StringUtils.isNullOrEmpty(outputFormat) ||
          inputFormat.equals(outputFormat)) {
        partitionFilters
      } else {
        try {
          val inDateFormat = new SimpleDateFormat(inputFormat)
          val outDateFormat = new SimpleDateFormat(outputFormat)
          partitionFilters.toArray.map {
            _.transformDown {
              case Literal(value, dataType) if dataType.isInstanceOf[StringType] =>
                val converted = outDateFormat.format(inDateFormat.parse(value.toString))
                Literal(UTF8String.fromString(converted), StringType)
            }
          }
        } catch {
          case NonFatal(e) =>
            logWarning("Fail to convert filters for TimestampBaseAvroKeyGenerator.")
            partitionFilters
        }
      }
    } else {
      partitionFilters
    }
  }

  private def getQueryPath(options: Map[String, String]) = {
    new Path(options.get("path") match {
      case Some(p) => p
      case None => throw new IllegalArgumentException("'path' option required")
    })
  }
}
