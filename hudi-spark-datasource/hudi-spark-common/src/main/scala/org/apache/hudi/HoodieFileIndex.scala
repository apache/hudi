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

import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.HoodieFileIndex.{collectReferencedColumns, convertFilterForTimestampKeyGenerator, getConfigProperties, DataSkippingFailureMode}
import org.apache.hudi.HoodieSparkConfUtils.getConfigValue
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TypedProperties}
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig.{TIMESTAMP_INPUT_DATE_FORMAT, TIMESTAMP_OUTPUT_DATE_FORMAT}
import org.apache.hudi.common.model.{FileSlice, HoodieBaseFile, HoodieLogFile}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.index.bucket.partition.PartitionBucketIndexUtils
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import javax.annotation.concurrent.NotThreadSafe

import java.text.SimpleDateFormat
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

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
 * 3、Else the partition columns size is not equal to the partition directory level and the
 * size is great than "1" (e.g. partition column is "dt,hh", the partition path is "2021/03/10/12")
 * , we read it as a Non-Partitioned table because we cannot know how to mapping the partition
 * path with the partition columns in this case.
 *
 * When flag `shouldUseStringTypeForTimestampPartitionKeyType` is set to true, the schema type for timestamp partition columns
 * in custom key generator is set as STRING instead of the base schema type for that field. This makes sure that with
 * output partition format as DD/MM/YYYY, there are no incompatible schema errors while reading the table.
 * Further the partition values are also updated to string. For example if output partition format is YYYY,
 * it can be represented as integer if the base schema type is integer. But since we are upgrading the schema
 * to STRING for all output formats, we need to update the partition values to STRING format as well to avoid
 * errors.
 *
 * TODO rename to HoodieSparkSqlFileIndex
 */
@NotThreadSafe
case class HoodieFileIndex(spark: SparkSession,
                           metaClient: HoodieTableMetaClient,
                           schemaSpec: Option[StructType],
                           options: Map[String, String],
                           @transient fileStatusCache: FileStatusCache = NoopCache,
                           includeLogFiles: Boolean = false,
                           shouldEmbedFileSlices: Boolean = false)
  extends SparkHoodieTableFileIndex(
    spark = spark,
    metaClient = metaClient,
    schemaSpec = schemaSpec,
    configProperties = getConfigProperties(spark, options, metaClient.getTableConfig),
    queryPaths = HoodieFileIndex.getQueryPaths(options),
    specifiedQueryInstant = options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key).map(HoodieSqlCommonUtils.formatQueryInstant),
    fileStatusCache = fileStatusCache,
    startCompletionTime = options.get(DataSourceReadOptions.START_COMMIT.key),
    endCompletionTime = options.get(DataSourceReadOptions.END_COMMIT.key)) with FileIndex {

  @transient protected var hasPushedDownPartitionPredicates: Boolean = false

  /**
   * NOTE: [[indicesSupport]] is a transient state, since it's only relevant while logical plan
   * is handled by the Spark's driver
   * The order of elements is important as in this order indices will be applied
   * during `lookupCandidateFilesInMetadataTable`
   */
  @transient private lazy val indicesSupport: List[SparkBaseIndexSupport] = List(
    new RecordLevelIndexSupport(spark, metadataConfig, metaClient),
    if (PartitionBucketIndexUtils.isPartitionSimpleBucketIndex(metaClient.getStorageConf, metaClient.getBasePath.toString)) {
      new PartitionBucketIndexSupport(spark, metadataConfig, metaClient,
        options.get(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key).map(HoodieSqlCommonUtils.formatQueryInstant))
    } else {
      new BucketIndexSupport(spark, metadataConfig, metaClient)
    },
    new SecondaryIndexSupport(spark, metadataConfig, metaClient),
    new ExpressionIndexSupport(spark, schema, metadataConfig, metaClient),
    new BloomFiltersIndexSupport(spark, metadataConfig, metaClient),
    new ColumnStatsIndexSupport(spark, schema, metadataConfig, metaClient)
  )

  private val enableHoodieExtension = spark.sessionState.conf.getConfString("spark.sql.extensions", "")
    .split(",")
    .map(_.trim)
    .contains("org.apache.spark.sql.hudi.HoodieSparkSessionExtension")

  override def rootPaths: Seq[Path] = getQueryPaths.asScala.map(e => new Path(e.toUri)).toSeq

  /**
   * Returns the FileStatus for all the base files (excluding log files). This should be used only for
   * cases where Spark directly fetches the list of files via HoodieFileIndex or for read optimized query logic
   * implemented internally within Hudi like HoodieBootstrapRelation. This helps avoid the use of path filter
   * to filter out log files within Spark.
   *
   * @return List of FileStatus for base files
   */
  def allBaseFiles: Seq[StoragePathInfo] = {
    getAllInputFileSlices.values.asScala.flatMap(_.asScala)
      .map(fs => fs.getBaseFile.orElse(null))
      .filter(_ != null)
      .map(_.getPathInfo)
      .toSeq
  }

  /**
   * Returns the FileStatus for all the base files and log files.
   *
   * @return List of FileStatus for base files and log files
   */
  private def allBaseFilesAndLogFiles: Seq[StoragePathInfo] = {
    getAllInputFileSlices.values.asScala.flatMap(_.asScala)
      .flatMap(fs => {
        val baseFileStatusOpt = getBaseFileInfo(Option.apply(fs.getBaseFile.orElse(null)))
        val logFilesStatus = fs.getLogFiles.map[StoragePathInfo](JFunction.toJavaFunction[HoodieLogFile, StoragePathInfo](lf => lf.getPathInfo))
        val files = logFilesStatus.collect(Collectors.toList[StoragePathInfo]).asScala
        baseFileStatusOpt.foreach(f => files.append(f))
        files
      }).toSeq
  }

  /**
   * Invoked by Spark to fetch list of latest base files per partition.
   *
   * @param partitionFilters partition column filters
   * @param dataFilters      data columns filters
   * @return list of PartitionDirectory containing partition to base files mapping
   */
  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val slices = filterFileSlices(dataFilters, partitionFilters).flatMap(
      { case (partitionOpt, fileSlices) =>
        fileSlices.filter(!_.isEmpty).map(fs => ( InternalRow.fromSeq(partitionOpt.get.values), fs))
      }
    )
    prepareFileSlices(slices)
  }

  protected def prepareFileSlices(slices: Seq[(InternalRow, FileSlice)]): Seq[PartitionDirectory] = {
    hasPushedDownPartitionPredicates = true

    val prunedPartitionsAndFilteredFileSlices = slices.map {
      case (partitionValues, fileSlice) =>
        if (shouldEmbedFileSlices) {
          PartitionDirectoryConverter.convertFileSliceToPartitionDirectory(
            partitionValues,
            fileSlice,
            options)
        } else {
          val baseFileStatusOpt = getBaseFileInfo(Option.apply(fileSlice.getBaseFile.orElse(null)))
          val logPathInfoStream = fileSlice.getLogFiles.map[StoragePathInfo](JFunction.toJavaFunction[HoodieLogFile, StoragePathInfo](lf => lf.getPathInfo))
          val files = logPathInfoStream.collect(Collectors.toList[StoragePathInfo]).asScala
          baseFileStatusOpt.foreach(f => files.append(f))
          val allCandidateFiles = files.map(fileInfo => new FileStatus(fileInfo.getLength, fileInfo.isDirectory, 0, fileInfo.getBlockSize,
            fileInfo.getModificationTime, new Path(fileInfo.getPath.toUri))).toSeq
          sparkAdapter.getSparkPartitionedFileUtils.newPartitionDirectory(partitionValues, allCandidateFiles)
        }
    }

    if (shouldReadAsPartitionedTable()) {
      prunedPartitionsAndFilteredFileSlices
    } else if (shouldEmbedFileSlices) {
      assert(partitionSchema.isEmpty)
      prunedPartitionsAndFilteredFileSlices
    } else {
      Seq(PartitionDirectory(InternalRow.empty, prunedPartitionsAndFilteredFileSlices.flatMap(_.files)))
    }
  }

  /**
   * The functions prunes the partition paths based on the input partition filters. For every partition path, the file
   * slices are further filtered after querying metadata table based on the data filters.
   *
   * @param dataFilters data columns filters
   * @param partitionFilters partition column filters
   * @param partitionPrune for HoodiePruneFileSourcePartitions rule only prune partitions
   * @return A sequence of pruned partitions and corresponding filtered file slices
   */
  def filterFileSlices(dataFilters: Seq[Expression], partitionFilters: Seq[Expression], isPartitionPruned: Boolean = false)
  : Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])] = {

    val (isPruned, prunedPartitionsAndFileSlices) =
      prunePartitionsAndGetFileSlices(dataFilters, partitionFilters)
    hasPushedDownPartitionPredicates = true

    // If there are no data filters, return all the file slices.
    // If isPartitionPurge is true, this fun is trigger by HoodiePruneFileSourcePartitions, don't look up candidate files
    // If there are no file slices, return empty list.
    if (prunedPartitionsAndFileSlices.isEmpty || dataFilters.isEmpty || isPartitionPruned ) {
      prunedPartitionsAndFileSlices
    } else {
      // Look up candidate files names in the col-stats or record level index, if all of the following conditions are true
      //    - Data-skipping is enabled
      //    - Col-Stats Index is present
      //    - Record-level Index is present
      //    - List of predicates (filters) is present
      val candidateFilesNamesOpt: Option[Set[String]] =
        lookupCandidateFilesInMetadataTable(dataFilters, prunedPartitionsAndFileSlices, isPruned) match {
        case Success(opt) => opt
        case Failure(e) =>
          logError("Failed to lookup candidate files in File Index", e)

          spark.sqlContext.getConf(DataSkippingFailureMode.configName, DataSkippingFailureMode.Fallback.value) match {
            case DataSkippingFailureMode.Fallback.value => Option.empty
            case DataSkippingFailureMode.Strict.value => throw new HoodieException(e);
          }
      }

      logDebug(s"Overlapping candidate files from Column Stats or Record Level Index: ${candidateFilesNamesOpt.getOrElse(Set.empty)}")

      var totalFileSliceSize = 0
      var candidateFileSliceSize = 0

      val prunedPartitionsAndFilteredFileSlices = prunedPartitionsAndFileSlices.map {
        case (partitionOpt, fileSlices) =>
          // Filter in candidate files based on the col-stats or record level index lookup
          val candidateFileSlices: Seq[FileSlice] = {
            fileSlices.filter(fs => {
              val fileSliceFiles = fs.getLogFiles.map[String](JFunction.toJavaFunction[HoodieLogFile, String](lf => lf.getPath.getName))
                .collect(Collectors.toSet[String])
              val baseFileStatusOpt = getBaseFileInfo(Option.apply(fs.getBaseFile.orElse(null)))
              baseFileStatusOpt.exists(f => fileSliceFiles.add(f.getPath.getName))
              if (candidateFilesNamesOpt.isDefined) {
                // if any file in the file slice is part of candidate file names, we need to inclue the file slice.
                // in other words, if all files in the file slice is not present in candidate file names, we can filter out the file slice.
                fileSliceFiles.stream().filter(fileSliceFile => candidateFilesNamesOpt.get.contains(fileSliceFile)).findAny().isPresent
              } else {
                true
              }
            })
          }

          totalFileSliceSize += fileSlices.size
          candidateFileSliceSize += candidateFileSlices.size
          (partitionOpt, candidateFileSlices)
      }

      val skippingRatio =
        if (!areAllFileSlicesCached) -1
        else if (getAllFiles().nonEmpty && totalFileSliceSize > 0)
          (totalFileSliceSize - candidateFileSliceSize) / totalFileSliceSize.toDouble
        else 0

      logInfo(s"Total file slices: $totalFileSliceSize; " +
        s"candidate file slices after data skipping: $candidateFileSliceSize; " +
        s"skipping percentage $skippingRatio")

      prunedPartitionsAndFilteredFileSlices
    }
  }

  /**
   * Prunes table partitions to list if possible.
   *
   * @param dataFilters      filters based on data columns
   * @param partitionFilters filters based on partition columns
   * @return a pair of elements, with the first element indicating whether the partition pruning
   *         is applied, and the second element as a list of partition paths and file slices
   */
  def prunePartitionsAndGetFileSlices(dataFilters: Seq[Expression],
                                      partitionFilters: Seq[Expression]):
  (Boolean, Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])]) = {
    val isPartitionedTable = getPartitionColumns.length > 0
    val prunedPartitionsTuple: (Boolean, Seq[PartitionPath]) =
      if (isPartitionedTable && partitionFilters.nonEmpty) {
        // For partitioned table and partition filters, prune the partitions by the partition filters
        if (shouldEmbedFileSlices) {
          (true, listMatchingPartitionPaths(convertFilterForTimestampKeyGenerator(metaClient, partitionFilters)))
        } else {
          (true, listMatchingPartitionPaths(partitionFilters))
        }
      } else if (isPartitionedTable && isDataSkippingEnabled) {
        // For partitioned table and no partition filters, if data skipping is enabled,
        // try using the PARTITION_STATS index to prune the partitions
        val prunedPartitionPaths = new PartitionStatsIndexSupport(spark, schema, metadataConfig, metaClient)
          .prunePartitions(this, dataFilters)
        if (prunedPartitionPaths.nonEmpty) {
          try {
            (true, prunedPartitionPaths.get.map(e => convertToPartitionPath(e)).toSeq)
          } catch {
            // If the partition values cannot be parsed by [[convertToPartitionPath]],
            // fall back to listing all partitions
            case _: HoodieException => (false, listMatchingPartitionPaths(Seq.empty))
          }
        } else if (isExpressionIndexEnabled) {
          val expressionIndexSupport = getExpressionIndexSupport
          lazy val filterReferencedColumns = collectReferencedColumns(spark, dataFilters, schema)
          val exprIndexPrunedPartitionsOpt = expressionIndexSupport.prunePartitions(this, dataFilters, filterReferencedColumns)
          if (exprIndexPrunedPartitionsOpt.nonEmpty) {
            (true, exprIndexPrunedPartitionsOpt.get.map(e => convertToPartitionPath(e)).toSeq)
          } else {
            // Cannot use expression index for pruning partitions,
            // fall back to listing all partitions
            (false, listMatchingPartitionPaths(Seq.empty))
          }
        } else {
          // Both partition stats pruning and expression index pruning was not helpful
          // Falling back to listing all partitions
          (false, listMatchingPartitionPaths(Seq.empty))
        }
      } else {
        // Listing all partitions for non-partitioned table,
        // or partitioned table without partition filter or data skipping or PARTITION_STATS index
        (false, listMatchingPartitionPaths(Seq.empty))
      }

    (prunedPartitionsTuple._1, getInputFileSlices(prunedPartitionsTuple._2: _*).asScala.map(
      { case (partition, fileSlices) =>
        (Option.apply(partition), fileSlices.asScala.map(f => f.withLogFiles(includeLogFiles)).toSeq) }).toSeq)
  }

  private def getExpressionIndexSupport: ExpressionIndexSupport = {
    indicesSupport.find(indexSupport => indexSupport.isInstanceOf[ExpressionIndexSupport]).get.asInstanceOf[ExpressionIndexSupport]
  }

  /**
   * In the fast bootstrap read code path, it gets the path info for the bootstrap base file instead of
   * skeleton file. Returns path info for the base file if available.
   */
  protected def getBaseFileInfo(baseFileOpt: Option[HoodieBaseFile]): Option[StoragePathInfo] = {
    baseFileOpt.map(baseFile => {
      if (shouldFastBootstrap) {
        if (baseFile.getBootstrapBaseFile.isPresent) {
          baseFile.getBootstrapBaseFile.get().getPathInfo
        } else {
          baseFile.getPathInfo
        }
      } else {
        baseFile.getPathInfo
      }
    })
  }

  /**
   * Computes pruned list of candidate base-files' names based on provided list of {@link dataFilters}
   * conditions, by leveraging Metadata Table's Record Level Index and Column Statistics index (hereon referred as
   * ColStats for brevity) bearing "min", "max", "num_nulls" statistics for all columns.
   *
   * NOTE: This method has to return complete set of candidate files, since only provided candidates will
   * ultimately be scanned as part of query execution. Hence, this method has to maintain the
   * invariant of conservatively including every base-file and log file's name, that is NOT referenced in its index.
   *
   * @param queryFilters list of original data filters passed down from querying engine
   * @return list of pruned (data-skipped) candidate base-files and log files' names
   */
  // scalastyle:off return
  private def lookupCandidateFilesInMetadataTable(queryFilters: Seq[Expression],
                                                  prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                                  shouldPushDownFilesFilter: Boolean): Try[Option[Set[String]]] = Try {
    // NOTE: For column stats, Data Skipping is only effective when it references columns that are indexed w/in
    //       the Column Stats Index (CSI). Following cases could not be effectively handled by Data Skipping:
    //          - Expressions on top-level column's fields (ie, for ex filters like "struct.field > 0", since
    //          CSI only contains stats for top-level columns, in this case for "struct")
    //          - Any expression not directly referencing top-level column (for ex, sub-queries, since there's
    //          nothing CSI in particular could be applied for)
    //       For record index, Data Skipping is only effective when one of the query filter is of type EqualTo
    //       or IN query on simple record keys. In such a case the record index is used to filter the file slices
    //       and candidate files are obtained from these file slices.

    lazy val queryReferencedColumns = collectReferencedColumns(spark, queryFilters, schema)
    if (isDataSkippingEnabled) {
      for(indexSupport: SparkBaseIndexSupport <- indicesSupport) {
        if (indexSupport.isIndexAvailable && indexSupport.supportsQueryType(options)) {
          val prunedFileNames = indexSupport.computeCandidateIsStrict(spark, this, queryFilters, queryReferencedColumns,
            prunedPartitionsAndFileSlices, shouldPushDownFilesFilter)
          if (prunedFileNames.nonEmpty) {
            return Try(prunedFileNames)
          }
        }
      }
    }
    validateConfig()
    Option.empty
  }

  override def refresh(): Unit = {
    super.refresh()
    indicesSupport.foreach(idx => idx.invalidateCaches())
    hasPushedDownPartitionPredicates = false
  }

  private def getAllFiles(): Seq[StoragePathInfo] = {
    if (includeLogFiles) allBaseFilesAndLogFiles else allBaseFiles
  }

  override def inputFiles: Array[String] =
    getAllFiles().map(_.getPath.toString).toArray

  override def sizeInBytes: Long = {
    val size = getTotalCachedFilesSize
    if (size == 0 && !enableHoodieExtension) {
      // Avoid always broadcast the hudi table if not enable HoodieExtension
      logWarning("Note: Please add 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension' to the Spark SQL configuration property " +
        "'spark.sql.extensions'.\n Multiple extensions can be set using a comma-separated list.")
      Long.MaxValue
    } else {
      size
    }
  }

  def hasPredicatesPushedDown: Boolean =
    hasPushedDownPartitionPredicates

  private def isDataSkippingEnabled: Boolean = getConfigValue(options, spark.sessionState.conf,
    DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, DataSourceReadOptions.ENABLE_DATA_SKIPPING.defaultValue.toString).toBoolean

  private def isMetadataTableEnabled: Boolean = metadataConfig.isEnabled()

  private def isColumnStatsIndexEnabled: Boolean = metadataConfig.isColumnStatsIndexEnabled

  private def isRecordIndexEnabled: Boolean = indicesSupport.exists(idx =>
    idx.getIndexName == RecordLevelIndexSupport.INDEX_NAME && idx.isIndexAvailable)

  private def isExpressionIndexEnabled: Boolean = indicesSupport.exists(idx =>
    idx.getIndexName == ExpressionIndexSupport.INDEX_NAME && idx.isIndexAvailable)

  private def isBucketIndexEnabled: Boolean = indicesSupport.exists(idx =>
    idx.getIndexName == BucketIndexSupport.INDEX_NAME && idx.isIndexAvailable)

  private def isPartitionStatsIndexEnabled: Boolean = indicesSupport.exists(idx =>
    idx.getIndexName == PartitionStatsIndexSupport.INDEX_NAME && idx.isIndexAvailable)

  private def isBloomFiltersIndexEnabled: Boolean = indicesSupport.exists(idx =>
    idx.getIndexName == BloomFiltersIndexSupport.INDEX_NAME && idx.isIndexAvailable)

  private def isSecondaryIndexEnabled: Boolean = indicesSupport.exists(idx =>
    idx.getIndexName == SecondaryIndexSupport.INDEX_NAME && idx.isIndexAvailable)

  private def isIndexAvailable: Boolean = indicesSupport.exists(idx => idx.isIndexAvailable)

  private def validateConfig(): Unit = {
    if (isDataSkippingEnabled && (!isMetadataTableEnabled || !isIndexAvailable)) {
      logWarning("Data skipping requires Metadata Table and at least one of the indices to be enabled! "
        + s"(isMetadataTableEnabled = $isMetadataTableEnabled, isColumnStatsIndexEnabled = $isColumnStatsIndexEnabled"
        + s", isRecordIndexApplicable = $isRecordIndexEnabled, isExpressionIndexEnabled = $isExpressionIndexEnabled, " +
        s"isBucketIndexEnable = $isBucketIndexEnabled, isPartitionStatsIndexEnabled = $isPartitionStatsIndexEnabled)"
        + s", isBloomFiltersIndexEnabled = $isBloomFiltersIndexEnabled)")
    }
  }

}

object HoodieFileIndex extends Logging {

  object DataSkippingFailureMode extends Enumeration {
    val configName = "hoodie.fileIndex.dataSkippingFailureMode"

    type DataSkippingFailureMode = Value

    case class Val(value: String) extends super.Val {
      override def toString(): String = value
    }

    import scala.language.implicitConversions
    implicit def valueToVal(x: Value): DataSkippingFailureMode = x.asInstanceOf[Val]

    val Fallback: Val = Val("fallback")
    val Strict: Val   = Val("strict")
  }

  def collectReferencedColumns(spark: SparkSession, queryFilters: Seq[Expression], schema: StructType): Seq[String] = {
    val resolver = spark.sessionState.analyzer.resolver
    val refs = queryFilters.flatMap(_.references)
    schema.fieldNames.filter { colName => refs.exists(r => resolver.apply(colName, r.name)) }
  }

  def getConfigProperties(spark: SparkSession, options: Map[String, String], tableConfig: HoodieTableConfig): TypedProperties = {
    val sqlConf: SQLConf = spark.sessionState.conf
    val properties = TypedProperties.fromMap(options.filter(p => p._2 != null).asJava)

    // TODO(HUDI-5361) clean up properties carry-over

    // To support metadata listing via Spark SQL we allow users to pass the config via SQL Conf in spark session. Users
    // would be able to run SET hoodie.metadata.enable=true in the spark sql session to enable metadata listing.
    val isMetadataTableEnabled = getConfigValue(options, sqlConf, HoodieMetadataConfig.ENABLE.key, null)
    if (isMetadataTableEnabled != null) {
      properties.setProperty(HoodieMetadataConfig.ENABLE.key(), String.valueOf(isMetadataTableEnabled))
    }

    val listingModeOverride = getConfigValue(options, sqlConf,
      DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key, null)
    if (listingModeOverride != null) {
      properties.setProperty(DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key, listingModeOverride)
    }

    if (tableConfig != null) {
      properties.setProperty(RECORDKEY_FIELD.key, tableConfig.getRecordKeyFields.orElse(Array.empty).mkString(","))
      properties.setProperty(PARTITIONPATH_FIELD.key, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(tableConfig).orElse(""))

      // for simple bucket index, we need to set the INDEX_TYPE, BUCKET_INDEX_HASH_FIELD, BUCKET_INDEX_NUM_BUCKETS
      val database = getDatabaseName(tableConfig, spark.catalog.currentDatabase)
      val tableName = tableConfig.getTableName

      if (spark.catalog.tableExists(database, tableName)) {
        val tableIdentifier = TableIdentifier(tableName, Some(database))
        val table = HoodieCatalogTable(spark, tableIdentifier)
        table.catalogProperties.foreach(kv => properties.setProperty(kv._1, kv._2))
      }
    }

    properties
  }

  def convertFilterForTimestampKeyGenerator(metaClient: HoodieTableMetaClient,
      partitionFilters: Seq[Expression]): Seq[Expression] = {

    val tableConfig = metaClient.getTableConfig
    val keyGenerator = tableConfig.getKeyGeneratorClassName

    if (keyGenerator != null && (keyGenerator.equals(classOf[TimestampBasedKeyGenerator].getCanonicalName) ||
        keyGenerator.equals(classOf[TimestampBasedAvroKeyGenerator].getCanonicalName))) {
      val inputFormat = tableConfig.getString(TIMESTAMP_INPUT_DATE_FORMAT)
      val outputFormat = tableConfig.getString(TIMESTAMP_OUTPUT_DATE_FORMAT)
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
                try {
                  val converted = outDateFormat.format(inDateFormat.parse(value.toString))
                  Literal(UTF8String.fromString(converted), StringType)
                } catch {
                  case _: java.text.ParseException =>
                    try {
                      outDateFormat.parse(value.toString)
                    } catch {
                      case e: Exception => throw new HoodieException("Partition filter for TimestampKeyGenerator cannot be converted to format " + outDateFormat.toString, e)
                    }
                    Literal(UTF8String.fromString(value.toString), StringType)
                }
            }
          }
        } catch {
          case NonFatal(e) =>
            logWarning("Fail to convert filters for TimestampBaseAvroKeyGenerator", e)
            partitionFilters
        }
      }
    } else {
      partitionFilters
    }
  }

  private def getQueryPaths(options: Map[String, String]): Seq[StoragePath] = {
    // NOTE: To make sure that globbing is appropriately handled w/in the
    //       `path`, we need to:
    //          - First, probe whether requested globbed paths has been resolved (and `glob.paths` was provided
    //          in options); otherwise
    //          - Treat `path` as fully-qualified (ie non-globbed) path
    val paths = options.get("glob.paths") match {
      case Some(globbed) =>
        globbed.split(",").toSeq
      case None =>
        val path = options.getOrElse("path",
          throw new IllegalArgumentException("'path' or 'glob paths' option required"))
        Seq(path)
    }

    paths.map(new StoragePath(_))
  }

  // if database name is not set, fall back to use 'default' instead of failing
  def getDatabaseName(tableConfig: HoodieTableConfig, defaultDatabase: String)  = {
    if (StringUtils.isNullOrEmpty(tableConfig.getDatabaseName)) {
      if (StringUtils.isNullOrEmpty(defaultDatabase)) {
        "default"
      } else {
        defaultDatabase
      }
    } else {
      tableConfig.getDatabaseName
    }
  }
}
