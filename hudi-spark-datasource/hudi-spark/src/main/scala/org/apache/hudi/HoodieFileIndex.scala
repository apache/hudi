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

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.{FileSystemViewStorageConfig, HoodieTableFileSystemView}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusCache, NoopCache, PartitionDirectory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

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
 * who'es directory level is 3).We can still read it as a partitioned table. We will mapping the
 * partition path (e.g. 2021/03/10) to the only partition column (e.g. "dt").
 *
 * 3、Else the the partition columns size is not equal to the partition directory level and the
 * size is great than "1" (e.g. partition column is "dt,hh", the partition path is "2021/03/10/12")
 * , we read it as a Non-Partitioned table because we cannot know how to mapping the partition
 * path with the partition columns in this case.
 *
 */
case class HoodieFileIndex(
     spark: SparkSession,
     metaClient: HoodieTableMetaClient,
     schemaSpec: Option[StructType],
     options: Map[String, String],
     @transient fileStatusCache: FileStatusCache = NoopCache)
  extends FileIndex with Logging with SparkAdapterSupport {

  private val basePath = metaClient.getBasePath

  @transient private val queryPath = new Path(options.getOrElse("path", "'path' option required"))

  private val queryType = options(QUERY_TYPE.key())

  private val tableType = metaClient.getTableType

  /**
   * Get the schema of the table.
   */
  lazy val schema: StructType = schemaSpec.getOrElse({
    val schemaUtil = new TableSchemaResolver(metaClient)
    SchemaConverters.toSqlType(schemaUtil.getTableAvroSchema)
      .dataType.asInstanceOf[StructType]
  })

  /**
   * Get the partition schema from the hoodie.properties.
   */
  private lazy val _partitionSchemaFromProperties: StructType = {
    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionFields
    val nameFieldMap = schema.fields.map(filed => filed.name -> filed).toMap

    if (partitionColumns.isPresent) {
      val partitionFields = partitionColumns.get().map(column =>
        nameFieldMap.getOrElse(column, throw new IllegalArgumentException(s"Cannot find column: '" +
          s"$column' in the schema[${schema.fields.mkString(",")}]")))
      new StructType(partitionFields)
    } else { // If the partition columns have not stored in hoodie.properites(the table that was
      // created earlier), we trait it as a non-partitioned table.
      logWarning("No partition columns available from hoodie.properties." +
        " Partition pruning will not work")
      new StructType()
    }
  }

  private lazy val engineContext = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))

  private lazy val configProperties = {
    val sqlConf: SQLConf = spark.sessionState.conf
    val properties = new Properties()

    // To support metadata listing via Spark SQL we allow users to pass the config via SQL Conf in spark session. Users
    // would be able to run SET hoodie.metadata.enable=true in the spark sql session to enable metadata listing.
    properties.put(HoodieMetadataConfig.METADATA_ENABLE_PROP,
      sqlConf.getConfString(HoodieMetadataConfig.METADATA_ENABLE_PROP.key(),
        HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS.toString))
    properties.put(HoodieMetadataConfig.METADATA_VALIDATE_PROP,
      sqlConf.getConfString(HoodieMetadataConfig.METADATA_VALIDATE_PROP.key(),
        HoodieMetadataConfig.METADATA_VALIDATE_PROP.defaultValue().toString))
    properties.putAll(options.asJava)
    properties
  }

  private lazy val fileSystemStorageConfig = FileSystemViewStorageConfig.newBuilder()
    .fromProperties(configProperties)
    .build()

  private lazy val metadataConfig = HoodieMetadataConfig.newBuilder
    .fromProperties(configProperties)
    .build()

  @transient @volatile private var fileSystemView: HoodieTableFileSystemView = _
  @transient @volatile private var cachedAllInputFileSlices: Map[PartitionRowPath, Seq[FileSlice]] = _
  @transient @volatile private var cachedFileSize: Long = 0L

  @volatile private var queryAsNonePartitionedTable: Boolean = _

  refresh0()

  override def rootPaths: Seq[Path] = queryPath :: Nil

  /**
   * Invoked by Spark to fetch list of latest base files per partition.
   *
   * @param partitionFilters partition column filters
   * @param dataFilters data columns filters
   * @return list of PartitionDirectory containing partition to base files mapping
   */
  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (queryAsNonePartitionedTable) { // Read as Non-Partitioned table.
      Seq(PartitionDirectory(InternalRow.empty, allFiles))
    } else {
      // Prune the partition path by the partition filters
      val prunedPartitions = prunePartition(cachedAllInputFileSlices.keys.toSeq, partitionFilters)
      prunedPartitions.map { partition =>
        val baseFileStatuses = cachedAllInputFileSlices(partition).map(fileSlice => {
          if (fileSlice.getBaseFile.isPresent) {
            fileSlice.getBaseFile.get().getFileStatus
          } else {
            null
          }
        }).filterNot(_ == null)

        PartitionDirectory(partition.values, baseFileStatuses)
      }
    }
  }

  /**
   * Fetch list of latest base files and log files per partition.
   *
   * @param partitionFilters partition column filters
   * @param dataFilters data column filters
   * @return mapping from string partition paths to its base/log files
   */
  def listFileSlices(partitionFilters: Seq[Expression],
                     dataFilters: Seq[Expression]): Map[String, Seq[FileSlice]] = {
    if (queryAsNonePartitionedTable) {
      // Read as Non-Partitioned table.
      cachedAllInputFileSlices.map(entry => (entry._1.partitionPath, entry._2))
    } else {
      // Prune the partition path by the partition filters
      val prunedPartitions = prunePartition(cachedAllInputFileSlices.keys.toSeq, partitionFilters)
      prunedPartitions.map(partition => {
        (partition.partitionPath, cachedAllInputFileSlices(partition))
      }).toMap
    }
  }

  override def inputFiles: Array[String] = {
    val fileStatusList = allFiles
    fileStatusList.map(_.getPath.toString).toArray
  }

  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    refresh0()
  }

  private def refresh0(): Unit = {
    val startTime = System.currentTimeMillis()
    val partitionFiles = loadPartitionPathFiles()
    val allFiles = partitionFiles.values.reduceOption(_ ++ _)
      .getOrElse(Array.empty[FileStatus])

    metaClient.reloadActiveTimeline()
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
    fileSystemView = new HoodieTableFileSystemView(metaClient, activeInstants, allFiles)

    (tableType, queryType) match {
      case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL) =>
        // Fetch and store latest base and log files, and their sizes
        cachedAllInputFileSlices = partitionFiles.map(p => {
          val latestSlices = if (activeInstants.lastInstant().isPresent) {
           fileSystemView.getLatestMergedFileSlicesBeforeOrOn(p._1.partitionPath,
             activeInstants.lastInstant().get().getTimestamp).iterator().asScala.toSeq
          } else {
            Seq()
          }
          (p._1, latestSlices)
        })
        cachedFileSize = cachedAllInputFileSlices.values.flatten.map(fileSlice => {
          if (fileSlice.getBaseFile.isPresent) {
            fileSlice.getBaseFile.get().getFileLen + fileSlice.getLogFiles.iterator().asScala.map(_.getFileSize).sum
          } else {
            fileSlice.getLogFiles.iterator().asScala.map(_.getFileSize).sum
          }
        }).sum
      case (_, _) =>
        // Fetch and store latest base files and its sizes
        cachedAllInputFileSlices = partitionFiles.map(p => {
          (p._1, fileSystemView.getLatestFileSlices(p._1.partitionPath).iterator().asScala.toSeq)
        })
        cachedFileSize = cachedAllInputFileSlices.values.flatten.map(_.getBaseFile.get().getFileLen).sum
    }

    // If the partition value contains InternalRow.empty, we query it as a non-partitioned table.
    queryAsNonePartitionedTable = partitionFiles.keys.exists(p => p.values == InternalRow.empty)
    val flushSpend = System.currentTimeMillis() - startTime
    logInfo(s"Refresh for table ${metaClient.getTableConfig.getTableName}," +
      s" spend: $flushSpend ms")
  }

  override def sizeInBytes: Long = {
    cachedFileSize
  }

  override def partitionSchema: StructType = {
    if (queryAsNonePartitionedTable) {
      // If we read it as Non-Partitioned table, we should not
      // return the partition schema.
      new StructType()
    } else {
      _partitionSchemaFromProperties
    }
  }

  /**
   * Get the data schema of the table.
   * @return
   */
  def dataSchema: StructType = {
    val partitionColumns = partitionSchema.fields.map(_.name).toSet
    StructType(schema.fields.filterNot(f => partitionColumns.contains(f.name)))
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
    cachedAllInputFileSlices.values.flatten
      .filter(_.getBaseFile.isPresent)
      .map(_.getBaseFile.get().getFileStatus)
      .toSeq
  }

  /**
   * Prune the partition by the filter.This implementation is fork from
   * org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex#prunePartitions.
   * @param partitionPaths All the partition paths.
   * @param predicates The filter condition.
   * @return The Pruned partition paths.
   */
  def prunePartition(partitionPaths: Seq[PartitionRowPath],
                     predicates: Seq[Expression]): Seq[PartitionRowPath] = {

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

      val prunedPartitionPaths = partitionPaths.filter {
        case PartitionRowPath(values, _) => boundPredicate.eval(values)
      }
      logInfo(s"Total partition size is: ${partitionPaths.size}," +
        s" after partition prune size is: ${prunedPartitionPaths.size}")
      prunedPartitionPaths
    } else {
      partitionPaths
    }
  }

  def getAllQueryPartitionPaths: Seq[PartitionRowPath] = {
    val queryPartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), queryPath)
    // Load all the partition path from the basePath, and filter by the query partition path.
    // TODO load files from the queryPartitionPath directly.
    val partitionPaths = FSUtils.getAllPartitionPaths(engineContext, metadataConfig, basePath).asScala
      .filter(_.startsWith(queryPartitionPath))

    val partitionSchema = _partitionSchemaFromProperties
    val timeZoneId = CaseInsensitiveMap(options)
      .get(DateTimeUtils.TIMEZONE_OPTION)
      .getOrElse(SQLConf.get.sessionLocalTimeZone)

    val sparkParsePartitionUtil = sparkAdapter.createSparkParsePartitionUtil(spark
      .sessionState.conf)
    // Convert partition path to PartitionRowPath
    partitionPaths.map { partitionPath =>
      val partitionRow = if (partitionSchema.fields.length == 0) {
        // This is a non-partitioned table
        InternalRow.empty
      } else {
        val partitionFragments = partitionPath.split("/")

        if (partitionFragments.length != partitionSchema.fields.length &&
          partitionSchema.fields.length == 1) {
          // If the partition column size is not equal to the partition fragment size
          // and the partition column size is 1, we map the whole partition path
          // to the partition column which can benefit from the partition prune.
          val prefix = s"${partitionSchema.fieldNames.head}="
          val partitionValue = if (partitionPath.startsWith(prefix)) {
            // support hive style partition path
            partitionPath.substring(prefix.length)
          } else {
            partitionPath
          }
          InternalRow.fromSeq(Seq(UTF8String.fromString(partitionValue)))
        } else if (partitionFragments.length != partitionSchema.fields.length &&
          partitionSchema.fields.length > 1) {
          // If the partition column size is not equal to the partition fragments size
          // and the partition column size > 1, we do not know how to map the partition
          // fragments to the partition columns. So we trait it as a Non-Partitioned Table
          // for the query which do not benefit from the partition prune.
          logWarning( s"Cannot do the partition prune for table $basePath." +
            s"The partitionFragments size (${partitionFragments.mkString(",")})" +
            s" is not equal to the partition columns size(${partitionSchema.fields.mkString(",")})")
          InternalRow.empty
        } else { // If partitionSeqs.length == partitionSchema.fields.length

          // Append partition name to the partition value if the
          // HIVE_STYLE_PARTITIONING is disable.
          // e.g. convert "/xx/xx/2021/02" to "/xx/xx/year=2021/month=02"
          val partitionWithName =
          partitionFragments.zip(partitionSchema).map {
            case (partition, field) =>
              if (partition.indexOf("=") == -1) {
                s"${field.name}=$partition"
              } else {
                partition
              }
          }.mkString("/")
          val pathWithPartitionName = new Path(basePath, partitionWithName)
          val partitionDataTypes = partitionSchema.fields.map(f => f.name -> f.dataType).toMap
          val partitionValues = sparkParsePartitionUtil.parsePartition(pathWithPartitionName,
            typeInference = false, Set(new Path(basePath)), partitionDataTypes,
            DateTimeUtils.getTimeZone(timeZoneId))

          // Convert partitionValues to InternalRow
          partitionValues.map(_.literals.map(_.value))
            .map(InternalRow.fromSeq)
            .getOrElse(InternalRow.empty)
        }
      }
      PartitionRowPath(partitionRow, partitionPath)
    }
  }

  /**
   * Load all partition paths and it's files under the query table path.
   */
  private def loadPartitionPathFiles(): Map[PartitionRowPath, Array[FileStatus]] = {
    val partitionRowPaths = getAllQueryPartitionPaths
    // List files in all of the partition path.
    val pathToFetch = mutable.ArrayBuffer[PartitionRowPath]()
    val cachePartitionToFiles = mutable.Map[PartitionRowPath, Array[FileStatus]]()
    // Fetch from the FileStatusCache
    partitionRowPaths.foreach { partitionRowPath =>
      fileStatusCache.getLeafFiles(partitionRowPath.fullPartitionPath(basePath)) match {
        case Some(filesInPartition) =>
          cachePartitionToFiles.put(partitionRowPath, filesInPartition)

        case None => pathToFetch.append(partitionRowPath)
      }
    }

    val fetchedPartitionToFiles =
      if (pathToFetch.nonEmpty) {
        val fullPartitionPathsToFetch = pathToFetch.map(p => (p, p.fullPartitionPath(basePath).toString)).toMap
        val partitionToFilesMap = FSUtils.getFilesInPartitions(engineContext, metadataConfig, basePath,
          fullPartitionPathsToFetch.values.toArray, fileSystemStorageConfig.getSpillableDir)
        fullPartitionPathsToFetch.map(p => {
          (p._1, partitionToFilesMap.get(p._2))
        })
      } else {
        Map.empty[PartitionRowPath, Array[FileStatus]]
      }

    // Update the fileStatusCache
    fetchedPartitionToFiles.foreach {
      case (partitionRowPath, filesInPartition) =>
        fileStatusCache.putLeafFiles(partitionRowPath.fullPartitionPath(basePath), filesInPartition)
    }
    cachePartitionToFiles.toMap ++ fetchedPartitionToFiles
  }

  /**
   * Represent a partition path.
   * e.g. PartitionPath(InternalRow("2021","02","01"), "2021/02/01"))
   * @param values The partition values of this partition path.
   * @param partitionPath The partition path string.
   */
  case class PartitionRowPath(values: InternalRow, partitionPath: String) {
    override def equals(other: Any): Boolean = other match {
      case PartitionRowPath(_, otherPath) => partitionPath == otherPath
      case _ => false
    }

    override def hashCode(): Int = {
      partitionPath.hashCode
    }

    def fullPartitionPath(basePath: String): Path = {
      if (partitionPath.isEmpty) {
        new Path(basePath) // This is a non-partition path
      } else {
        new Path(basePath, partitionPath)
      }
    }
  }
}
