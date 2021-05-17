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
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, SerializableConfiguration}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.config.HoodieWriteConfig
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
  extends FileIndex with Logging {

  private val basePath = metaClient.getBasePath

  @transient private val queryPath = new Path(options.getOrElse("path", "'path' option required"))
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
    val partitionColumns = tableConfig.getPartitionColumns
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

  @transient @volatile private var fileSystemView: HoodieTableFileSystemView = _
  @transient @volatile private var cachedAllInputFiles: Array[HoodieBaseFile] = _
  @transient @volatile private var cachedFileSize: Long = 0L
  @transient @volatile private var cachedAllPartitionPaths: Seq[PartitionRowPath] = _

  @volatile private var queryAsNonePartitionedTable: Boolean = _

  refresh0()

  override def rootPaths: Seq[Path] = queryPath :: Nil

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (queryAsNonePartitionedTable) { // Read as Non-Partitioned table.
      Seq(PartitionDirectory(InternalRow.empty, allFiles))
    } else {
      // Prune the partition path by the partition filters
      val prunedPartitions = prunePartition(cachedAllPartitionPaths, partitionFilters)
      prunedPartitions.map { partition =>
        val fileStatues = fileSystemView.getLatestBaseFiles(partition.partitionPath).iterator()
          .asScala.toSeq
          .map(_.getFileStatus)
        PartitionDirectory(partition.values, fileStatues)
      }
    }
  }

  override def inputFiles: Array[String] = {
    cachedAllInputFiles.map(_.getFileStatus.getPath.toString)
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
    cachedAllInputFiles = fileSystemView.getLatestBaseFiles.iterator().asScala.toArray
    cachedAllPartitionPaths = partitionFiles.keys.toSeq
    cachedFileSize = cachedAllInputFiles.map(_.getFileLen).sum

    // If the partition value contains InternalRow.empty, we query it as a non-partitioned table.
    queryAsNonePartitionedTable = cachedAllPartitionPaths
      .exists(p => p.values == InternalRow.empty)
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

  def allFiles: Seq[FileStatus] = cachedAllInputFiles.map(_.getFileStatus)

  /**
   * Prune the partition by the filter.This implementation is fork from
   * org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex#prunePartitions.
   * @param partitionPaths All the partition paths.
   * @param predicates The filter condition.
   * @return The Pruned partition paths.
   */
  private def prunePartition(partitionPaths: Seq[PartitionRowPath],
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

  /**
   * Load all partition paths and it's files under the query table path.
   */
  private def loadPartitionPathFiles(): Map[PartitionRowPath, Array[FileStatus]] = {
    val sparkEngine = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val properties = new Properties()
    properties.putAll(options.asJava)
    val metadataConfig = HoodieMetadataConfig.newBuilder.fromProperties(properties).build()

    val queryPartitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), queryPath)
    // Load all the partition path from the basePath, and filter by the query partition path.
    // TODO load files from the queryPartitionPath directly.
    val partitionPaths = FSUtils.getAllPartitionPaths(sparkEngine, metadataConfig, basePath).asScala
      .filter(_.startsWith(queryPartitionPath))

    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(basePath).withProperties(properties).build()
    val maxListParallelism = writeConfig.getFileListingParallelism

    val serializableConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
    val partitionSchema = _partitionSchemaFromProperties
    val timeZoneId = CaseInsensitiveMap(options)
      .get(DateTimeUtils.TIMEZONE_OPTION)
      .getOrElse(SQLConf.get.sessionLocalTimeZone)

    val sparkParsePartitionUtil = HoodieSparkUtils.createSparkParsePartitionUtil(spark
      .sessionState.conf)
    // Convert partition path to PartitionRowPath
    val partitionRowPaths = partitionPaths.map { partitionPath =>
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
          InternalRow.fromSeq(Seq(UTF8String.fromString(partitionPath)))
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
          // HIVE_STYLE_PARTITIONING_OPT_KEY is disable.
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
    // Fetch the rest from the file system.
    val fetchedPartition2Files =
      spark.sparkContext.parallelize(pathToFetch, Math.min(pathToFetch.size, maxListParallelism))
        .map { partitionRowPath =>
          // Here we use a LocalEngineContext to get the files in the partition.
          // We can do this because the TableMetadata.getAllFilesInPartition only rely on the
          // hadoopConf of the EngineContext.
          val engineContext = new HoodieLocalEngineContext(serializableConf.get())
          val filesInPartition =  FSUtils.getFilesInPartition(engineContext, metadataConfig,
              basePath, partitionRowPath.fullPartitionPath(basePath))
          (partitionRowPath, filesInPartition)
        }.collect().map(f => f._1 -> f._2).toMap

    // Update the fileStatusCache
    fetchedPartition2Files.foreach {
      case (partitionRowPath, filesInPartition) =>
        fileStatusCache.putLeafFiles(partitionRowPath.fullPartitionPath(basePath), filesInPartition)
    }
    cachePartitionToFiles.toMap ++ fetchedPartition2Files
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
