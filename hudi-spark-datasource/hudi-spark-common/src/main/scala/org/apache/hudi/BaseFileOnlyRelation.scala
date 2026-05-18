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

package org.apache.hudi

import org.apache.hudi.DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX
import org.apache.hudi.HoodieBaseRelation.projectReader
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType

/**
 * [[BaseRelation]] implementation only reading Base files of Hudi tables, essentially supporting following querying
 * modes:
 * <ul>
 *  <li>For COW tables: Snapshot</li>
 *  <li>For MOR tables: Read-optimized</li>
 * </ul>
 *
 * NOTE: The reason this Relation is used in-liue of Spark's default [[HadoopFsRelation]] is primarily due to the
 * fact that it injects real partition's path as the value of the partition field, which Hudi ultimately persists
 * as part of the record payload. In some cases, however, partition path might not necessarily be equal to the
 * verbatim value of the partition path field (when custom [[KeyGenerator]] is used) therefore leading to incorrect
 * partition field values being written
 */
case class BaseFileOnlyRelation(override val sqlContext: SQLContext,
                                override val metaClient: HoodieTableMetaClient,
                                override val optParams: Map[String, String],
                                private val userSchema: Option[StructType],
                                private val prunedDataSchema: Option[StructType] = None)
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema, prunedDataSchema)
    with SparkAdapterSupport {

  case class HoodieBaseFileSplit(filePartition: FilePartition) extends HoodieFileSplit

  override type FileSplit = HoodieBaseFileSplit
  override type Relation = BaseFileOnlyRelation

  // TODO(HUDI-3204) this is to override behavior (exclusively) for COW tables to always extract
  //                 partition values from partition path
  //                 For more details please check HUDI-4161
  // NOTE: This override has to mirror semantic of whenever this Relation is converted into [[HadoopFsRelation]],
  //       which is currently done for all cases, except when Schema Evolution is enabled
  override protected val shouldExtractPartitionValuesFromPartitionPath: Boolean =
  internalSchemaOpt.isEmpty

  override lazy val mandatoryFields: Seq[String] = Seq.empty

  // Before Spark 3.4.0: PartitioningAwareFileIndex.BASE_PATH_PARAM
  // Since Spark 3.4.0: FileIndexOptions.BASE_PATH_PARAM
  val BASE_PATH_PARAM = "basePath"

  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  protected override def composeRDD(fileSplits: Seq[HoodieBaseFileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    val (partitionSchema, dataSchema, requiredDataSchema) = tryPrunePartitionColumns(tableSchema, requiredSchema)
    val baseFileReader = createBaseFileReader(
      spark = sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = requiredDataSchema,
      filters = filters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(new Configuration(conf), requiredSchema.internalSchema)
    )

    // NOTE: In some case schema of the reader's output (reader's schema) might not match the schema expected by the caller.
    //       This could occur for ex, when requested schema contains partition columns which might not be persisted w/in the
    //       data file, but instead would be parsed from the partition path. In that case output of the file-reader will have
    //       different ordering of the fields than the original required schema (for more details please check out
    //       [[ParquetFileFormat]] impl). In that case we have to project the rows from the file-reader's schema
    //       back into the one expected by the caller
    val projectedReader = projectReader(baseFileReader, requiredSchema.structTypeSchema)

    // SPARK-37273 FileScanRDD constructor changed in SPARK 3.3
    sparkAdapter.createHoodieFileScanRDD(sparkSession, projectedReader.apply, fileSplits.map(_.filePartition), requiredSchema.structTypeSchema)
      .asInstanceOf[HoodieUnsafeRDD]
  }

  protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[HoodieBaseFileSplit] = {
    val fileSlices = listLatestFileSlices(partitionFilters, dataFilters)
    val fileSplits = fileSlices.flatMap { fileSlice =>
      // TODO fix, currently assuming parquet as underlying format
      val pathInfo: StoragePathInfo = fileSlice.getBaseFile.get.getPathInfo
      HoodieDataSourceHelper.splitFiles(
        sparkSession = sparkSession,
        file = pathInfo,
        partitionValues = getPartitionColumnsAsInternalRow(pathInfo)
      )
    }
      // NOTE: It's important to order the splits in the reverse order of their
      //       size so that we can subsequently bucket them in an efficient manner
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes

    sparkAdapter.getFilePartitions(sparkSession, fileSplits, maxSplitBytes)
      .map(HoodieBaseFileSplit.apply)
  }

  /**
   * NOTE: We have to fallback to [[HadoopFsRelation]] to make sure that all of the Spark optimizations could be
   *       equally applied to Hudi tables, since some of those are predicated on the usage of [[HadoopFsRelation]],
   *       and won't be applicable in case of us using our own custom relations (one of such optimizations is [[SchemaPruning]]
   *       rule; you can find more details in HUDI-3896)
   */
  def toHadoopFsRelation: HadoopFsRelation = {
    val enableFileIndex = HoodieSparkConfUtils.getConfigValue(optParams, sparkSession.sessionState.conf,
      ENABLE_HOODIE_FILE_INDEX.key, ENABLE_HOODIE_FILE_INDEX.defaultValue.toString).toBoolean
    if (enableFileIndex) {
      // NOTE: There are currently 2 ways partition values could be fetched:
      //          - Source columns (producing the values used for physical partitioning) will be read
      //          from the data file
      //          - Values parsed from the actual partition path would be appended to the final dataset
      //
      //        In the former case, we don't need to provide the partition-schema to the relation,
      //        therefore we simply stub it w/ empty schema and use full table-schema as the one being
      //        read from the data file.
      //
      //        In the latter, we have to specify proper partition schema as well as "data"-schema, essentially
      //        being a table-schema with all partition columns stripped out
      val (partitionSchema, dataSchema) = if (shouldExtractPartitionValuesFromPartitionPath) {
        (fileIndex.partitionSchema, fileIndex.dataSchema)
      } else {
        (StructType(Nil), tableStructSchema)
      }

      HadoopFsRelation(
        location = fileIndex,
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = fileFormat,
        optParams)(sparkSession)
    } else {
      val readPathsStr = optParams.get(DataSourceReadOptions.READ_PATHS.key)
      val extraReadPaths = readPathsStr.map(p => p.split(",").toSeq).getOrElse(Seq())
      // NOTE: Spark is able to infer partitioning values from partition path only when Hive-style partitioning
      //       scheme is used. Therefore, we fallback to reading the table as non-partitioned (specifying
      //       partitionColumns = Seq.empty) whenever Hive-style partitioning is not involved
      val partitionColumns: Seq[String] = if (tableConfig.getHiveStylePartitioningEnable.toBoolean) {
        this.partitionColumns
      } else {
        Seq.empty
      }

      DataSource.apply(
        sparkSession = sparkSession,
        paths = extraReadPaths,
        // Here we should specify the schema to the latest commit schema since
        // the table schema evolution.
        userSpecifiedSchema = userSchema.orElse(Some(tableStructSchema)),
        className = fileFormatClassName,
        options = optParams ++ Map(
          // Since we're reading the table as just collection of files we have to make sure
          // we only read the latest version of every Hudi's file-group, which might be compacted, clustered, etc.
          // while keeping previous versions of the files around as well.
          //
          // We rely on [[HoodieROTablePathFilter]], to do proper filtering to assure that
          "mapreduce.input.pathFilter.class" -> classOf[HoodieROTablePathFilter].getName,

          // We have to override [[EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH]] setting, since
          // the relation might have this setting overridden
          DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key -> shouldExtractPartitionValuesFromPartitionPath.toString,

          // NOTE: We have to specify table's base-path explicitly, since we're requesting Spark to read it as a
          //       list of globbed paths which complicates partitioning discovery for Spark.
          //       Please check [[PartitioningAwareFileIndex#basePaths]] comment for more details.
          BASE_PATH_PARAM -> metaClient.getBasePath.toString
        ),
        partitionColumns = partitionColumns
      )
        .resolveRelation()
        .asInstanceOf[HadoopFsRelation]
    }
  }
}
