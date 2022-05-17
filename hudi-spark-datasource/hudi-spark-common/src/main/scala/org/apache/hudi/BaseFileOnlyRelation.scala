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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType

/**
 * [[BaseRelation]] implementation only reading Base files of Hudi tables, essentially supporting following querying
 * modes:
 * <ul>
 * <li>For COW tables: Snapshot</li>
 * <li>For MOR tables: Read-optimized</li>
 * </ul>
 *
 * NOTE: The reason this Relation is used in liue of Spark's default [[HadoopFsRelation]] is primarily due to the
 * fact that it injects real partition's path as the value of the partition field, which Hudi ultimately persists
 * as part of the record payload. In some cases, however, partition path might not necessarily be equal to the
 * verbatim value of the partition path field (when custom [[KeyGenerator]] is used) therefore leading to incorrect
 * partition field values being written
 */
class BaseFileOnlyRelation(sqlContext: SQLContext,
                           metaClient: HoodieTableMetaClient,
                           optParams: Map[String, String],
                           userSchema: Option[StructType],
                           globPaths: Seq[Path])
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) with SparkAdapterSupport {

  override type FileSplit = HoodieBaseFileSplit

  override lazy val mandatoryFields: Seq[String] =
  // TODO reconcile, record's key shouldn't be mandatory for base-file only relation
    Seq(recordKeyField)

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "true")
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
      filters = filters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = HoodieDataSourceHelper.getConfigurationWithInternalSchema(new Configuration(conf), requiredSchema.internalSchema, metaClient.getBasePath, validCommits)
    )

    new HoodieFileScanRDD(sparkSession, baseFileReader, fileSplits)
  }

  protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[HoodieBaseFileSplit] = {
    val partitions = listLatestBaseFiles(globPaths, partitionFilters, dataFilters)
    val fileSplits = partitions.values.toSeq
      .flatMap { files =>
        files.flatMap { file =>
          // TODO fix, currently assuming parquet as underlying format
          HoodieDataSourceHelper.splitFiles(
            sparkSession = sparkSession,
            file = file,
            partitionValues = getPartitionColumnsAsInternalRow(file)
          )
        }
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
      val (tableFileFormat, formatClassName) =
        metaClient.getTableConfig.getBaseFileFormat match {
          case HoodieFileFormat.ORC => (new OrcFileFormat, "orc")
          case HoodieFileFormat.PARQUET =>
            // We're delegating to Spark to append partition values to every row only in cases
            // when these corresponding partition-values are not persisted w/in the data file itself
            val parquetFileFormat = sparkAdapter.createHoodieParquetFileFormat(shouldExtractPartitionValuesFromPartitionPath).get
            (parquetFileFormat, HoodieParquetFileFormat.FILE_FORMAT_ID)
        }

    if (globPaths.isEmpty) {
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
        fileFormat = tableFileFormat,
        optParams)(sparkSession)
    } else {
      val readPathsStr = optParams.get(DataSourceReadOptions.READ_PATHS.key)
      val extraReadPaths = readPathsStr.map(p => p.split(",").toSeq).getOrElse(Seq())

      DataSource.apply(
        sparkSession = sparkSession,
        paths = extraReadPaths,
        userSpecifiedSchema = userSchema,
        className = formatClassName,
        // Since we're reading the table as just collection of files we have to make sure
        // we only read the latest version of every Hudi's file-group, which might be compacted, clustered, etc.
        // while keeping previous versions of the files around as well.
        //
        // We rely on [[HoodieROTablePathFilter]], to do proper filtering to assure that
        options = optParams ++ Map(
          "mapreduce.input.pathFilter.class" -> classOf[HoodieROTablePathFilter].getName
        ),
        partitionColumns = partitionColumns
      )
        .resolveRelation()
        .asInstanceOf[HadoopFsRelation]
    }
  }
}
