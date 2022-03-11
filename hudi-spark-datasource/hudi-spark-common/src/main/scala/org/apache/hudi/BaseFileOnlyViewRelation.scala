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
import org.apache.hudi.HoodieBaseRelation.createBaseFileReader
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.HoodieROTablePathFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources._
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
class BaseFileOnlyViewRelation(sqlContext: SQLContext,
                               metaClient: HoodieTableMetaClient,
                               optParams: Map[String, String],
                               userSchema: Option[StructType],
                               globPaths: Seq[Path])
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) with SparkAdapterSupport {

  private val fileIndex = HoodieFileIndex(sparkSession, metaClient, userSchema, optParams,
    FileStatusCache.getOrCreate(sqlContext.sparkSession))

  override def doBuildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[InternalRow] = {
    // NOTE: In case list of requested columns doesn't contain the Primary Key one, we
    //       have to add it explicitly so that
    //          - Merging could be performed correctly
    //          - In case 0 columns are to be fetched (for ex, when doing {@code count()} on Spark's [[Dataset]],
    //          Spark still fetches all the rows to execute the query correctly
    //
    //       It's okay to return columns that have not been requested by the caller, as those nevertheless will be
    //       filtered out upstream
    val fetchedColumns: Array[String] = appendMandatoryColumns(requiredColumns)

    val (requiredAvroSchema, requiredStructSchema) =
      HoodieSparkUtils.getRequiredSchema(tableAvroSchema, fetchedColumns)

    val filterExpressions = convertToExpressions(filters)
    val (partitionFilters, dataFilters) = HoodieCatalystExpressionUtils.splitPartitionAndDataPredicates(
      sparkSession, filterExpressions, partitionColumns)

    val filePartitions = getPartitions(partitionFilters, dataFilters)

    val partitionSchema = StructType(Nil)
    val tableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchema.toString)
    val requiredSchema = HoodieTableSchema(requiredStructSchema, requiredAvroSchema.toString)

    val baseFileReader = createBaseFileReader(
      spark = sparkSession,
      partitionSchema = partitionSchema,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      filters = filters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = new Configuration(conf)
    )

    new HoodieFileScanRDD(sparkSession, baseFileReader, filePartitions)
  }

  private def getPartitions(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[FilePartition] = {
    val partitionDirectories = if (globPaths.isEmpty) {
      val hoodieFileIndex = HoodieFileIndex(sparkSession, metaClient, userSchema, optParams,
        FileStatusCache.getOrCreate(sqlContext.sparkSession))
      hoodieFileIndex.listFiles(partitionFilters, dataFilters)
    } else {
      sqlContext.sparkContext.hadoopConfiguration.setClass(
        "mapreduce.input.pathFilter.class",
        classOf[HoodieROTablePathFilter],
        classOf[org.apache.hadoop.fs.PathFilter])

      val inMemoryFileIndex = HoodieSparkUtils.createInMemoryFileIndex(sparkSession, globPaths)
      inMemoryFileIndex.listFiles(partitionFilters, dataFilters)
    }

    val partitions = partitionDirectories.flatMap { partition =>
      partition.files.flatMap { file =>
        // TODO move to adapter
        // TODO fix, currently assuming parquet as underlying format
        HoodieDataSourceHelper.splitFiles(
          sparkSession = sparkSession,
          file = file,
          // TODO clarify why this is required
          partitionValues = InternalRow.empty
        )
      }
    }

    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes

    sparkAdapter.getFilePartitions(sparkSession, partitions, maxSplitBytes)
  }

  private def convertToExpressions(filters: Array[Filter]): Array[Expression] = {
    val catalystExpressions = HoodieSparkUtils.convertToCatalystExpressions(filters, tableStructSchema)

    val failedExprs = catalystExpressions.zipWithIndex.filter { case (opt, _) => opt.isEmpty }
    if (failedExprs.nonEmpty) {
      val failedFilters = failedExprs.map(p => filters(p._2))
      logWarning(s"Failed to convert Filters into Catalyst expressions (${failedFilters.map(_.toString)})")
    }

    catalystExpressions.filter(_.isDefined).map(_.get).toArray
  }
}
