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

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.{PartitionedFile, _}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{BooleanType, StructType}

import scala.util.Try

/**
 * The implement of [[BaseRelation]], which is used to respond to query that only touches the base files(Parquet),
 * like query COW tables in Snapshot-Query and Read_Optimized mode and MOR tables in Read_Optimized mode.
 */
class BaseFileOnlyViewRelation(
    sqlContext: SQLContext,
    metaClient: HoodieTableMetaClient,
    optParams: Map[String, String],
    userSchema: Option[StructType]
  ) extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) with SparkAdapterSupport {

  private val fileIndex = HoodieFileIndex(sparkSession,
    metaClient,
    userSchema,
    optParams,
    FileStatusCache.getOrCreate(sqlContext.sparkSession)
  )

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")

    val filterExpressions = HoodieSparkUtils.convertToCatalystExpressions(filters, tableStructSchema)
      .getOrElse(Literal(true, BooleanType))
    val (partitionFilters, dataFilters) = {
      val splited = filters.map { filter =>
        HoodieDataSourceHelper.splitPartitionAndDataPredicates(
          sparkSession, filterExpressions, partitionColumns)
      }
      (splited.flatMap(_._1), splited.flatMap(_._2))
    }

    val partitionFiles = fileIndex.listFiles(partitionFilters, dataFilters).flatMap { partition =>
      partition.files.flatMap { file =>
        HoodieDataSourceHelper.splitFiles(
          sparkSession = sparkSession,
          file = file,
          partitionValues = partition.values
        )
      }
    }
    val emptyPartitionFiles = partitionFiles.map{ f =>
      PartitionedFile(InternalRow.empty, f.filePath, f.start, f.length)
    }
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val filePartitions = sparkAdapter.getFilePartitions(sparkSession, emptyPartitionFiles, maxSplitBytes)

    val requiredSchemaParquetReader = HoodieDataSourceHelper.buildHoodieParquetReader(
      sparkSession = sparkSession,
      dataSchema = tableStructSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = tableStructSchema,
      filters = filters,
      options = optParams,
      hadoopConf = sparkSession.sessionState.newHadoopConf()
    )

    new HoodieFileScanRDD(sparkSession, requiredColumns, tableStructSchema,
      requiredSchemaParquetReader, filePartitions)
  }
}
