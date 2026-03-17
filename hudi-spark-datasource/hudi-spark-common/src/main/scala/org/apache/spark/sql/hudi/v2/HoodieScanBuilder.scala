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

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.HoodieCatalystExpressionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Scan builder for DSv2 CoW snapshot reads.
 * Accepts column pruning and filter pushdown (partition pruning + data skipping).
 */
class HoodieScanBuilder(spark: SparkSession,
                        metaClient: HoodieTableMetaClient,
                        tableSchema: StructType,
                        options: Map[String, String]) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SparkAdapterSupport {

  private var requiredSchema: StructType = tableSchema
  private var _pushedFilters: Array[Filter] = Array.empty
  private var partitionFilterExprs: Seq[Expression] = Seq.empty
  private var dataFilterExprs: Seq[Expression] = Seq.empty

  private lazy val fileIndex = HoodieFileIndex(spark, metaClient, None, options,
    includeLogFiles = false, shouldEmbedFileSlices = false)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, postScan) = filters.partition { f =>
      HoodieCatalystExpressionUtils.convertToCatalystExpression(f, tableSchema).isDefined
    }

    val expressions = pushed.flatMap(f =>
      HoodieCatalystExpressionUtils.convertToCatalystExpression(f, tableSchema))

    val (partFilters, datFilters) = HoodieCatalystExpressionUtils
      .splitPartitionAndDataPredicates(spark, expressions, fileIndex.partitionSchema.fieldNames)

    partitionFilterExprs = partFilters.toSeq
    dataFilterExprs = datFilters.toSeq

    _pushedFilters = pushed

    // Data filters are only used for file-level skipping (via metadata indices),
    // not for row-level filtering. Return them as postScan so Spark applies
    // row-level filtering. Partition filters are fully handled by partition pruning.
    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    val dataFilterArr = pushed.filterNot(f => f.references.forall(partFieldNames.contains))
    postScan ++ dataFilterArr
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def build(): Scan = {
    val partFieldNames = fileIndex.partitionSchema.fieldNames.toSet
    val requiredDataSchema = StructType(requiredSchema.filterNot(f => partFieldNames.contains(f.name)))
    val requiredPartitionSchema = StructType(requiredSchema.filter(f => partFieldNames.contains(f.name)))

    val hadoopConf = spark.sessionState.newHadoopConf()
    val readerOptions = options + (FileFormat.OPTION_RETURNING_BATCH -> "false")
    val reader = sparkAdapter.createParquetFileReader(false, spark.sessionState.conf, readerOptions, hadoopConf)
    val broadcastReader = spark.sparkContext.broadcast(reader)
    val broadcastConf = spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val fullPartSchema = fileIndex.partitionSchema
    val fileSlicesPerPartition = fileIndex.filterFileSlices(dataFilterExprs, partitionFilterExprs)

    val partitions = fileSlicesPerPartition.flatMap { case (partitionOpt, fileSlices) =>
      fileSlices.filter(fs => fs.getBaseFile.isPresent).map { fs =>
        val baseFile = fs.getBaseFile.get()
        val allPartValues = partitionOpt.map(_.getValues).getOrElse(Array.empty[AnyRef])

        val partValues = if (requiredPartitionSchema.isEmpty) {
          Array.empty[AnyRef]
        } else {
          requiredPartitionSchema.fieldNames.map { name =>
            val idx = fullPartSchema.fieldIndex(name)
            allPartValues(idx)
          }
        }

        HoodieInputPartition(0, baseFile.getPath, baseFile.getFileSize, partValues)
      }
    }.zipWithIndex.map { case (p, i) => p.copy(index = i) }.toArray[InputPartition]

    new HoodieBatchScan(
      requiredSchema,
      partitions,
      broadcastReader,
      broadcastConf,
      requiredDataSchema,
      requiredPartitionSchema,
      _pushedFilters)
  }
}
