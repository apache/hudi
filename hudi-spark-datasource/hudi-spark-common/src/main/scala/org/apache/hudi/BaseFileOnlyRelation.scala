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
class BaseFileOnlyRelation(sqlContext: SQLContext,
                           metaClient: HoodieTableMetaClient,
                           optParams: Map[String, String],
                           userSchema: Option[StructType],
                           globPaths: Seq[Path])
  extends HoodieBaseRelation(sqlContext, metaClient, optParams, userSchema) with SparkAdapterSupport {

  override type FileSplit = HoodieBaseFileSplit

  protected override def composeRDD(fileSplits: Seq[HoodieBaseFileSplit],
                                    partitionSchema: StructType,
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    filters: Array[Filter]): HoodieUnsafeRDD = {
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

    new HoodieFileScanRDD(sparkSession, baseFileReader, fileSplits)
  }

  protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[HoodieBaseFileSplit] = {
    val partitions = listLatestBaseFiles(globPaths, partitionFilters, dataFilters)
    val fileSplits = partitions.values.toSeq.flatMap { files =>
      files.flatMap { file =>
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

    sparkAdapter.getFilePartitions(sparkSession, fileSplits, maxSplitBytes).map(HoodieBaseFileSplit.apply)
  }
}
