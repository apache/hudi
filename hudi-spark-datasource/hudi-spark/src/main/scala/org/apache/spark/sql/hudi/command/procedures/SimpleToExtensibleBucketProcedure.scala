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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieExtensibleBucketMetadata, HoodieTableType}
import org.apache.hudi.common.util.{MathUtils, ValidationUtils}
import org.apache.hudi.hive.HiveSyncTool
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils
import org.apache.hudi.table.HoodieSparkTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.util.{Failure, Success, Try}

class SimpleToExtensibleBucketProcedure extends BaseProcedure with ProcedureBuilder
  with ProvidesHoodieConfig with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table_name", DataTypes.StringType),
    ProcedureParameter.required(1, "partition_initial_bucket_number", DataTypes.IntegerType)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("result", DataTypes.BooleanType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)
    val tableName = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val initialBucketNum = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Integer]
    ValidationUtils.checkArgument(MathUtils.isPowerOf2(initialBucketNum),
      s"Initial bucket number $initialBucketNum is not power of 2. Please provide a power of 2 bucket number.")
    val basePath = getBasePath(Option(tableName), Option.empty)
    val metaClient = createMetaClient(jsc, basePath)
    var client: SparkRDDWriteClient[_] = null
    var hiveSyncTool: HiveSyncTool = null
    try {
      client = HoodieCLIUtils.createHoodieWriteClient(sparkSession, basePath, Map.empty, Option(tableName))
      val table = HoodieSparkTable.create(client.getConfig, client.getEngineContext)
      val tableType = client.getConfig.getTableType
      // 1. check if the table is merge on read table
      ValidationUtils.checkState(tableType == HoodieTableType.MERGE_ON_READ, s"Table $tableName is not merge on read table.")
      // 2. check if the table is simple bucket table with non-blocking concurrency control
      val isSimpleBucket = client.getConfig.isSimpleBucketIndex
      val isNonBlockingConcurrencyControl = client.getConfig.isNonBlockingConcurrencyControl
      ValidationUtils.checkState(isSimpleBucket && isNonBlockingConcurrencyControl,
        s"Table $tableName is not simple bucket table with non-blocking concurrency control. " +
          s"Please check the configuration. isSimpleBucketIndex: $isSimpleBucket, isNonBlockingConcurrencyControl: $isNonBlockingConcurrencyControl")
      // 3. create the initial bucket metadata for each partition
      val bucketNum = client.getConfig.getBucketIndexNumBuckets
      ValidationUtils.checkState(MathUtils.isPowerOf2(bucketNum),
        s"Current bucket number $bucketNum is not power of 2. Unable to update to extensible-bucket.")

      FSUtils.getAllPartitionPaths(client.getEngineContext, metaClient.getStorage, basePath, false)
        .forEach(partitionPath => {
          val metadata = ExtensibleBucketIndexUtils.loadOrCreateMetadata(table, partitionPath, bucketNum)
          ValidationUtils.checkState(metadata.getBucketNum == bucketNum
            && metadata.getBucketVersion == 0
            && metadata.getLastVersionBucketNum == bucketNum
            && metadata.getInstant.equals(HoodieExtensibleBucketMetadata.INIT_INSTANT)
            && metadata.getPartitionPath.equals(partitionPath),
            s"Failed to initialize the bucket metadata for partition $partitionPath with loaded metadata $metadata")
        })
      // 3. update the table config to extensible bucket
      val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, tableName)
      val tableConfig = hoodieCatalogTable.tableConfig
      val hiveSyncConfig = buildHiveSyncConfig(sparkSession, hoodieCatalogTable, tableConfig)
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val hiveConf = new HiveConf
      hiveConf.addResource(hadoopConf)
      hiveSyncTool = new HiveSyncTool(hiveSyncConfig.getProps, hiveConf)
      hiveSyncTool.updateSimpleToExtensibleBucket(initialBucketNum)
    } finally {
      if (client != null) {
        client.close()
      }
      if (hiveSyncTool != null) {
        hiveSyncTool.close()
      }
    }
    Seq(Row(true))
  }

  override def build = new SimpleToExtensibleBucketProcedure()
}

object SimpleToExtensibleBucketProcedure {
  val NAME: String = "simple_to_extensible_bucket"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): SimpleToExtensibleBucketProcedure = new SimpleToExtensibleBucketProcedure()
  }
}

