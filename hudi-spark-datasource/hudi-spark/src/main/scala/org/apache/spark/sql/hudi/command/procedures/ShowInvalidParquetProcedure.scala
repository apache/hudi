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

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.command.procedures
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import collection.JavaConverters._

class ShowInvalidParquetProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "path", DataTypes.StringType),
    ProcedureParameter.optional(1, "limit", DataTypes.IntegerType, 100),
    ProcedureParameter.optional(2, "needDelete", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "partitions", DataTypes.StringType, ""),
    ProcedureParameter.optional(4, "instants", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("path", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val srcPath = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(1))
    val needDelete = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]
    val partitions = getArgValueOrDefault(args, PARAMETERS(3)).map(_.toString).getOrElse("")
    val instants = getArgValueOrDefault(args, PARAMETERS(4)).map(_.toString).getOrElse("")
    val storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())
    val storage = new HoodieHadoopStorage(srcPath, storageConf)
    val metadataConfig = HoodieMetadataConfig.newBuilder.enable(false).build
    val metadata = HoodieTableMetadata.create(new HoodieSparkEngineContext(jsc), storage, metadataConfig, srcPath)
    val partitionPaths: java.util.List[String] = metadata.getPartitionPathWithPathPrefixes(partitions.split(",").toList.asJava)
    val partitionPathsSize = if (partitionPaths.size() == 0) 1 else partitionPaths.size()
    val instantsList = if (StringUtils.isNullOrEmpty(instants)) Array.empty[String] else instants.split(",")

    val javaRdd: JavaRDD[String] = jsc.parallelize(partitionPaths, partitionPathsSize)
    val parquetRdd = javaRdd.rdd.map(part => {
        val fs = HadoopFSUtils.getFs(new Path(srcPath), storageConf.unwrap())
        HadoopFSUtils.getAllDataFilesInPartition(fs, HadoopFSUtils.constructAbsolutePathInHadoopPath(srcPath, part)).filter(fileStatus => {
          var isFilter = true
          if (!instantsList.isEmpty) {
            val parquetCommitTime = FSUtils.getCommitTimeWithFullPath(fileStatus.getPath.toString)
            isFilter = instantsList.contains(parquetCommitTime)
          }
          isFilter
        })
    }).flatMap(_.toList)
      .filter(status => {
        val filePath = status.getPath
        var isInvalid = false
        if (filePath.toString.endsWith(".parquet")) {
          try ParquetFileReader.readFooter(storageConf.unwrap(), filePath, SKIP_ROW_GROUPS).getFileMetaData catch {
            case e: Exception =>
              isInvalid = e.getMessage.contains("is not a Parquet file")
              if (isInvalid && needDelete) {
                val fs = HadoopFSUtils.getFs(new Path(srcPath), storageConf.unwrap())
                try {
                  isInvalid = !fs.delete(filePath, false)
                } catch {
                  case ex: Exception =>
                    isInvalid = true
                }
              }
          }
        }
        isInvalid
      })
      .map(status => Row(status.getPath.toString))
    if (limit.isDefined) {
      parquetRdd.take(limit.get.asInstanceOf[Int])
    } else {
      parquetRdd.collect()
    }
  }

  override def build = new ShowInvalidParquetProcedure()
}

object ShowInvalidParquetProcedure {
  val NAME = "show_invalid_parquet"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowInvalidParquetProcedure()
  }
}
