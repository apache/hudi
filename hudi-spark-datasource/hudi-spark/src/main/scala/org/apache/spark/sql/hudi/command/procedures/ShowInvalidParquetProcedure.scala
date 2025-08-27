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
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.{HoodieTableMetadata, NativeTableMetadataFactory}
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage

import collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

class ShowInvalidParquetProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "path", DataTypes.StringType),
    ProcedureParameter.optional(1, "parallelism", DataTypes.IntegerType, 100),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 100),
    ProcedureParameter.optional(3, "needDelete", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "partitions", DataTypes.StringType, ""),
    ProcedureParameter.optional(5, "instants", DataTypes.StringType, "")
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("path", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val srcPath = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val parallelism = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Int]
    val limit = getArgValueOrDefault(args, PARAMETERS(2))
    val needDelete = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Boolean]
    val partitions = getArgValueOrDefault(args, PARAMETERS(4)).map(_.toString).getOrElse("")
    val instants = getArgValueOrDefault(args, PARAMETERS(5)).map(_.toString).getOrElse("")

    val storageConf = HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())
    val storage = new HoodieHadoopStorage(srcPath, storageConf)
    val metadataConfig = HoodieMetadataConfig.newBuilder.enable(false).build
    val metadata = NativeTableMetadataFactory.getInstance().create(new HoodieSparkEngineContext(jsc), storage, metadataConfig, srcPath)
    val partitionPaths: java.util.List[String] = metadata.getPartitionPathWithPathPrefixes(partitions.split(",").toList.asJava)
    val instantsList = if (StringUtils.isNullOrEmpty(instants)) Array.empty[String] else instants.split(",")
    val fileStatus = partitionPaths.asScala.flatMap(part => {
      val fs = HadoopFSUtils.getFs(new Path(srcPath), storageConf.unwrap())
      HadoopFSUtils.getAllDataFilesInPartition(fs, HadoopFSUtils.constructAbsolutePathInHadoopPath(srcPath, part))
    }).toList

    if (fileStatus.isEmpty) {
      Seq.empty
    } else {
      val parquetRdd = jsc.parallelize(fileStatus, Math.min(fileStatus.size, parallelism)).filter(fileStatus => {
        if (instantsList.nonEmpty) {
          val parquetCommitTime = FSUtils.getCommitTimeWithFullPath(fileStatus.getPath.toString)
          instantsList.contains(parquetCommitTime)
        } else {
          true
        }
      }).filter(status => {
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
      }).map(status => Row(status.getPath.toString))

      if (limit.isDefined) {
        parquetRdd.take(limit.get.asInstanceOf[Int])
      } else {
        parquetRdd.collect()
      }
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