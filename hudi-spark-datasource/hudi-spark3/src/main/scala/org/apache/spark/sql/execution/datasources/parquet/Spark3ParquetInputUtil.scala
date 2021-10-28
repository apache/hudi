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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputSplit, ParquetRecordReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

class Spark3ParquetInputUtil extends SparkParquetInputUtil {

  def getInputDataFrame(sqlContext: SQLContext,
                        baseFileRDD: RDD[HoodieBaseFile],
                        readStructType: StructType): DataFrame = {
    val inputRDD = baseFileRDD.flatMap(baseFile => {
      getInternalRowIterator(baseFile, sqlContext.sparkContext.hadoopConfiguration)
    })
    sqlContext.sparkSession.internalCreateDataFrame(inputRDD, readStructType, isStreaming = false)
  }

  def getInternalRowIterator(baseFile: HoodieBaseFile, hadoopConf: Configuration): RecordReaderIterator[InternalRow] = {
    val filePath = new Path(baseFile.getPath)
    val inputSplit = new ParquetInputSplit(filePath, 0, baseFile.getFileLen, baseFile.getFileLen,
      Array.empty, null)

    // TODO Make sure this input variable is valid
    val readSupport = constructReadSupport(inputSplit, hadoopConf)
    val reader = new ParquetRecordReader[InternalRow](readSupport)
    val iter = new RecordReaderIterator[InternalRow](reader)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val attemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId)

    reader.initialize(inputSplit, attemptContext)
    iter
  }

  def getAttemptContext(serializableConf: SerializableConfiguration, readStructType: StructType): TaskAttemptContextImpl = {
    val hadoopConf = serializableConf.get()
    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, readStructType.json)
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, readStructType.json)

    hadoopConf.set(SQLConf.PARQUET_BINARY_AS_STRING.key, "false")
    hadoopConf.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, "true")
    new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID(new TaskID(
      new JobID(), TaskType.MAP, 0), 0))
  }

  private def constructReadSupport(inputSplit: InputSplit, hadoopConf: Configuration): ParquetReadSupport = {

    val timestampConversion = hadoopConf.getBoolean(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key, false)
    val filePath = inputSplit.asInstanceOf[ParquetInputSplit].getPath
    val footerFileMetaData =
      ParquetFileReader.readFooter(hadoopConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    val createBy = footerFileMetaData.getCreatedBy
    val kvMetaData = footerFileMetaData.getKeyValueMetaData

    val isCreatedByParquetMr = createBy.startsWith("parquet-mr")
    val convertTz =
      if (timestampConversion && !isCreatedByParquetMr) {
        Some(DateTimeUtils.getZoneId(hadoopConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }
    val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
      kvMetaData.get,
      SQLConf.get.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ))
    new ParquetReadSupport(convertTz, false, datetimeRebaseMode)
  }
}
