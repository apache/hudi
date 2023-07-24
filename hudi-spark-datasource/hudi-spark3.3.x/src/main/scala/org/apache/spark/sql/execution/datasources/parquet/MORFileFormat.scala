/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.{DataSourceReadOptions, HoodieBaseRelation, HoodieTableSchema, HoodieTableState, InternalRowBroadcast, RecordMergingFileIterator, SkipMergeIterator}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.jdk.CollectionConverters.asScalaIteratorConverter

class MORFileFormat(private val shouldAppendPartitionValues: Boolean,
                    tableState: Broadcast[HoodieTableState],
                    tableSchema: Broadcast[HoodieTableSchema],
                    tableName: String,
                    mergeType: String,
                    mandatoryFields: Seq[String]) extends Spark33HoodieParquetFileFormat(shouldAppendPartitionValues) {
  var isProjected = false

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = false

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val dataSchemaWithPartition = StructType(dataSchema.fields ++ partitionSchema.fields)
    val requiredSchemaWithMandatoryFields = requiredSchema.fields.toBuffer
    val partitionSchemaForReader = if (shouldAppendPartitionValues) {
      partitionSchema
    } else {
      requiredSchemaWithMandatoryFields.append(partitionSchema.fields:_*)
      StructType(Seq.empty)
    }
    for (field <- mandatoryFields) {
      if (requiredSchema.getFieldIndex(field).isEmpty) {
        val fieldToAdd = dataSchemaWithPartition.fields(dataSchemaWithPartition.getFieldIndex(field).get)
        requiredSchemaWithMandatoryFields.append(fieldToAdd)
      }
    }
    val requiredSchemaWithMandatory = StructType(requiredSchemaWithMandatoryFields.toArray)
    val fileReader =   super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchemaForReader,
      requiredSchemaWithMandatory, filters, options, hadoopConf, allowVectorized = false)
    val morReader = super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchemaForReader,
      requiredSchemaWithMandatory, Seq.empty, options, hadoopConf, allowVectorized = false)
    val broadcastedHadoopConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
     file.partitionValues match {
        case broadcast: InternalRowBroadcast =>
          val filePath = new Path(new URI(file.filePath))
          val fileSliceOpt = broadcast.getSlice(FSUtils.getFileId(filePath.getName))
          fileSliceOpt match {
            case Some(fileSlice) =>
              val logFiles = fileSlice.getLogFiles.sorted(HoodieLogFile.getLogFileComparator).iterator().asScala.toList
              val requiredAvroSchema = HoodieBaseRelation.convertToAvroSchema(requiredSchemaWithMandatory, tableName)
              mergeType match {
                case DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL =>
                  new SkipMergeIterator(logFiles, filePath.getParent, morReader(file), requiredSchemaWithMandatory,
                    tableSchema.value, requiredSchemaWithMandatory, requiredAvroSchema, tableState.value,
                    broadcastedHadoopConf.value.value)
                case DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL =>
                  new RecordMergingFileIterator(logFiles, filePath.getParent, morReader(file), requiredSchemaWithMandatory,
                    tableSchema.value, requiredSchemaWithMandatory, requiredAvroSchema, tableState.value,
                    broadcastedHadoopConf.value.value)
              }
            case _ => fileReader(file)
          }
        case _ =>
          fileReader(file)
      }
    }
  }
}
