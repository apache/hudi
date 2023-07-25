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
import org.apache.spark.sql.{HoodieCatalystExpressionUtils, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.mutable
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
    val partitionSchemaForReader = if (shouldAppendPartitionValues) {
      partitionSchema
    } else {
      //requiredSchemaWithMandatoryFields.append(partitionSchema.fields:_*)
      StructType(Seq.empty)
    }
    val added: mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    for (field <- mandatoryFields) {
      if (requiredSchema.getFieldIndex(field).isEmpty) {
        val fieldToAdd = dataSchemaWithPartition.fields(dataSchemaWithPartition.getFieldIndex(field).get)
        added.append(fieldToAdd)
      }
    }

    val addedFields = StructType(added.toArray)
    val requiredSchemaWithMandatory = StructType(requiredSchema.toArray ++ addedFields.fields)
    val fileReader =   super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema,
      requiredSchema, filters, options, hadoopConf, true, allowVectorized = false, "file")
    val morReader = super.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchemaForReader,
      requiredSchemaWithMandatory, Seq.empty, options, hadoopConf, shouldAppendPartitionValues, allowVectorized = false, "mor")
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
              val iter = mergeType match {
                case DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL =>
                  new SkipMergeIterator(logFiles, filePath.getParent, morReader(file), requiredSchemaWithMandatory,
                    tableSchema.value, requiredSchemaWithMandatory, requiredAvroSchema, tableState.value,
                    broadcastedHadoopConf.value.value)
                case DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL =>
                  new RecordMergingFileIterator(logFiles, filePath.getParent, morReader(file), requiredSchemaWithMandatory,
                    tableSchema.value, requiredSchemaWithMandatory, requiredAvroSchema, tableState.value,
                    broadcastedHadoopConf.value.value)
              }
              if (partitionSchema.nonEmpty && !shouldAppendPartitionValues) {
                addProj(iter, requiredSchema,  addedFields, partitionSchema, broadcast.getInternalRow)
              } else {
                iter
              }
            case _ => fileReader(file)
          }
        case _ =>
          fileReader(file)
      }
    }
  }

  def addProj(iter: Iterator[InternalRow],
              requiredSchema: StructType,
              addedSchema: StructType,
              partitionSchema: StructType,
              partitionValues: InternalRow): Iterator[InternalRow] = {
    val unsafeProjection =  HoodieCatalystExpressionUtils.generateUnsafeProjection(StructType(requiredSchema ++ addedSchema ++ partitionSchema),  StructType(requiredSchema ++ partitionSchema))
    val joinedRow = new JoinedRow()
    iter.map(d => unsafeProjection(joinedRow(d, partitionValues)))
  }
}

