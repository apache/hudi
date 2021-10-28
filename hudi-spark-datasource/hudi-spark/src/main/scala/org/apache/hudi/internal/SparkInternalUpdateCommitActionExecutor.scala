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

package org.apache.hudi.internal

import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieCommitMetadata}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieCommitException
import org.apache.hudi.index.HoodieIndexUtils
import org.apache.hudi.{AvroConversionUtils, UpdateUtil}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, lit}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

import scala.collection.JavaConverters._

class SparkInternalUpdateCommitActionExecutor(
                                               @transient sqlContext: SQLContext,
                                               @transient context: HoodieEngineContext,
                                               hoodieWriteConfig: HoodieWriteConfig,
                                               instantTime: String,
                                               tblName: String) extends BaseSparkInternalCommitActionExecutor {

  @transient private val log = LogManager.getLogger(getClass)

  val fullSchema: StructType = AvroConversionUtils.convertAvroSchemaToStructType(
    HoodieAvroUtils.createHoodieWriteSchema(hoodieWriteConfig.getSchema))
  val allFieldSet: Set[String] = fullSchema.fields.map(_.name).toSet

  val updateJoinFieldSet: Set[String] = hoodieWriteConfig.getUpdateJoinFields.asScala.toSet
  val updateNullFieldSet: Set[String] = hoodieWriteConfig.getUpdateNullFields.asScala.toSet

  val SHOULD_UPDATE_FIELD_NAME = "_should_updated_"

  override def execute(inputDF: DataFrame): Unit = {
    val inputSchema = inputDF.schema

    val (updateFields, addFields) = generateUpdateAndAddFields(inputSchema)
    val updateFieldSet = updateFields.map(_.name).toSet
    validateSchema(inputSchema, updateFieldSet, addFields.map(_.name).toSet)

    val nullableFullSchema = getNullableFullSchema(updateFieldSet, addFields)
    val requiredSchema = getReadSchema(nullableFullSchema, updateFieldSet)
    val outputSchema = getWriteSchema(requiredSchema, addFields)

    val extraMetadata = Map(HoodieCommitMetadata.SCHEMA_KEY ->
      AvroConversionUtils.convertStructTypeToAvroSchema(nullableFullSchema, tblName).toString
    ).asJava

    val writerHelper = new DataSourceInternalWriterHelper(instantTime, hoodieWriteConfig,
      inputDF.schema, sqlContext.sparkSession, sqlContext.sparkContext.hadoopConfiguration, extraMetadata)
    val hoodieTable = writerHelper.getHoodieTable

    writerHelper.createInflightCommit()

    val sparkParquetInputUtil = sparkAdapter.createSparkParquetInputUtil(sqlContext
      .sparkSession.sessionState.conf)

    val readDF = sparkParquetInputUtil.getInputDataFrame(sqlContext,
      getLatestBaseFileRDD(writerHelper), requiredSchema)

    val updateToNullFieldIndex = outputSchema.fields.zipWithIndex
      .filter(tuple => updateNullFieldSet.contains(tuple._1.name))
      .map(_._2).toSet

    val renamedInputDF = inputDF
      .select(getColumns(inputDF, updateFieldSet): _*)
      .withColumn(SHOULD_UPDATE_FIELD_NAME, lit(true))

    val joinedDF = readDF
      .join(broadcast(renamedInputDF), updateJoinFieldSet.toArray, "left_outer")
      .select(getColumns(readDF) ++ getColumns(renamedInputDF): _*)

    val writeStatList = joinedDF.queryExecution.toRdd.mapPartitionsWithIndex((partId, internalRows) => {
      UpdateUtil.updateFileInternal(partId, internalRows, instantTime, hoodieTable, hoodieWriteConfig,
        outputSchema, updateToNullFieldIndex)
    }, true).collect.toList.asJava

    // TODO Add Column Family Related metadata
    context.setJobStatus(this.getClass.getSimpleName, "Commit write status collect")
    writerHelper.commit(writeStatList)
  }

  def getLatestBaseFileRDD(writerHelper: DataSourceInternalWriterHelper): RDD[HoodieBaseFile] = {
    val partitionBaseFilePath = HoodieIndexUtils
      .getLatestBaseFilesForAllPartitions(hoodieWriteConfig.getUpdatePartitionPaths,
        context, writerHelper.getHoodieTable)
      .asScala
      .map(_.getValue)
    sqlContext.sparkContext
      .parallelize(partitionBaseFilePath, partitionBaseFilePath.size)
  }

  def getLatestBaseFile(writerHelper: DataSourceInternalWriterHelper): Seq[HoodieBaseFile] = {
    HoodieIndexUtils
      .getLatestBaseFilesForAllPartitions(hoodieWriteConfig.getUpdatePartitionPaths,
        context, writerHelper.getHoodieTable)
      .asScala
      .map(_.getValue)
  }

  private def validateSchema(
                              inputSchema: StructType, updateFields: Set[String], addFields: Set[String]): Unit = {
    val inputFieldSet = inputSchema.fields.map(_.name).toSet
    if (updateJoinFieldSet.exists(!inputFieldSet.contains(_))) {
      throw new HoodieCommitException(s"Update join field $updateJoinFieldSet should be primitive" +
        "type and in input dataframe.")
    }
    updateJoinFieldSet.foreach(updateJoinField =>
      if (!allFieldSet.contains(updateJoinField)) {
        throw new HoodieCommitException(s"Update join field $updateJoinField should be primitive " +
          s"type and in the read schema.")
      })
    updateNullFieldSet.foreach(updateNullField =>
      if (!allFieldSet.contains(updateNullField)) {
        log.warn(s"Update null field $updateNullField is not in the read schema, this field " +
          s"will not be updated. ")
      } else if (updateFields.contains(updateNullField) || addFields.contains(updateNullField)) {
        throw new HoodieCommitException(s"Field $updateNullField cannot be both update null field" +
          s"and update value field.")
      }
    )
  }

  private def generateUpdateAndAddFields(inputSchema: StructType): (Array[StructField], Array[StructField]) = {
    // TODO Support nested type
    val (updateFields, addFields) = inputSchema
      .filter(field => !updateJoinFieldSet.contains(field.name))
      .partition(field => allFieldSet.contains(field.name))
    (updateFields.toArray, addFields.toArray)
  }

  def getNullableFullSchema(
                             updateFieldSet: Set[String], addFields: Array[StructField]): StructType = {
    StructType.apply(
      fullSchema.fields.map(field => {
        if (updateNullFieldSet.contains(field.name) || updateFieldSet.contains(field.name)) {
          StructField(field.name, field.dataType, true, field.metadata)
        } else {
          field
        }
      }) ++ addFields)
  }

  private def getReadSchema(nullableSchema: StructType, updateFieldSet: Set[String]): StructType = {
    // TODO Support Column Family
    nullableSchema
  }

  private def getColumns(df: DataFrame): Array[Column] = {
    getColumns(df, Set.empty)
  }

  private def getColumns(df: DataFrame, renameColumnSet: Set[String]): Array[Column] = {
    df.schema.fields.map(field => {
      if (renameColumnSet.contains(field.name)) {
        col(field.name).as("_rename_field_" + field.name)
      } else {
        col(field.name)
      }
    })
  }

  private def getWriteSchema(
                              readStructType: StructType,
                              addFields: Array[StructField]): StructType = {
    // TODO Support Column Family & Update nested fields
    if (!addFields.isEmpty) {
      throw new UnsupportedOperationException("Update with adding fields is not supported yet.")
    }
    StructType.apply(readStructType ++ addFields)
  }
}
