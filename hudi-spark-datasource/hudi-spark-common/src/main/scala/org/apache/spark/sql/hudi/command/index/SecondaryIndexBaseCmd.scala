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

package org.apache.spark.sql.hudi.command.index

import com.fasterxml.jackson.annotation.{JsonAutoDetect, PropertyAccessor}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.index.SecondaryIndexField
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.exception.HoodieSecondaryIndexException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.Properties
import scala.collection.JavaConversions

trait SecondaryIndexBaseCmd extends Logging {

  def createSecondaryIndex(
      sparkSession: SparkSession,
      indexName: String,
      tableId: TableIdentifier,
      colName: String,
      indexType: String): Seq[InternalRow] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    val indexFields = toScalaOption(metaClient.getTableConfig.getSecondaryIndexFields)
    if (indexExists(indexFields, indexName, Some(colName))) {
      throw new HoodieSecondaryIndexException("Secondary index already exists:" + indexName)
    }

    val newIndexField = SecondaryIndexField.builder()
        .setIndexName(indexName)
        .setIndexColumn(colName)
        .setIndexType(indexType)
        .build()

    val newIndexFields = indexFields.map(_ :+ newIndexField).getOrElse(Array(newIndexField))
    val updatedProps = new Properties()
    updatedProps.put(HoodieTableConfig.SECONDARY_INDEX_FIELDS.key(),
      getObjectMapper.writeValueAsString(newIndexFields.sortBy(_.getIndexName)))
    HoodieTableConfig.update(metaClient.getFs, new Path(metaClient.getMetaPath), updatedProps)
    logInfo(s"Success to create secondary index: $newIndexField")

    Seq.empty
  }

  def showSecondaryIndexes(
      sparkSession: SparkSession,
      tableId: TableIdentifier): Seq[InternalRow] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    val indexFields = metaClient.getTableConfig.getSecondaryIndexFields

    toScalaOption(indexFields).map(_.map(i =>
      InternalRow(
        UTF8String.fromString(i.getIndexName),
        UTF8String.fromString(i.getIndexColumn),
        UTF8String.fromString(i.getIndexType.name().toLowerCase)
      )
    ).toSeq).getOrElse(Seq.empty[InternalRow])
  }

  def dropSecondaryIndex(
      sparkSession: SparkSession,
      indexName: String,
      tableId: TableIdentifier): Seq[InternalRow] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    val indexFields = toScalaOption(metaClient.getTableConfig.getSecondaryIndexFields)
    if (!indexExists(indexFields, indexName)) {
      throw new HoodieSecondaryIndexException("Secondary index not exists:" + indexName)
    }

    val indexFieldsToKeep =
      indexFields.map(_.filter(!_.getIndexName.equals(indexName))).filter(!_.isEmpty)
    if (indexFieldsToKeep.nonEmpty) {
      val updatedProps = new Properties()
      updatedProps.put(HoodieTableConfig.SECONDARY_INDEX_FIELDS.key(),
        getObjectMapper.writeValueAsString(indexFieldsToKeep.get.sortBy(_.getIndexName)))
      HoodieTableConfig.update(metaClient.getFs, new Path(metaClient.getMetaPath), updatedProps)
    } else {
      HoodieTableConfig.delete(metaClient.getFs, new Path(metaClient.getMetaPath),
        JavaConversions.setAsJavaSet(Set(HoodieTableConfig.SECONDARY_INDEX_FIELDS.key())))
    }

    // TODO: need to delete index files safely(index files maybe used by running queries)
    logInfo(s"Success to drop secondary index: ${indexName}")

    Seq.empty
  }

  /**
   * Create hoodie table meta client according to given table identifier and
   * spark session
   *
   * @param tableId      The table identifier
   * @param sparkSession The spark session
   * @return The hoodie table meta client
   */
  def createHoodieTableMetaClient(
      tableId: TableIdentifier,
      sparkSession: SparkSession): HoodieTableMetaClient = {
    val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(tableId)
    val basePath = getTableLocation(catalogTable, sparkSession)

    HoodieTableMetaClient.builder()
        .setConf(sparkSession.sqlContext.sparkContext.hadoopConfiguration)
        .setBasePath(basePath)
        .build()
  }

  /**
   * Check secondary index exists. In secondary index definition, index name
   * must be unique, so the index name will be checked firstly,
   *
   * @param indexFields Current secondary indexes
   * @param indexName   The index name to check
   * @param indexColumn The column name to check
   * @return true if the secondary index exists
   */
  def indexExists(
      indexFields: Option[Array[SecondaryIndexField]],
      indexName: String,
      indexColumn: Option[String] = None): Boolean = {
    indexFields.exists(i => {
      i.exists(_.getIndexName.equals(indexName)) ||
          // Column name need to be checked if it's given
          indexColumn.exists(column => i.exists(_.getIndexColumn.equals(column)))
    })
  }

  protected def toScalaOption[T](opt: org.apache.hudi.common.util.Option[T]): Option[T] =
    if (opt.isPresent) Some(opt.get) else None

  protected def getObjectMapper: ObjectMapper = {
    val mapper = new ObjectMapper
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
    mapper
  }

  def toStructType: Seq[Attribute] => StructType = (attrs: Seq[Attribute]) =>
    StructType(attrs.map(f => StructField(f.name, f.dataType, f.nullable, f.metadata)).toArray)
}
