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

package org.apache.spark.sql.hudi.command

import com.fasterxml.jackson.annotation.{JsonAutoDetect, PropertyAccessor}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.secondary.index.SecondaryIndexManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.getTableLocation
import org.apache.spark.sql.{Row, SparkSession}

import java.util

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter}

case class CreateIndexCommand(
    tableId: TableIdentifier,
    indexName: String,
    indexType: String,
    ignoreIfExists: Boolean,
    columns: Seq[(Attribute, Map[String, String])],
    options: Map[String, String],
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    val columnsMap: java.util.LinkedHashMap[String, java.util.Map[String, String]] =
      new util.LinkedHashMap[String, java.util.Map[String, String]]()
    columns.map(c => columnsMap.put(c._1.name, c._2.asJava))

    SecondaryIndexManager.getInstance().create(
      metaClient, indexName, indexType, ignoreIfExists, columnsMap, options.asJava)

    // Invalidate cached table for queries do not access related table
    // through {@code DefaultSource}
    val qualifiedTableName = QualifiedTableName(
      tableId.database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
      tableId.table)
    sparkSession.sessionState.catalog.invalidateCachedTable(qualifiedTableName)
    Seq.empty
  }
}

case class DropIndexCommand(
    tableId: TableIdentifier,
    indexName: String,
    ignoreIfNotExists: Boolean,
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    SecondaryIndexManager.getInstance().drop(metaClient, indexName, ignoreIfNotExists)

    // Invalidate cached table for queries do not access related table
    // through {@code DefaultSource}
    val qualifiedTableName = QualifiedTableName(
      tableId.database.getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase),
      tableId.table)
    sparkSession.sessionState.catalog.invalidateCachedTable(qualifiedTableName)
    Seq.empty
  }
}

case class ShowIndexesCommand(
    tableId: TableIdentifier,
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    val secondaryIndexes = SecondaryIndexManager.getInstance().show(metaClient)

    val mapper = getObjectMapper
    toScalaOption(secondaryIndexes).map(x =>
      x.asScala.map(i => {
        val colOptions =
          if (i.getColumns.values().asScala.forall(_.isEmpty)) "" else mapper.writeValueAsString(i.getColumns)
        val options = if (i.getOptions.isEmpty) "" else mapper.writeValueAsString(i.getOptions)
        Row(i.getIndexName, i.getColumns.keySet().asScala.mkString(","),
          i.getIndexType.name().toLowerCase, colOptions, options)
      }).toSeq).getOrElse(Seq.empty[Row])
  }

  protected def getObjectMapper: ObjectMapper = {
    val mapper = new ObjectMapper
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
    mapper
  }
}

case class RefreshIndexCommand(
    tableId: TableIdentifier,
    indexName: String,
    override val output: Seq[Attribute]) extends IndexBaseCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val metaClient = createHoodieTableMetaClient(tableId, sparkSession)
    SecondaryIndexManager.getInstance().refresh(metaClient, indexName)
    Seq.empty
  }
}

abstract class IndexBaseCommand extends HoodieLeafRunnableCommand with Logging {

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
}
