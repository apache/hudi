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

package org.apache.hudi

import org.apache.hudi.avro.model.HoodieClusteringGroup
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.SparkException
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isHoodieConfigKey

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, propertiesAsScalaMapConverter}

object HoodieCLIUtils {

  def createHoodieWriteClient(sparkSession: SparkSession,
                              basePath: String,
                              conf: Map[String, String],
                              tableName: Option[String]): SparkRDDWriteClient[_] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf())).build()
    val schemaUtil = new TableSchemaResolver(metaClient)
    val schemaStr = schemaUtil.getTableAvroSchema(false).toString

    // If tableName is provided, we need to add catalog props
    val catalogProps = tableName match {
      case Some(value) => HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs(
        getHoodieCatalogTable(sparkSession, value).catalogProperties)
      case None => Map.empty
    }

    // Priority: defaults < catalog props < table config < sparkSession conf < specified conf
    val finalParameters = HoodieWriterUtils.parametersWithWriteDefaults(
      (catalogProps ++
        metaClient.getTableConfig.getProps.asScala.toMap ++
        sparkSession.sqlContext.getAllConfs.filterKeys(isHoodieConfigKey) ++
        conf).toMap
    )

    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    DataSourceUtils.createHoodieClient(jsc, schemaStr, basePath,
      metaClient.getTableConfig.getTableName, finalParameters.asJava)
  }

  def extractPartitions(clusteringGroups: Seq[HoodieClusteringGroup]): String = {
    var partitionPaths: Seq[String] = Seq.empty
    clusteringGroups.foreach(g =>
      g.getSlices.asScala.foreach(slice =>
        partitionPaths = partitionPaths :+ slice.getPartitionPath
      )
    )

    partitionPaths.sorted.mkString(",")
  }

  def getHoodieCatalogTable(sparkSession: SparkSession, table: String): HoodieCatalogTable = {
    val seq: Seq[String] = table.split('.')
    seq match {
      case Seq(tableName) =>
        HoodieCatalogTable(sparkSession, TableIdentifier(tableName))
      case Seq(database, tableName) =>
        HoodieCatalogTable(sparkSession, TableIdentifier(tableName, Some(database)))
      case _ =>
        throw new SparkException(s"Unsupported identifier $table")
    }
  }

  def getTableIdentifier(table: String): (String, Option[String]) = {
    val arrayStr: Array[String] = table.split('.')
    arrayStr.toSeq match {
      case Seq(tableName) =>
        (tableName, None)
      case Seq(database, tableName) =>
        (tableName, Some(database))
      case _ =>
        throw new SparkException(s"Unsupported identifier $table")
    }
  }

  def extractOptions(s: String): Map[String, String] = {
    StringUtils.split(s, ",").asScala
      .map(split => StringUtils.split(split, "="))
      .map(pair => pair.get(0) -> pair.get(1))
      .toMap
  }

  def getLockOptions(tablePath: String): Map[String, String] = {
    val props = FileSystemBasedLockProvider.getLockConfig(tablePath)
    props.stringPropertyNames.asScala
      .map(key => key -> props.getString(key))
      .toMap
  }
}
