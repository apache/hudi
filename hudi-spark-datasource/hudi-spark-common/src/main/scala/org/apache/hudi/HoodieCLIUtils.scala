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
import org.apache.hudi.common.config.{HoodieCommonConfig, TypedProperties}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StorageSchemes

import org.apache.spark.SparkException
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.hudi.HoodieOptionConfig
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.filterHoodieConfigs

import java.util.ArrayList

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, propertiesAsScalaMapConverter}

object HoodieCLIUtils extends Logging {

  def createHoodieWriteClient(sparkSession: SparkSession,
                              basePath: String,
                              conf: Map[String, String],
                              tableName: Option[String]): SparkRDDWriteClient[_] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
      .setConf(HadoopFSUtils.getStorageConf(sparkSession.sessionState.newHadoopConf())).build()
    val schemaUtil = new TableSchemaResolver(metaClient)
    val schemaStr = schemaUtil.getTableSchema(false).toString

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
        filterHoodieConfigs(sparkSession.sqlContext.getAllConfs) ++
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

  /**
   * Parse a comma-separated string of key=value pairs into a Map.
   *
   * Notes:
   *  - Whitespace surrounding keys/values is trimmed; empty tokens (e.g. from a
   *    trailing comma or `", ,"`) are silently ignored.
   *  - The delimiter is the first `=` in a token, so values may themselves
   *    contain `=` (e.g. `k=a=b` parses to `k -> "a=b"`).
   *  - Values cannot contain literal commas; the parser does not support
   *    escaping. Configs that need commas should be set via Spark conf instead.
   *  - If the same key appears more than once, a WARN is logged and the last
   *    occurrence wins (consistent with `toMap`'s last-write-wins semantics).
   *
   * @throws IllegalArgumentException if a non-empty token does not contain `=`
   *                                  or has an empty key.
   */
  def extractOptions(s: String): Map[String, String] = {
    if (s == null) {
      Map.empty
    } else {
      // Single pass: build the result Map and collect duplicate keys at the
      // same time, avoiding an intermediate Seq + groupBy + toMap chain.
      val (result, duplicates) = StringUtils.split(s, ",").asScala
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(parseOptionToken)
        .foldLeft((Map.empty[String, String], Set.empty[String])) {
          case ((acc, dups), (key, value)) =>
            val newDups = if (acc.contains(key)) dups + key else dups
            (acc + (key -> value), newDups)
        }

      if (duplicates.nonEmpty) {
        logWarning(s"Duplicate option keys detected: ${duplicates.mkString(", ")}. "
          + "The last occurrence will take effect.")
      }
      result
    }
  }

  private def parseOptionToken(token: String): (String, String) = {
    val delimiterIndex = token.indexOf('=')
    if (delimiterIndex <= 0) {
      throw new IllegalArgumentException(
        s"Invalid options format: '$token'. Expected 'key=value' pairs separated by commas, "
          + "for example: 'k1=v1,k2=v2'.")
    }

    val key = token.substring(0, delimiterIndex).trim
    if (key.isEmpty) {
      throw new IllegalArgumentException(
        s"Invalid options format: '$token'. Option key must not be empty and options should "
          + "follow 'key=value' format.")
    }

    val value = token.substring(delimiterIndex + 1).trim
    key -> value
  }

  def getLockOptions(tablePath: String, schema: String, lockConfig: TypedProperties): Map[String, String] = {
    val customSupportedFSs = lockConfig.getStringList(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key, ",", new ArrayList[String])
    if (schema == null || customSupportedFSs.contains(schema) || StorageSchemes.isAtomicCreationSupported(schema)) {
      logInfo("Auto config filesystem lock provider for metadata table")
      val props = FileSystemBasedLockProvider.getLockConfig(tablePath)
      props.stringPropertyNames.asScala
        .map(key => key -> props.getString(key))
        .toMap
    } else {
      Map.empty
    }
  }
}
