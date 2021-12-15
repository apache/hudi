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

package org.apache.hudi

import java.util.Properties

import org.apache.hudi.DataSourceOptionsHelper.allAlternatives
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE
import org.apache.hudi.common.config.{DFSPropertiesConfiguration, HoodieConfig, TypedProperties}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.command.SqlKeyGenerator

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters._

/**
 * WriterUtils to assist in write path in Datasource and tests.
 */
object HoodieWriterUtils {

  def javaParametersWithWriteDefaults(parameters: java.util.Map[String, String]): java.util.Map[String, String] = {
    mapAsJavaMap(parametersWithWriteDefaults(parameters.asScala.toMap))
  }

  /**
    * Add default options for unspecified write options keys.
    *
    * @param parameters
    * @return
    */
  def parametersWithWriteDefaults(parameters: Map[String, String]): Map[String, String] = {
    val globalProps = DFSPropertiesConfiguration.getGlobalProps.asScala
    val props = new Properties()
    props.putAll(parameters)
    val hoodieConfig: HoodieConfig = new HoodieConfig(props)
    hoodieConfig.setDefaultValue(OPERATION)
    hoodieConfig.setDefaultValue(TABLE_TYPE)
    hoodieConfig.setDefaultValue(PRECOMBINE_FIELD)
    hoodieConfig.setDefaultValue(PAYLOAD_CLASS_NAME)
    hoodieConfig.setDefaultValue(RECORDKEY_FIELD)
    hoodieConfig.setDefaultValue(KEYGENERATOR_CLASS_NAME)
    hoodieConfig.setDefaultValue(ENABLE)
    hoodieConfig.setDefaultValue(COMMIT_METADATA_KEYPREFIX)
    hoodieConfig.setDefaultValue(INSERT_DROP_DUPS)
    hoodieConfig.setDefaultValue(STREAMING_RETRY_CNT)
    hoodieConfig.setDefaultValue(STREAMING_RETRY_INTERVAL_MS)
    hoodieConfig.setDefaultValue(STREAMING_IGNORE_FAILED_BATCH)
    hoodieConfig.setDefaultValue(META_SYNC_CLIENT_TOOL_CLASS_NAME)
    hoodieConfig.setDefaultValue(HIVE_SYNC_ENABLED)
    hoodieConfig.setDefaultValue(META_SYNC_ENABLED)
    hoodieConfig.setDefaultValue(HIVE_DATABASE)
    hoodieConfig.setDefaultValue(HIVE_TABLE)
    hoodieConfig.setDefaultValue(HIVE_BASE_FILE_FORMAT)
    hoodieConfig.setDefaultValue(HIVE_USER)
    hoodieConfig.setDefaultValue(HIVE_PASS)
    hoodieConfig.setDefaultValue(HIVE_URL)
    hoodieConfig.setDefaultValue(HIVE_PARTITION_FIELDS)
    hoodieConfig.setDefaultValue(HIVE_PARTITION_EXTRACTOR_CLASS)
    hoodieConfig.setDefaultValue(HIVE_STYLE_PARTITIONING)
    hoodieConfig.setDefaultValue(HIVE_USE_JDBC)
    hoodieConfig.setDefaultValue(HIVE_CREATE_MANAGED_TABLE)
    hoodieConfig.setDefaultValue(HIVE_SYNC_AS_DATA_SOURCE_TABLE)
    hoodieConfig.setDefaultValue(ASYNC_COMPACT_ENABLE)
    hoodieConfig.setDefaultValue(INLINE_CLUSTERING_ENABLE)
    hoodieConfig.setDefaultValue(ASYNC_CLUSTERING_ENABLE)
    hoodieConfig.setDefaultValue(ENABLE_ROW_WRITER)
    hoodieConfig.setDefaultValue(RECONCILE_SCHEMA)
    hoodieConfig.setDefaultValue(DROP_PARTITION_COLUMNS)
    Map() ++ hoodieConfig.getProps.asScala ++ globalProps ++ DataSourceOptionsHelper.translateConfigurations(parameters)
  }

  def toProperties(params: Map[String, String]): TypedProperties = {
    val props = new TypedProperties()
    params.foreach(kv => props.setProperty(kv._1, kv._2))
    props
  }

  /**
   * Get the partition columns to stored to hoodie.properties.
   * @param parameters
   * @return
   */
  def getPartitionColumns(parameters: Map[String, String]): String = {
    val props = new Properties()
    props.putAll(parameters.asJava)
    HoodieSparkUtils.getPartitionColumns(props)
  }

  def convertMapToHoodieConfig(parameters: Map[String, String]): HoodieConfig = {
    val properties = new Properties()
    properties.putAll(mapAsJavaMap(parameters))
    new HoodieConfig(properties)
  }

  def getOriginKeyGenerator(parameters: Map[String, String]): String = {
    val kg = parameters.getOrElse(KEYGENERATOR_CLASS_NAME.key(), null)
    if (classOf[SqlKeyGenerator].getCanonicalName == kg) {
      parameters.getOrElse(SqlKeyGenerator.ORIGIN_KEYGEN_CLASS_NAME, null)
    } else {
      kg
    }
  }

  /**
   * Detects conflicts between new parameters and existing table configurations
   */
  def validateTableConfig(spark: SparkSession, params: Map[String, String],
      tableConfig: HoodieConfig): Unit = {
    val resolver = spark.sessionState.conf.resolver
    val diffConfigs = StringBuilder.newBuilder
    params.foreach { case (key, value) =>
      val existingValue = getStringFromTableConfigWithAlternatives(tableConfig, key)
      if (null != existingValue && !resolver(existingValue, value)) {
        diffConfigs.append(s"$key:\t$value\t${tableConfig.getString(key)}\n")
      }
    }

    if (null != tableConfig) {
      val datasourceRecordKey = params.getOrElse(RECORDKEY_FIELD.key(), null)
      val tableConfigRecordKey = tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS)
      if (null != datasourceRecordKey && null != tableConfigRecordKey
          && datasourceRecordKey != tableConfigRecordKey) {
        diffConfigs.append(s"RecordKey:\t$datasourceRecordKey\t$tableConfigRecordKey\n")
      }

      val datasourcePreCombineKey = params.getOrElse(PRECOMBINE_FIELD.key(), null)
      val tableConfigPreCombineKey = tableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD)
      if (null != datasourcePreCombineKey && null != tableConfigPreCombineKey
          && datasourcePreCombineKey != tableConfigPreCombineKey) {
        diffConfigs.append(s"PreCombineKey:\t$datasourcePreCombineKey\t$tableConfigPreCombineKey\n")
      }

      val datasourceKeyGen = getOriginKeyGenerator(params)
      val tableConfigKeyGen = tableConfig.getString(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME)
      if (null != datasourceKeyGen && null != tableConfigKeyGen
          && datasourceKeyGen != tableConfigKeyGen) {
        diffConfigs.append(s"KeyGenerator:\t$datasourceKeyGen\t$tableConfigKeyGen\n")
      }
    }

    if (diffConfigs.nonEmpty) {
      diffConfigs.insert(0, "\nConfig conflict(key\tcurrent value\texisting value):\n")
      throw new HoodieException(diffConfigs.toString.trim)
    }
  }

  private def getStringFromTableConfigWithAlternatives(tableConfig: HoodieConfig, key: String): String = {
    if (null == tableConfig) {
      null
    } else {
      if (allAlternatives.contains(key)) {
        tableConfig.getString(allAlternatives(key))
      } else {
        tableConfig.getString(key)
      }
    }
  }

  val sparkDatasourceConfigsToTableConfigsMap = Map(
    TABLE_NAME -> HoodieTableConfig.NAME,
    TABLE_TYPE -> HoodieTableConfig.TYPE,
    PRECOMBINE_FIELD -> HoodieTableConfig.PRECOMBINE_FIELD,
    PARTITIONPATH_FIELD -> HoodieTableConfig.PARTITION_FIELDS,
    RECORDKEY_FIELD -> HoodieTableConfig.RECORDKEY_FIELDS,
    PAYLOAD_CLASS_NAME -> HoodieTableConfig.PAYLOAD_CLASS_NAME
  )
  def mappingSparkDatasourceConfigsToTableConfigs(options: Map[String, String]): Map[String, String] = {
    val includingTableConfigs = scala.collection.mutable.Map() ++ options
    sparkDatasourceConfigsToTableConfigsMap.foreach(kv => {
      if (options.containsKey(kv._1.key)) {
        includingTableConfigs(kv._2.key) = options(kv._1.key)
        includingTableConfigs.remove(kv._1.key)
      }
    })
    includingTableConfigs.toMap
  }
}
