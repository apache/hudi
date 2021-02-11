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
    Map(OPERATION.key -> OPERATION.defaultValue,
      TABLE_TYPE.key -> TABLE_TYPE.defaultValue,
      PRECOMBINE_FIELD.key -> PRECOMBINE_FIELD.defaultValue,
      PAYLOAD_CLASS_NAME.key -> PAYLOAD_CLASS_NAME.defaultValue,
      RECORDKEY_FIELD.key -> RECORDKEY_FIELD.defaultValue,
      PARTITIONPATH_FIELD.key -> PARTITIONPATH_FIELD.defaultValue,
      KEYGENERATOR_CLASS_NAME.key -> DEFAULT_KEYGENERATOR_CLASS_OPT_VAL,
      ENABLE.key -> ENABLE.defaultValue.toString,
      COMMIT_METADATA_KEYPREFIX.key -> COMMIT_METADATA_KEYPREFIX.defaultValue,
      INSERT_DROP_DUPS.key -> INSERT_DROP_DUPS.defaultValue,
      STREAMING_RETRY_CNT.key -> STREAMING_RETRY_CNT.defaultValue,
      STREAMING_RETRY_INTERVAL_MS.key -> STREAMING_RETRY_INTERVAL_MS.defaultValue,
      STREAMING_IGNORE_FAILED_BATCH.key -> STREAMING_IGNORE_FAILED_BATCH.defaultValue,
      META_SYNC_CLIENT_TOOL_CLASS_NAME.key -> META_SYNC_CLIENT_TOOL_CLASS_NAME.defaultValue,
      HIVE_SYNC_ENABLED.key -> HIVE_SYNC_ENABLED.defaultValue,
      META_SYNC_ENABLED.key -> META_SYNC_ENABLED.defaultValue,
      HIVE_DATABASE.key -> HIVE_DATABASE.defaultValue,
      HIVE_TABLE.key -> HIVE_TABLE.defaultValue,
      HIVE_BASE_FILE_FORMAT.key -> HIVE_BASE_FILE_FORMAT.defaultValue,
      HIVE_USER.key -> HIVE_USER.defaultValue,
      HIVE_PASS.key -> HIVE_PASS.defaultValue,
      HIVE_URL.key -> HIVE_URL.defaultValue,
      HIVE_PARTITION_FIELDS.key -> HIVE_PARTITION_FIELDS.defaultValue,
      HIVE_PARTITION_EXTRACTOR_CLASS.key -> HIVE_PARTITION_EXTRACTOR_CLASS.defaultValue,
      HIVE_STYLE_PARTITIONING.key -> HIVE_STYLE_PARTITIONING.defaultValue,
      HIVE_USE_JDBC.key -> HIVE_USE_JDBC.defaultValue,
      HIVE_CREATE_MANAGED_TABLE.key() -> HIVE_CREATE_MANAGED_TABLE.defaultValue.toString,
      HIVE_SYNC_AS_DATA_SOURCE_TABLE.key() -> HIVE_SYNC_AS_DATA_SOURCE_TABLE.defaultValue(),
      ASYNC_COMPACT_ENABLE.key -> ASYNC_COMPACT_ENABLE.defaultValue,
      INLINE_CLUSTERING_ENABLE.key -> INLINE_CLUSTERING_ENABLE.defaultValue,
      ASYNC_CLUSTERING_ENABLE.key -> ASYNC_CLUSTERING_ENABLE.defaultValue,
      ENABLE_ROW_WRITER.key -> ENABLE_ROW_WRITER.defaultValue,
      RECONCILE_SCHEMA.key -> RECONCILE_SCHEMA.defaultValue.toString,
      DROP_PARTITION_COLUMNS.key -> DROP_PARTITION_COLUMNS.defaultValue
    ) ++ globalProps ++ DataSourceOptionsHelper.translateConfigurations(parameters)
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
