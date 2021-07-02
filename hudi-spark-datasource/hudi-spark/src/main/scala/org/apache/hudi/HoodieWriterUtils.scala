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

import scala.collection.JavaConverters._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{HoodieConfig, TypedProperties}

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_ENABLE_PROP
import org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_VALIDATE_PROP
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.keygen.{BaseKeyGenerator, CustomAvroKeyGenerator, CustomKeyGenerator, KeyGenerator}

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
    Map(OPERATION_OPT_KEY.key -> OPERATION_OPT_KEY.defaultValue,
      TABLE_TYPE_OPT_KEY.key -> TABLE_TYPE_OPT_KEY.defaultValue,
      PRECOMBINE_FIELD_OPT_KEY.key -> PRECOMBINE_FIELD_OPT_KEY.defaultValue,
      PAYLOAD_CLASS_OPT_KEY.key -> PAYLOAD_CLASS_OPT_KEY.defaultValue,
      RECORDKEY_FIELD_OPT_KEY.key -> RECORDKEY_FIELD_OPT_KEY.defaultValue,
      PARTITIONPATH_FIELD_OPT_KEY.key -> PARTITIONPATH_FIELD_OPT_KEY.defaultValue,
      KEYGENERATOR_CLASS_OPT_KEY.key -> DEFAULT_KEYGENERATOR_CLASS_OPT_VAL,
      METADATA_ENABLE_PROP.key -> METADATA_ENABLE_PROP.defaultValue.toString,
      METADATA_VALIDATE_PROP.key -> METADATA_VALIDATE_PROP.defaultValue.toString,
      COMMIT_METADATA_KEYPREFIX_OPT_KEY.key -> COMMIT_METADATA_KEYPREFIX_OPT_KEY.defaultValue,
      INSERT_DROP_DUPS_OPT_KEY.key -> INSERT_DROP_DUPS_OPT_KEY.defaultValue,
      STREAMING_RETRY_CNT_OPT_KEY.key -> STREAMING_RETRY_CNT_OPT_KEY.defaultValue,
      STREAMING_RETRY_INTERVAL_MS_OPT_KEY.key -> STREAMING_RETRY_INTERVAL_MS_OPT_KEY.defaultValue,
      STREAMING_IGNORE_FAILED_BATCH_OPT_KEY.key -> STREAMING_IGNORE_FAILED_BATCH_OPT_KEY.defaultValue,
      META_SYNC_CLIENT_TOOL_CLASS.key -> META_SYNC_CLIENT_TOOL_CLASS.defaultValue,
      HIVE_SYNC_ENABLED_OPT_KEY.key -> HIVE_SYNC_ENABLED_OPT_KEY.defaultValue,
      META_SYNC_ENABLED_OPT_KEY.key -> META_SYNC_ENABLED_OPT_KEY.defaultValue,
      HIVE_DATABASE_OPT_KEY.key -> HIVE_DATABASE_OPT_KEY.defaultValue,
      HIVE_TABLE_OPT_KEY.key -> HIVE_TABLE_OPT_KEY.defaultValue,
      HIVE_BASE_FILE_FORMAT_OPT_KEY.key -> HIVE_BASE_FILE_FORMAT_OPT_KEY.defaultValue,
      HIVE_USER_OPT_KEY.key -> HIVE_USER_OPT_KEY.defaultValue,
      HIVE_PASS_OPT_KEY.key -> HIVE_PASS_OPT_KEY.defaultValue,
      HIVE_URL_OPT_KEY.key -> HIVE_URL_OPT_KEY.defaultValue,
      HIVE_PARTITION_FIELDS_OPT_KEY.key -> HIVE_PARTITION_FIELDS_OPT_KEY.defaultValue,
      HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.key -> HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY.defaultValue,
      HIVE_STYLE_PARTITIONING_OPT_KEY.key -> HIVE_STYLE_PARTITIONING_OPT_KEY.defaultValue,
      HIVE_USE_JDBC_OPT_KEY.key -> HIVE_USE_JDBC_OPT_KEY.defaultValue,
      ASYNC_COMPACT_ENABLE_OPT_KEY.key -> ASYNC_COMPACT_ENABLE_OPT_KEY.defaultValue,
      ENABLE_ROW_WRITER_OPT_KEY.key -> ENABLE_ROW_WRITER_OPT_KEY.defaultValue
    ) ++ DataSourceOptionsHelper.translateConfigurations(parameters)
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
    val props = new TypedProperties()
    props.putAll(parameters.asJava)
    val keyGen = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props)
    getPartitionColumns(keyGen)
  }

  def getPartitionColumns(keyGen: KeyGenerator): String = {
    keyGen match {
      // For CustomKeyGenerator and CustomAvroKeyGenerator, the partition path filed format
      // is: "field_name: field_type", we extract the field_name from the partition path field.
      case c: BaseKeyGenerator
        if c.isInstanceOf[CustomKeyGenerator] || c.isInstanceOf[CustomAvroKeyGenerator] =>
          c.getPartitionPathFields.asScala.map(pathField =>
            pathField.split(CustomAvroKeyGenerator.SPLIT_REGEX)
                .headOption.getOrElse(s"Illegal partition path field format: '$pathField' for ${c.getClass.getSimpleName}"))
            .mkString(",")

      case b: BaseKeyGenerator => b.getPartitionPathFields.asScala.mkString(",")
      case _=> null
    }
  }

  def convertMapToHoodieConfig(parameters: Map[String, String]): HoodieConfig = {
    val properties = new Properties()
    properties.putAll(mapAsJavaMap(parameters))
    new HoodieConfig(properties)
  }
}
