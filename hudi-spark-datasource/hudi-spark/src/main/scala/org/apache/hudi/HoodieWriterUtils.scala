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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.TypedProperties

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE
import org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_VALIDATE
import org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_ENABLE_PROP
import org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_VALIDATE_PROP

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
    Map(OPERATION_OPT_KEY -> DEFAULT_OPERATION_OPT_VAL,
      TABLE_TYPE_OPT_KEY -> DEFAULT_TABLE_TYPE_OPT_VAL,
      PRECOMBINE_FIELD_OPT_KEY -> DEFAULT_PRECOMBINE_FIELD_OPT_VAL,
      PAYLOAD_CLASS_OPT_KEY -> DEFAULT_PAYLOAD_OPT_VAL,
      RECORDKEY_FIELD_OPT_KEY -> DEFAULT_RECORDKEY_FIELD_OPT_VAL,
      PARTITIONPATH_FIELD_OPT_KEY -> DEFAULT_PARTITIONPATH_FIELD_OPT_VAL,
      KEYGENERATOR_CLASS_OPT_KEY -> DEFAULT_KEYGENERATOR_CLASS_OPT_VAL,
      METADATA_ENABLE_PROP -> DEFAULT_METADATA_ENABLE.toString,
      METADATA_VALIDATE_PROP -> DEFAULT_METADATA_VALIDATE.toString,
      COMMIT_METADATA_KEYPREFIX_OPT_KEY -> DEFAULT_COMMIT_METADATA_KEYPREFIX_OPT_VAL,
      INSERT_DROP_DUPS_OPT_KEY -> DEFAULT_INSERT_DROP_DUPS_OPT_VAL,
      STREAMING_RETRY_CNT_OPT_KEY -> DEFAULT_STREAMING_RETRY_CNT_OPT_VAL,
      STREAMING_RETRY_INTERVAL_MS_OPT_KEY -> DEFAULT_STREAMING_RETRY_INTERVAL_MS_OPT_VAL,
      STREAMING_IGNORE_FAILED_BATCH_OPT_KEY -> DEFAULT_STREAMING_IGNORE_FAILED_BATCH_OPT_VAL,
      META_SYNC_CLIENT_TOOL_CLASS -> DEFAULT_META_SYNC_CLIENT_TOOL_CLASS,
      HIVE_SYNC_ENABLED_OPT_KEY -> DEFAULT_HIVE_SYNC_ENABLED_OPT_VAL,
      META_SYNC_ENABLED_OPT_KEY -> DEFAULT_META_SYNC_ENABLED_OPT_VAL,
      HIVE_DATABASE_OPT_KEY -> DEFAULT_HIVE_DATABASE_OPT_VAL,
      HIVE_TABLE_OPT_KEY -> DEFAULT_HIVE_TABLE_OPT_VAL,
      HIVE_BASE_FILE_FORMAT_OPT_KEY -> DEFAULT_HIVE_BASE_FILE_FORMAT_OPT_VAL,
      HIVE_USER_OPT_KEY -> DEFAULT_HIVE_USER_OPT_VAL,
      HIVE_PASS_OPT_KEY -> DEFAULT_HIVE_PASS_OPT_VAL,
      HIVE_URL_OPT_KEY -> DEFAULT_HIVE_URL_OPT_VAL,
      HIVE_PARTITION_FIELDS_OPT_KEY -> DEFAULT_HIVE_PARTITION_FIELDS_OPT_VAL,
      HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL,
      HIVE_STYLE_PARTITIONING_OPT_KEY -> DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL,
      HIVE_USE_JDBC_OPT_KEY -> DEFAULT_HIVE_USE_JDBC_OPT_VAL,
      ASYNC_COMPACT_ENABLE_OPT_KEY -> DEFAULT_ASYNC_COMPACT_ENABLE_OPT_VAL,
      ENABLE_ROW_WRITER_OPT_KEY -> DEFAULT_ENABLE_ROW_WRITER_OPT_VAL
    ) ++ translateStorageTypeToTableType(parameters)
  }

  def toProperties(params: Map[String, String]): TypedProperties = {
    val props = new TypedProperties()
    params.foreach(kv => props.setProperty(kv._1, kv._2))
    props
  }
}
