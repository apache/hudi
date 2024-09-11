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

import org.apache.hudi.AutoRecordKeyGenerationUtils.shouldAutoGenerateRecordKeys
import org.apache.hudi.DataSourceOptionsHelper.allAlternatives
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE
import org.apache.hudi.common.config.{DFSPropertiesConfiguration, HoodieCommonConfig, HoodieConfig, TypedProperties}
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hive.HiveSyncConfigHolder
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.keygen.{NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.hudi.util.SparkKeyGenUtils

import org.apache.spark.sql.hudi.command.{MergeIntoKeyGenerator, SqlKeyGenerator}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * WriterUtils to assist in write path in Datasource and tests.
 */
object HoodieWriterUtils {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Add default options for unspecified write options keys.
   */
  def parametersWithWriteDefaults(parameters: Map[String, String]): Map[String, String] = {
    val globalProps = DFSPropertiesConfiguration.getGlobalProps.asScala
    val props = TypedProperties.fromMap(parameters.asJava)
    val hoodieConfig: HoodieConfig = new HoodieConfig(props)
    hoodieConfig.setDefaultValue(OPERATION)
    hoodieConfig.setDefaultValue(TABLE_TYPE)
    hoodieConfig.setDefaultValue(PRECOMBINE_FIELD)
    hoodieConfig.setDefaultValue(PAYLOAD_CLASS_NAME)
    hoodieConfig.setDefaultValue(KEYGENERATOR_CLASS_NAME)
    hoodieConfig.setDefaultValue(ENABLE)
    hoodieConfig.setDefaultValue(COMMIT_METADATA_KEYPREFIX)
    hoodieConfig.setDefaultValue(INSERT_DROP_DUPS)
    hoodieConfig.setDefaultValue(STREAMING_RETRY_CNT)
    hoodieConfig.setDefaultValue(STREAMING_RETRY_INTERVAL_MS)
    hoodieConfig.setDefaultValue(STREAMING_IGNORE_FAILED_BATCH)
    hoodieConfig.setDefaultValue(META_SYNC_CLIENT_TOOL_CLASS_NAME)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_SYNC_ENABLED)
    hoodieConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_ENABLED)
    hoodieConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_DATABASE_NAME)
    hoodieConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_TABLE_NAME)
    hoodieConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_USER)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_PASS)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_URL)
    hoodieConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS)
    hoodieConfig.setDefaultValue(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS)
    hoodieConfig.setDefaultValue(HIVE_STYLE_PARTITIONING)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_USE_JDBC)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE)
    hoodieConfig.setDefaultValue(HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE)
    hoodieConfig.setDefaultValue(ASYNC_COMPACT_ENABLE)
    hoodieConfig.setDefaultValue(INLINE_CLUSTERING_ENABLE)
    hoodieConfig.setDefaultValue(ASYNC_CLUSTERING_ENABLE)
    hoodieConfig.setDefaultValue(ENABLE_ROW_WRITER)
    hoodieConfig.setDefaultValue(RECONCILE_SCHEMA)
    hoodieConfig.setDefaultValue(DROP_PARTITION_COLUMNS)
    hoodieConfig.setDefaultValue(KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED)
    Map() ++ hoodieConfig.getProps.asScala ++ globalProps ++ DataSourceOptionsHelper.translateConfigurations(parameters)
  }

  /**
   * Determines whether writes need to take prepped path or regular non-prepped path.
   * - For spark-sql writes (UPDATES, DELETES), we could use prepped flow due to the presences of meta fields.
   * - For pkless tables, if incoming df has meta fields, we could use prepped flow.
   *
   * @param hoodieConfig hoodie config of interest.
   * @param parameters   raw parameters.
   * @param operation    operation type.
   * @param df           incoming dataframe
   * @return true if prepped writes, false otherwise.
   */
  def canDoPreppedWrites(hoodieConfig: HoodieConfig, parameters: Map[String, String], operation: WriteOperationType, df: Dataset[Row]): Boolean = {
    var isPrepped = false
    if (shouldAutoGenerateRecordKeys(parameters)
      && parameters.getOrElse(SPARK_SQL_WRITES_PREPPED_KEY, "false").equals("false")
      && parameters.getOrElse(SPARK_SQL_MERGE_INTO_PREPPED_KEY, "false").equals("false")
      && df.schema.fieldNames.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
      // with pk less table, writes using spark-ds writer can potentially use the prepped path if meta fields are present in the incoming df.
      if (operation == WriteOperationType.UPSERT) {
        log.warn("Changing operation type to UPSERT PREPPED for pk less table upserts ")
        isPrepped = true
      } else if (operation == WriteOperationType.DELETE) {
        log.warn("Changing operation type to DELETE PREPPED for pk less table deletes ")
        isPrepped = true
      }
    }
    isPrepped
  }

  /**
   * Fetch params by translating alternatives if any. Do not set any default as this method is intended to be called
   * before validation.
   *
   * @param parameters hash map of parameters.
   * @return hash map of raw with translated parameters.
   */
  def getParamsWithAlternatives(parameters: Map[String, String]): Map[String, String] = {
    val globalProps = DFSPropertiesConfiguration.getGlobalProps.asScala
    val props = TypedProperties.fromMap(parameters.asJava)
    val hoodieConfig: HoodieConfig = new HoodieConfig(props)
    // do not set any default as this is called before validation.
    Map() ++ hoodieConfig.getProps.asScala ++ globalProps ++ DataSourceOptionsHelper.translateConfigurations(parameters)
  }

  /**
   * Get the partition columns to stored to hoodie.properties.
   */
  def getPartitionColumns(parameters: Map[String, String]): String = {
    SparkKeyGenUtils.getPartitionColumns(TypedProperties.fromMap(parameters.asJava))
  }

  def convertMapToHoodieConfig(parameters: Map[String, String]): HoodieConfig = {
    val properties = TypedProperties.fromMap(parameters.asJava)
    new HoodieConfig(properties)
  }

  def getOriginKeyGenerator(parameters: Map[String, String]): String = {
    val kg = parameters.getOrElse(KEYGENERATOR_CLASS_NAME.key(), null)
    if (classOf[SqlKeyGenerator].getCanonicalName == kg || classOf[MergeIntoKeyGenerator].getCanonicalName == kg) {
      parameters.getOrElse(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, null)
    } else {
      kg
    }
  }

  def validateTableConfig(spark: SparkSession, params: Map[String, String],
                          tableConfig: HoodieConfig): Unit = {
    validateTableConfig(spark, params, tableConfig, false)
  }

  /**
   * Detects conflicts between new parameters and existing table configurations
   */
  def validateTableConfig(spark: SparkSession, params: Map[String, String],
                          tableConfig: HoodieConfig, isOverWriteMode: Boolean): Unit = {
    // If Overwrite is set as save mode, we don't need to do table config validation.
    if (!isOverWriteMode) {
      val resolver = spark.sessionState.conf.resolver
      val diffConfigs = StringBuilder.newBuilder
      params.foreach { case (key, value) =>
        // Base file format can change between writes, so ignore it.
        if (!HoodieTableConfig.BASE_FILE_FORMAT.key.equals(key)) {
          val existingValue = getStringFromTableConfigWithAlternatives(tableConfig, key)
          if (null != existingValue && !resolver(existingValue, value)) {
            diffConfigs.append(s"$key:\t$value\t${tableConfig.getString(key)}\n")
          }
        }
      }

      if (null != tableConfig) {
        val datasourceRecordKey = params.getOrElse(RECORDKEY_FIELD.key(), null)
        val tableConfigRecordKey = tableConfig.getString(HoodieTableConfig.RECORDKEY_FIELDS)
        if ((null != datasourceRecordKey && null != tableConfigRecordKey
          && datasourceRecordKey != tableConfigRecordKey) || (null != datasourceRecordKey && datasourceRecordKey.nonEmpty
          && tableConfigRecordKey == null)) {
          // if both are non null, they should match.
          // if incoming record key is non empty, table config should also be non empty.
          diffConfigs.append(s"RecordKey:\t$datasourceRecordKey\t$tableConfigRecordKey\n")
        }

        val datasourcePreCombineKey = params.getOrElse(PRECOMBINE_FIELD.key(), null)
        val tableConfigPreCombineKey = tableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD)
        if (null != datasourcePreCombineKey && null != tableConfigPreCombineKey && datasourcePreCombineKey != tableConfigPreCombineKey) {
          diffConfigs.append(s"PreCombineKey:\t$datasourcePreCombineKey\t$tableConfigPreCombineKey\n")
        }

        val datasourceKeyGen = getOriginKeyGenerator(params)
        val tableConfigKeyGen = KeyGeneratorType.getKeyGeneratorClassName(tableConfig)
        if (null != datasourceKeyGen && null != tableConfigKeyGen
          && datasourceKeyGen != tableConfigKeyGen) {
          diffConfigs.append(s"KeyGenerator:\t$datasourceKeyGen\t$tableConfigKeyGen\n")
        }

        // Please note that the validation of partition path fields needs the key generator class
        // for the table, since the custom key generator expects a different format of
        // the value of the write config "hoodie.datasource.write.partitionpath.field"
        // e.g., "col:simple,ts:timestamp", whereas the table config "hoodie.table.partition.fields"
        // in hoodie.properties stores "col,ts".
        // The "params" here may only contain the write config of partition path field,
        // so we need to pass in the validated key generator class name.
        val validatedKeyGenClassName = if (tableConfigKeyGen != null) {
          Option(tableConfigKeyGen)
        } else if (datasourceKeyGen != null) {
          Option(datasourceKeyGen)
        } else {
          None
        }
        val datasourcePartitionFields = params.getOrElse(PARTITIONPATH_FIELD.key(), null)
        val currentPartitionFields = if (datasourcePartitionFields == null) {
          null
        } else {
          SparkKeyGenUtils.getPartitionColumns(validatedKeyGenClassName, TypedProperties.fromMap(params.asJava))
        }
        val tableConfigPartitionFields = HoodieTableConfig.getPartitionFieldProp(tableConfig).orElse(null)
        if (null != datasourcePartitionFields && null != tableConfigPartitionFields
          && currentPartitionFields != tableConfigPartitionFields) {
          diffConfigs.append(s"PartitionPath:\t$currentPartitionFields\t$tableConfigPartitionFields\n")
        }
      }

      if (diffConfigs.nonEmpty) {
        diffConfigs.insert(0, "\nConfig conflict(key\tcurrent value\texisting value):\n")
        throw new HoodieException(diffConfigs.toString.trim)
      }
    }

    // Check schema evolution for bootstrap table.
    // now we do not support bootstrap table.
    if (params.get(OPERATION.key).contains(BOOTSTRAP_OPERATION_OPT_VAL)
      && params.getOrElse(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "false").toBoolean) {
      throw new HoodieException(String
        .format("now schema evolution cannot support bootstrap table, pls set %s to false", HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key()))
    }
  }

  /**
   * Detects conflicts between datasourceKeyGen and existing table configuration keyGen
   */
  def validateKeyGeneratorConfig(datasourceKeyGen: String, tableConfig: HoodieConfig): Unit = {
    val diffConfigs = StringBuilder.newBuilder

    if (null != tableConfig) {
      val tableConfigKeyGen = KeyGeneratorType.getKeyGeneratorClassName(tableConfig)
      if (null != tableConfigKeyGen && null != datasourceKeyGen) {
        val nonPartitionedTableConfig = tableConfigKeyGen.equals(classOf[NonpartitionedKeyGenerator].getCanonicalName)
        val simpleKeyDataSourceConfig = datasourceKeyGen.equals(classOf[SimpleKeyGenerator].getCanonicalName)
        if (nonPartitionedTableConfig && simpleKeyDataSourceConfig) {
          diffConfigs.append(s"KeyGenerator:\t$datasourceKeyGen\t$tableConfigKeyGen\n")
        }
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

  private val sparkDatasourceConfigsToTableConfigsMap = Map(
    TABLE_NAME -> HoodieTableConfig.NAME,
    TABLE_TYPE -> HoodieTableConfig.TYPE,
    PRECOMBINE_FIELD -> HoodieTableConfig.PRECOMBINE_FIELD,
    PARTITIONPATH_FIELD -> HoodieTableConfig.PARTITION_FIELDS,
    RECORDKEY_FIELD -> HoodieTableConfig.RECORDKEY_FIELDS,
    PAYLOAD_CLASS_NAME -> HoodieTableConfig.PAYLOAD_CLASS_NAME,
    PAYLOAD_TYPE -> HoodieTableConfig.PAYLOAD_TYPE,
    RECORD_MERGER_STRATEGY -> HoodieTableConfig.RECORD_MERGER_STRATEGY
  )

  def mappingSparkDatasourceConfigsToTableConfigs(options: Map[String, String]): Map[String, String] = {
    val includingTableConfigs = scala.collection.mutable.Map() ++ options
    sparkDatasourceConfigsToTableConfigsMap.foreach(kv => {
      if (options.contains(kv._1.key)) {
        includingTableConfigs(kv._2.key) = options(kv._1.key)
        includingTableConfigs.remove(kv._1.key)
      }
    })
    includingTableConfigs.toMap
  }
}
