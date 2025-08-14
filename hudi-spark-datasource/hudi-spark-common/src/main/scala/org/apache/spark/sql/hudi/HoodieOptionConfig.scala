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

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.avro.HoodieAvroUtils.getRootLevelFieldName
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


/**
 * The HoodieOptionConfig defines some short name for the hoodie
 * option key and value.
 */
object HoodieOptionConfig {

  /**
   * The short name for the value of COW_TABLE_TYPE_OPT_VAL.
   */
  val SQL_VALUE_TABLE_TYPE_COW = "cow"

  /**
   * The short name for the value of MOR_TABLE_TYPE_OPT_VAL.
   */
  val SQL_VALUE_TABLE_TYPE_MOR = "mor"


  val SQL_KEY_TABLE_PRIMARY_KEY: HoodieSQLOption[String] = buildConf()
    .withSqlKey("primaryKey")
    .withHoodieKey(DataSourceWriteOptions.RECORDKEY_FIELD.key)
    .withTableConfigKey(HoodieTableConfig.RECORDKEY_FIELDS.key)
    .build()

  val SQL_KEY_TABLE_TYPE: HoodieSQLOption[String] = buildConf()
    .withSqlKey("type")
    .withHoodieKey(DataSourceWriteOptions.TABLE_TYPE.key)
    .withTableConfigKey(HoodieTableConfig.TYPE.key)
    .defaultValue(SQL_VALUE_TABLE_TYPE_COW)
    .build()

  val SQL_KEY_PRECOMBINE_FIELD: HoodieSQLOption[String] = buildConf()
    .withSqlKey("preCombineField")
    .withHoodieKey(DataSourceWriteOptions.PRECOMBINE_FIELD.key)
    .withTableConfigKey(HoodieTableConfig.ORDERING_FIELDS.key)
    .build()

  val SQL_PAYLOAD_CLASS: HoodieSQLOption[String] = buildConf()
    .withSqlKey("payloadClass")
    .withHoodieKey(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key)
    .withTableConfigKey(HoodieTableConfig.PAYLOAD_CLASS_NAME.key)
    .build()

  val SQL_RECORD_MERGE_MODE: HoodieSQLOption[String] = buildConf()
    .withSqlKey("recordMergeMode")
    .withHoodieKey(HoodieWriteConfig.RECORD_MERGE_MODE.key)
    .withTableConfigKey(HoodieTableConfig.RECORD_MERGE_MODE.key)
    .build()

  val SQL_RECORD_MERGE_STRATEGY_ID: HoodieSQLOption[String] = buildConf()
    .withSqlKey("recordMergeStrategyId")
    .withHoodieKey(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key)
    .withTableConfigKey(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key)
    .build()

  /**
   * The mapping of the sql short name key to the hoodie's config key.
   */
  private lazy val sqlOptionKeyToWriteConfigKey: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
        .filter(f => f.getType == classOf[HoodieSQLOption[_]])
        .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieSQLOption[_]]})
        .map(option => option.sqlKeyName -> option.hoodieKeyName)
        .toMap
  }

  private lazy val writeConfigKeyToSqlOptionKey: Map[String, String] =
    sqlOptionKeyToWriteConfigKey.map(f => f._2 -> f._1)

  /**
   * The mapping of the sql short name key to the hoodie table config key
   * defined in HoodieTableConfig.
   */
  private lazy val sqlOptionKeyToTableConfigKey: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
      .filter(f => f.getType == classOf[HoodieSQLOption[_]])
      .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieSQLOption[_]]})
      .filter(_.tableConfigKey.isDefined)
      .map(option => option.sqlKeyName -> option.tableConfigKey.get)
      .toMap
  }

  private lazy val tableConfigKeyToSqlOptionKey: Map[String, String] =
    sqlOptionKeyToTableConfigKey.map(f => f._2 -> f._1)

  /**
   * Mapping of the short sql value to the hoodie's config value
   */
  private val sqlOptionValueToHoodieConfigValue: Map[String, String] = Map (
    SQL_VALUE_TABLE_TYPE_COW -> HoodieTableType.COPY_ON_WRITE.name,
    SQL_VALUE_TABLE_TYPE_MOR -> HoodieTableType.MERGE_ON_READ.name
  )

  private lazy val hoodieConfigValueToSqlOptionValue = sqlOptionValueToHoodieConfigValue.map(f => f._2 -> f._1)

  def withDefaultSqlOptions(options: Map[String, String]): Map[String, String] = defaultSqlOptions ++ options

  /**
   * Map SQL options to data source write configs.
   */
  def mapSqlOptionsToDataSourceWriteConfigs(options: Map[String, String]): Map[String, String] = {
    options.map (kv =>
      sqlOptionKeyToWriteConfigKey.getOrElse(kv._1, kv._1) -> sqlOptionValueToHoodieConfigValue.getOrElse(kv._2, kv._2))
  }

  /**
   * Mapping the hoodie configs (including data source write configs and hoodie table configs) to SQL options.
   */
  def mapHoodieConfigsToSqlOptions(options: Map[String, String]): Map[String, String] = {
    options.map { case (k, v) =>
      if (writeConfigKeyToSqlOptionKey.contains(k)) {
        writeConfigKeyToSqlOptionKey(k) -> hoodieConfigValueToSqlOptionValue.getOrElse(v, v)
      } else if (tableConfigKeyToSqlOptionKey.contains(k)) {
        tableConfigKeyToSqlOptionKey(k) -> hoodieConfigValueToSqlOptionValue.getOrElse(v, v)
      } else {
        k -> v
      }
    }
  }

  /**
   * Map SQL options to table configs.
   */
  def mapSqlOptionsToTableConfigs(options: Map[String, String]): Map[String, String] = {
    options.map { case (k, v) =>
      if (sqlOptionKeyToTableConfigKey.contains(k)) {
        // support table type incase-sensitive
        sqlOptionKeyToTableConfigKey(k) -> sqlOptionValueToHoodieConfigValue.getOrElse(v.toLowerCase, v)
      } else {
        k -> v
      }
    }
  }

  val defaultSqlOptions: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
      .filter(f => f.getType == classOf[HoodieSQLOption[_]])
      .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieSQLOption[_]]})
      .filter(option => option.tableConfigKey.isDefined && option.defaultValue.isDefined)
      .map(option => option.sqlKeyName -> option.defaultValue.get.toString)
      .toMap
  }

  /**
   * Get the table type from the table options.
   * @param options
   * @return
   */
  def getTableType(options: Map[String, String]): String = {
    val params = mapSqlOptionsToDataSourceWriteConfigs(options)
    params.getOrElse(DataSourceWriteOptions.TABLE_TYPE.key,
      DataSourceWriteOptions.TABLE_TYPE.defaultValue)
  }

  def deleteHoodieOptions(options: Map[String, String]): Map[String, String] = {
    options.filterNot(_._1.startsWith("hoodie.")).filterNot(kv => sqlOptionKeyToWriteConfigKey.contains(kv._1))
  }

  /**
   * The opposite of `deleteHoodieOptions`, this method extract all hoodie related
   * options(start with `hoodie.` and all sql options)
   */
  def extractHoodieOptions(options: Map[String, String]): Map[String, String] = {
    options.filter(_._1.startsWith("hoodie.")) ++ extractSqlOptions(options)
  }

  // extract primaryKey, preCombineField, type options
  def extractSqlOptions(options: Map[String, String]): Map[String, String] = {
    val sqlOptions = mapHoodieConfigsToSqlOptions(options)
    val targetOptions = sqlOptionKeyToWriteConfigKey.keySet -- Set(SQL_PAYLOAD_CLASS.sqlKeyName, SQL_RECORD_MERGE_STRATEGY_ID.sqlKeyName, SQL_RECORD_MERGE_MODE.sqlKeyName)
    sqlOptions.filterKeys(targetOptions.contains).toMap
  }

  // validate primaryKey, preCombineField and type options
  def validateTable(spark: SparkSession, schema: StructType, sqlOptions: Map[String, String]): Unit = {
    val resolver = spark.sessionState.conf.resolver
    // validate primary key
    val primaryKeys = sqlOptions.get(SQL_KEY_TABLE_PRIMARY_KEY.sqlKeyName)
      .map(_.split(",").filter(_.length > 0))
    if (primaryKeys.isDefined) {
      primaryKeys.get.foreach { primaryKey =>
        ValidationUtils.checkArgument(schema.exists(f => resolver(f.name, getRootLevelFieldName(primaryKey))),
          s"Can't find primaryKey `$primaryKey` in ${schema.treeString}.")
      }
    }

    // validate preCombine key
    val preCombineKey = sqlOptions.get(SQL_KEY_PRECOMBINE_FIELD.sqlKeyName)
    if (preCombineKey.isDefined && preCombineKey.get.nonEmpty) {
      ValidationUtils.checkArgument(schema.exists(f => resolver(f.name, getRootLevelFieldName(preCombineKey.get))),
        s"Can't find preCombineKey `${preCombineKey.get}` in ${schema.treeString}.")
    }

    // validate table type
    val tableType = sqlOptions.get(SQL_KEY_TABLE_TYPE.sqlKeyName)
    ValidationUtils.checkArgument(tableType.nonEmpty, "No `type` is specified.")
    ValidationUtils.checkArgument(
      tableType.get.equalsIgnoreCase(SQL_VALUE_TABLE_TYPE_COW) ||
      tableType.get.equalsIgnoreCase(SQL_VALUE_TABLE_TYPE_MOR),
      s"'type' must be '$SQL_VALUE_TABLE_TYPE_COW' or '$SQL_VALUE_TABLE_TYPE_MOR'")

    // validate table index type
    val indexType = sqlOptions.get(HoodieIndexConfig.INDEX_TYPE.key())
    if (!indexType.isEmpty) {
      HoodieIndexConfig.INDEX_TYPE.checkValues(indexType.get)
    }
  }

  def buildConf[T](): HoodieSQLOptionBuilder[T] = {
    new HoodieSQLOptionBuilder[T]
  }

  def makeOptionsCaseInsensitive(sqlOptions: Map[String, String]): Map[String, String] = {
    // Make Keys Case Insensitive
    val standardOptions = Seq(SQL_KEY_TABLE_PRIMARY_KEY, SQL_KEY_PRECOMBINE_FIELD,
      SQL_KEY_TABLE_TYPE, SQL_PAYLOAD_CLASS, SQL_RECORD_MERGE_STRATEGY_ID, SQL_RECORD_MERGE_MODE).map(key => key.sqlKeyName)

    sqlOptions.map(option => {
      standardOptions.find(x => x.toLowerCase().contains(option._1.toLowerCase())) match {
        case Some(standardKey) => (standardKey, option._2)
        case None => (option._1, option._2)
      }
    })
  }
}

case class HoodieSQLOption[T](
    sqlKeyName: String,
    hoodieKeyName: String,
    tableConfigKey: Option[String],
    defaultValue: Option[T]
)

class HoodieSQLOptionBuilder[T] {

  private var sqlKeyName: String = _
  private var hoodieKeyName: String =_
  private var tableConfigKey: String =_
  private var defaultValue: T =_

  def withSqlKey(sqlKeyName: String): HoodieSQLOptionBuilder[T] = {
    this.sqlKeyName = sqlKeyName
    this
  }

  def withHoodieKey(hoodieKeyName: String): HoodieSQLOptionBuilder[T] = {
    this.hoodieKeyName = hoodieKeyName
    this
  }

  def withTableConfigKey(tableConfigKey: String): HoodieSQLOptionBuilder[T] = {
    this.tableConfigKey = tableConfigKey
    this
  }

  def defaultValue(defaultValue: T): HoodieSQLOptionBuilder[T] = {
    this.defaultValue = defaultValue
    this
  }

  def build(): HoodieSQLOption[T] = {
    HoodieSQLOption(sqlKeyName, hoodieKeyName, Option(tableConfigKey), Option(defaultValue))
  }
}
