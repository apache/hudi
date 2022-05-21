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
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.ValidationUtils
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
    .defaultValue(DataSourceWriteOptions.RECORDKEY_FIELD.defaultValue())
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
    .withTableConfigKey(HoodieTableConfig.PRECOMBINE_FIELD.key)
    .build()

  val SQL_PAYLOAD_CLASS: HoodieSQLOption[String] = buildConf()
    .withSqlKey("payloadClass")
    .withHoodieKey(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key)
    .withTableConfigKey(HoodieTableConfig.PAYLOAD_CLASS_NAME.key)
    .defaultValue(classOf[DefaultHoodieRecordPayload].getName)
    .build()

  /**
   * The mapping of the sql short name key to the hoodie's config key.
   */
  private lazy val keyMapping: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
        .filter(f => f.getType == classOf[HoodieSQLOption[_]])
        .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieSQLOption[_]]})
        .map(option => option.sqlKeyName -> option.hoodieKeyName)
        .toMap
  }

  /**
   * The mapping of the sql short name key to the hoodie table config key
   * defined in HoodieTableConfig.
   */
  private lazy val keyTableConfigMapping: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
      .filter(f => f.getType == classOf[HoodieSQLOption[_]])
      .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieSQLOption[_]]})
      .filter(_.tableConfigKey.isDefined)
      .map(option => option.sqlKeyName -> option.tableConfigKey.get)
      .toMap
  }

  private lazy val tableConfigKeyToSqlKey: Map[String, String] =
    keyTableConfigMapping.map(f => f._2 -> f._1)

  /**
   * Mapping of the short sql value to the hoodie's config value
   */
  private val valueMapping: Map[String, String] = Map (
    SQL_VALUE_TABLE_TYPE_COW -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    SQL_VALUE_TABLE_TYPE_MOR -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL
  )

  private lazy val reverseValueMapping = valueMapping.map(f => f._2 -> f._1)

  def withDefaultSqlOptions(options: Map[String, String]): Map[String, String] = defaultSqlOptions ++ options

  /**
   * Mapping the sql's short name key/value in the options to the hoodie's config key/value.
   * @param options
   * @return
   */
  def mappingSqlOptionToHoodieParam(options: Map[String, String]): Map[String, String] = {
    options.map (kv =>
      keyMapping.getOrElse(kv._1, kv._1) -> valueMapping.getOrElse(kv._2, kv._2))
  }

  /**
   * Mapping the sql options to the hoodie table config which used to store to the hoodie
   * .properties when create the table.
   * @param options
   * @return
   */
  def mappingSqlOptionToTableConfig(options: Map[String, String]): Map[String, String] = {
    options.map { case (k, v) =>
      if (keyTableConfigMapping.contains(k)) {
        keyTableConfigMapping(k) -> valueMapping.getOrElse(v, v)
      } else {
        k -> v
      }
    }
  }

  /**
   * Mapping the table config (loaded from the hoodie.properties) to the sql options.
   */
  def mappingTableConfigToSqlOption(options: Map[String, String]): Map[String, String] = {
    options.map(kv => tableConfigKeyToSqlKey.getOrElse(kv._1, kv._1) -> reverseValueMapping.getOrElse(kv._2, kv._2))
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
   * Get the primary key from the table options.
   * @param options
   * @return
   */
  def getPrimaryColumns(options: Map[String, String]): Array[String] = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.get(DataSourceWriteOptions.RECORDKEY_FIELD.key)
      .map(_.split(",").filter(_.nonEmpty))
      .getOrElse(Array.empty)
  }

  /**
   * Get the table type from the table options.
   * @param options
   * @return
   */
  def getTableType(options: Map[String, String]): String = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.getOrElse(DataSourceWriteOptions.TABLE_TYPE.key,
      DataSourceWriteOptions.TABLE_TYPE.defaultValue)
  }

  def getPreCombineField(options: Map[String, String]): Option[String] = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key).filter(_.nonEmpty)
  }

  def deleteHoodieOptions(options: Map[String, String]): Map[String, String] = {
    options.filterNot(_._1.startsWith("hoodie.")).filterNot(kv => keyMapping.contains(kv._1))
  }

  // extract primaryKey, preCombineField, type options
  def extractSqlOptions(options: Map[String, String]): Map[String, String] = {
    val sqlOptions = mappingTableConfigToSqlOption(options)
    val targetOptions = keyMapping.keySet -- Set(SQL_PAYLOAD_CLASS.sqlKeyName)
    sqlOptions.filterKeys(targetOptions.contains)
  }

  // validate primaryKey, preCombineField and type options
  def validateTable(spark: SparkSession, schema: StructType, sqlOptions: Map[String, String]): Unit = {
    val resolver = spark.sessionState.conf.resolver

    // validate primary key
    val primaryKeys = sqlOptions.get(SQL_KEY_TABLE_PRIMARY_KEY.sqlKeyName)
      .map(_.split(",").filter(_.length > 0))
    ValidationUtils.checkArgument(primaryKeys.nonEmpty, "No `primaryKey` is specified.")
    primaryKeys.get.foreach { primaryKey =>
      ValidationUtils.checkArgument(schema.exists(f => resolver(f.name, getRootLevelFieldName(primaryKey))),
        s"Can't find primaryKey `$primaryKey` in ${schema.treeString}.")
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
  }

  def buildConf[T](): HoodieSQLOptionBuilder[T] = {
    new HoodieSQLOptionBuilder[T]
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
