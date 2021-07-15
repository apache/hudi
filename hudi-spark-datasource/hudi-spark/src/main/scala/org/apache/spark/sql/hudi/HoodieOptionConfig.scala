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
import org.apache.hudi.common.model.DefaultHoodieRecordPayload
import org.apache.hudi.common.table.HoodieTableConfig


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


  val SQL_KEY_TABLE_PRIMARY_KEY: HoodieOption[String] = buildConf()
    .withSqlKey("primaryKey")
    .withHoodieKey(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY)
    .withTableConfigKey(HoodieTableConfig.HOODIE_TABLE_RECORDKEY_FIELDS)
    .build()

  val SQL_KEY_TABLE_TYPE: HoodieOption[String] = buildConf()
    .withSqlKey("type")
    .withHoodieKey(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY)
    .withTableConfigKey(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME)
    .defaultValue(SQL_VALUE_TABLE_TYPE_COW)
    .build()

  val SQL_KEY_PRECOMBINE_FIELD: HoodieOption[String] = buildConf()
    .withSqlKey("preCombineField")
    .withHoodieKey(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY)
    .withTableConfigKey(HoodieTableConfig.HOODIE_TABLE_PRECOMBINE_FIELD)
    .build()

  val SQL_PAYLOAD_CLASS: HoodieOption[String] = buildConf()
    .withSqlKey("payloadClass")
    .withHoodieKey(DataSourceWriteOptions.PAYLOAD_CLASS_OPT_KEY)
    .withTableConfigKey(HoodieTableConfig.HOODIE_PAYLOAD_CLASS_PROP_NAME)
    .defaultValue(classOf[DefaultHoodieRecordPayload].getName)
    .build()

  /**
   * The mapping of the sql short name key to the hoodie's config key.
   */
  private lazy val keyMapping: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
        .filter(f => f.getType == classOf[HoodieOption[_]])
        .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieOption[_]]})
        .map(option => option.sqlKeyName -> option.hoodieKeyName)
        .toMap
  }

  /**
   * The mapping of the sql short name key to the hoodie table config key
   * defined in HoodieTableConfig.
   */
  private lazy val keyTableConfigMapping: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
      .filter(f => f.getType == classOf[HoodieOption[_]])
      .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieOption[_]]})
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
   * .properites when create the table.
   * @param options
   * @return
   */
  def mappingSqlOptionToTableConfig(options: Map[String, String]): Map[String, String] = {
    defaultTableConfig ++
      options.filterKeys(k => keyTableConfigMapping.contains(k))
        .map(kv => keyTableConfigMapping(kv._1) -> valueMapping.getOrElse(kv._2, kv._2))
  }

  /**
   * Mapping the table config (loaded from the hoodie.properties) to the sql options.
   * @param options
   * @return
   */
  def mappingTableConfigToSqlOption(options: Map[String, String]): Map[String, String] = {
    options.filterKeys(k => tableConfigKeyToSqlKey.contains(k))
      .map(kv => tableConfigKeyToSqlKey(kv._1) -> reverseValueMapping.getOrElse(kv._2, kv._2))
  }

  private lazy val defaultTableConfig: Map[String, String] = {
    HoodieOptionConfig.getClass.getDeclaredFields
      .filter(f => f.getType == classOf[HoodieOption[_]])
      .map(f => {f.setAccessible(true); f.get(HoodieOptionConfig).asInstanceOf[HoodieOption[_]]})
      .filter(option => option.tableConfigKey.isDefined && option.defaultValue.isDefined)
      .map(option => option.tableConfigKey.get ->
        valueMapping.getOrElse(option.defaultValue.get.toString, option.defaultValue.get.toString))
      .toMap
  }

  /**
   * Get the primary key from the table options.
   * @param options
   * @return
   */
  def getPrimaryColumns(options: Map[String, String]): Array[String] = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.get(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY)
      .map(_.split(",").filter(_.length > 0))
      .getOrElse(Array.empty)
  }

  /**
   * Get the table type from the table options.
   * @param options
   * @return
   */
  def getTableType(options: Map[String, String]): String = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.getOrElse(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY,
      DataSourceWriteOptions.DEFAULT_TABLE_TYPE_OPT_VAL)
  }

  def getPreCombineField(options: Map[String, String]): Option[String] = {
    val params = mappingSqlOptionToHoodieParam(options)
    params.get(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY)
  }

  def buildConf[T](): HoodieOptions[T] = {
    new HoodieOptions[T]
  }
}

case class HoodieOption[T](sqlKeyName: String, hoodieKeyName: String,
                           defaultValue: Option[T], tableConfigKey: Option[String] = None)

class HoodieOptions[T] {

  private var sqlKeyName: String = _
  private var hoodieKeyName: String =_
  private var tableConfigKey: String =_
  private var defaultValue: T =_

  def withSqlKey(sqlKeyName: String): HoodieOptions[T] = {
    this.sqlKeyName = sqlKeyName
    this
  }

  def withHoodieKey(hoodieKeyName: String): HoodieOptions[T] = {
    this.hoodieKeyName = hoodieKeyName
    this
  }

  def withTableConfigKey(tableConfigKey: String): HoodieOptions[T] = {
    this.tableConfigKey = tableConfigKey
    this
  }

  def defaultValue(defaultValue: T): HoodieOptions[T] = {
    this.defaultValue = defaultValue
    this
  }

  def build(): HoodieOption[T] = {
    HoodieOption(sqlKeyName, hoodieKeyName, Option(defaultValue), Option(tableConfigKey))
  }
}
