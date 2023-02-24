/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.{HoodiePayloadProps, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.AutoRecordKeyGenExpression

import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object RecordKeyAutoGen {

  /**
   * Set of operations supporting record-key auto-gen (currently only [[WriteOperationType.INSERT]],
   * [[WriteOperationType.BULK_INSERT]])
   */
  private val supportedOperations: Set[String] =
    Set(WriteOperationType.INSERT, WriteOperationType.BULK_INSERT).map(_.value)

  /**
   * Set of operations compatible w/ record-key auto-gen (additionally to [[supportedOperations]]
   * [[WriteOperationType.DELETE]] is a compatible operation)
   */
  private val compatibleOperations: Set[String] = supportedOperations ++
    Set(WriteOperationType.DELETE).map(_.value)

  def tryRecordKeyAutoGen(df: DataFrame, commitInstant: String, config: HoodieConfig): DataFrame = {
    val shouldAutoGenRecordKeys = config.getBooleanOrDefault(HoodieTableConfig.AUTO_GEN_RECORD_KEYS)
    val operation = config.getStringOrDefault(DataSourceWriteOptions.OPERATION)

    if (shouldAutoGenRecordKeys && supportedOperations.contains(operation)) {
      // TODO reorder to keep all meta-fields as first?
      df.withColumn(HoodieRecord.AUTOGEN_ROW_KEY, new Column(AutoRecordKeyGenExpression(commitInstant)))
    } else {
      df
    }
  }

  def handleAutoGenRecordKeysConfig(mergedParams: mutable.Map[String, String]): Unit = {
    val shouldAutoGenRecordKeys = mergedParams.getOrElse(HoodieTableConfig.AUTO_GEN_RECORD_KEYS.key,
      HoodieTableConfig.AUTO_GEN_RECORD_KEYS.defaultValue.toString).toBoolean
    if (shouldAutoGenRecordKeys) {
      val operation = mergedParams.getOrElse(DataSourceWriteOptions.OPERATION.key,
        DataSourceWriteOptions.OPERATION.defaultValue)
      if (!compatibleOperations.contains(operation)) {
        throw new HoodieException(s"Operation '$operation' is not compatible with record key auto-generation")
      }

      // In case when keys will be auto-generated we have to override following configuration
      //    - Record key-field name (to the auto-gen'd one)
      mergedParams ++= Map(
        KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key -> HoodieRecord.AUTOGEN_ROW_KEY
      )

      // In case when keys will be auto-generated we override following configuration
      //    - Pre-combine field (since updating is not compatible w/ auto-gen anyway)
      //    - Payload ordering field
      mergedParams --= Seq(
        HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key,
        HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY
      )
    }
  }

  def handleAutoGenRecordKeysConfigJava(hoodieConfig: HoodieConfig): Unit = {
    val props = hoodieConfig.getProps
    handleAutoGenRecordKeysConfig(props.asScala.asInstanceOf[mutable.Map[String, String]])
    new HoodieConfig(props)
  }

}
