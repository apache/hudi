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

import org.apache.hudi.DataSourceWriteOptions.INSERT_DROP_DUPS
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.MetaFieldsMode
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.{ConfigUtils, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieKeyGeneratorException
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

import org.slf4j.LoggerFactory

object AutoRecordKeyGenerationUtils {
  private val log = LoggerFactory.getLogger(getClass)

  def mayBeValidateParamsForAutoGenerationOfRecordKeys(parameters: Map[String, String], hoodieConfig: HoodieConfig): Unit = {
    if (shouldAutoGenerateRecordKeys(parameters)) {
      // de-dup is not supported with auto generation of record keys
      if (parameters.getOrElse(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key(),
        HoodieWriteConfig.COMBINE_BEFORE_INSERT.defaultValue()).toBoolean) {
        throw new HoodieKeyGeneratorException("Enabling " + HoodieWriteConfig.COMBINE_BEFORE_INSERT.key() + " is not supported with auto generation of record keys ")
      }
      // drop dupes is not supported
      if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
        throw new HoodieKeyGeneratorException("Enabling " + INSERT_DROP_DUPS.key() + " is not supported with auto generation of record keys ")
      }
      // virtual keys are not supported with auto generation of record keys. Resolve the mode rather
      // than the deprecated boolean alone — a selective mode also leaves _hoodie_record_key
      // unpopulated, so auto-generated keys would be computed and then discarded.
      val metaFieldsMode = MetaFieldsMode.resolve(
        parameters.getOrElse(HoodieTableConfig.META_FIELDS_MODE.key(), null),
        parameters.getOrElse(HoodieTableConfig.POPULATE_META_FIELDS.key(),
          HoodieTableConfig.POPULATE_META_FIELDS.defaultValue().toString).toBoolean)
      if (!metaFieldsMode.isRecordKeyPopulated) {
        // Name whichever property the user actually set, so the error points at the config to change.
        val offendingKey =
          if (parameters.contains(HoodieTableConfig.META_FIELDS_MODE.key())) HoodieTableConfig.META_FIELDS_MODE.key()
          else HoodieTableConfig.POPULATE_META_FIELDS.key()
        throw new HoodieKeyGeneratorException(offendingKey + " is not supported with auto generation of record keys"
          + " (resolved meta fields mode " + metaFieldsMode + " does not populate _hoodie_record_key)")
      }
      val orderingFieldsStr = ConfigUtils.getOrderingFieldsStrDuringWrite(hoodieConfig.getProps)
      if (StringUtils.nonEmpty(orderingFieldsStr)) {
        log.warn("Ordering field " + orderingFieldsStr + " will be ignored with auto record key generation enabled")
      }
    }
  }

  def shouldAutoGenerateRecordKeys(parameters: Map[String, String]): Boolean = {
    val recordKeyFromTableConfig = parameters.getOrElse(HoodieTableConfig.RECORDKEY_FIELDS.key(), "")
    val recordKeyFromWriterConfig = parameters.getOrElse(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "")
    recordKeyFromTableConfig.isEmpty && recordKeyFromWriterConfig.isEmpty
  }
}
