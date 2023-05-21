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
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieKeyGeneratorException
import org.apache.hudi.keygen.constant.KeyGeneratorOptions

object AutoRecordKeyGenerationUtils {

  def mayBeValidateParamsForAutoGenerationOfRecordKeys(parameters: Map[String, String], hoodieConfig: HoodieConfig): Unit = {
    val autoGenerateRecordKeys = !parameters.contains(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()) // if record key is not configured,
    // hudi will auto generate.
    if (autoGenerateRecordKeys) {
      // de-dup is not supported with auto generation of record keys
      if (parameters.getOrElse(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key(),
        HoodieWriteConfig.COMBINE_BEFORE_INSERT.defaultValue()).toBoolean) {
        throw new HoodieKeyGeneratorException("Enabling " + HoodieWriteConfig.COMBINE_BEFORE_INSERT.key() + " is not supported with auto generation of record keys ")
      }
      // drop dupes is not supported
      if (hoodieConfig.getBoolean(INSERT_DROP_DUPS)) {
        throw new HoodieKeyGeneratorException("Enabling " + INSERT_DROP_DUPS.key() + " is not supported with auto generation of record keys ")
      }
      // virtual keys are not supported with auto generation of record keys.
      if (!parameters.getOrElse(HoodieTableConfig.POPULATE_META_FIELDS.key(), HoodieTableConfig.POPULATE_META_FIELDS.defaultValue().toString).toBoolean) {
        throw new HoodieKeyGeneratorException("Disabling " + HoodieTableConfig.POPULATE_META_FIELDS.key() + " is not supported with auto generation of record keys")
      }
    }
  }
}
