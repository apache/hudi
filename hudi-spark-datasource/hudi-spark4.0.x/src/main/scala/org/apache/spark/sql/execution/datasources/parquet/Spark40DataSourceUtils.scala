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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.SPARK_VERSION_METADATA_KEY
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.util.Utils

object Spark40DataSourceUtils {

  /**
   * NOTE: This method was copied from [[Spark32PlusDataSourceUtils]], and is required to maintain runtime
   * compatibility against Spark 3.5.0
   */
  // scalastyle:off
  def int96RebaseMode(lookupFileMeta: String => String,
                      modeByConfig: String): LegacyBehaviorPolicy.Value = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return LegacyBehaviorPolicy.CORRECTED
    }
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 3.0 and earlier follow the legacy hybrid calendar and we need to
      // rebase the INT96 timestamp values.
      // Files written by Spark 3.1 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version < "3.1.0" || lookupFileMeta("org.apache.spark.legacyINT96") != null) {
        LegacyBehaviorPolicy.LEGACY
      } else {
        LegacyBehaviorPolicy.CORRECTED
      }
    }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
  }
  // scalastyle:on

  /**
   * NOTE: This method was copied from Spark 3.2.0, and is required to maintain runtime
   * compatibility against Spark 3.2.0
   */
  // scalastyle:off
  def datetimeRebaseMode(lookupFileMeta: String => String,
                         modeByConfig: String): LegacyBehaviorPolicy.Value = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return LegacyBehaviorPolicy.CORRECTED
    }
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by Spark 3.0 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version < "3.0.0" || lookupFileMeta("org.apache.spark.legacyDateTime") != null) {
        LegacyBehaviorPolicy.LEGACY
      } else {
        LegacyBehaviorPolicy.CORRECTED
      }
    }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
  }
  // scalastyle:on

}
