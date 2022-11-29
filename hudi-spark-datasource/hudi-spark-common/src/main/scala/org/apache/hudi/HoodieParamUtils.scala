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

import org.apache.spark.sql.SparkSession

/**
 * Hoodie parameter utils.
 */
object HoodieParamUtils {

   def isSchemaEvolutionEnabledOnRead(optParams: Map[String, String], sparkSession: SparkSession): Boolean = {
    // NOTE: Schema evolution could be configured both t/h optional parameters vehicle as well as
    //       t/h Spark Session configuration (for ex, for Spark SQL)
    optParams.getOrElse(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean ||
      sparkSession.conf.get(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
        DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean
  }

}
