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

import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.internal.schema.InternalSchema

/**
 * Util methods for Schema evolution in Hudi
 */
object HoodieSchemaUtils {
  /**
   * get latest internalSchema from table
   *
   * @param config          instance of {@link HoodieConfig}
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Pair of(boolean, table schema), where first entry will be true only if schema conversion is required.
   */
  def getLatestTableInternalSchema(config: HoodieConfig,
                                   tableMetaClient: HoodieTableMetaClient): Option[InternalSchema] = {
    if (!config.getBooleanOrDefault(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
      Option.empty[InternalSchema]
    } else {
      try {
        val tableSchemaResolver = new TableSchemaResolver(tableMetaClient)
        val internalSchemaOpt = tableSchemaResolver.getTableInternalSchemaFromCommitMetadata
        if (internalSchemaOpt.isPresent) Some(internalSchemaOpt.get()) else None
      } catch {
        case _: Exception => None
      }
    }
  }
}