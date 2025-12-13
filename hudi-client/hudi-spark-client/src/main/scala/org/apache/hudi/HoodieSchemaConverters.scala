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

import org.apache.hudi.common.schema.HoodieSchema
import org.apache.spark.sql.types.DataType

/**
 * Allows conversion between HoodieSchema and Spark's Catalyst DataType.
 *
 * This trait provides version-agnostic interface for schema conversion,
 * with version-specific implementations handling differences like TimestampNTZType.
 */
trait HoodieSchemaConverters {

  /**
   * Converts HoodieSchema to Spark DataType.
   *
   * @param hoodieSchema HoodieSchema to convert
   * @return Tuple of (DataType, nullable)
   */
  def toSqlType(hoodieSchema: HoodieSchema): (DataType, Boolean)

  /**
   * Converts Spark DataType to HoodieSchema.
   *
   * @param catalystType Spark DataType to convert
   * @param nullable Whether the schema should be nullable
   * @param recordName Name for record types
   * @param nameSpace Namespace for record types
   * @return HoodieSchema corresponding to the Spark DataType
   */
  def toHoodieType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String = ""): HoodieSchema

}