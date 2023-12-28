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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType

/**
 * Utils on schema, which have different implementation across Spark versions.
 */
trait HoodieSchemaUtils {
  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames           column names to check.
   * @param colType               column type name, used in an exception message.
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not.
   */
  def checkColumnNameDuplication(columnNames: Seq[String],
                                 colType: String,
                                 caseSensitiveAnalysis: Boolean): Unit

  /**
   * SPARK-44353 StructType#toAttributes was removed in Spark 3.5.0
   * Use DataTypeUtils#toAttributes for Spark 3.5+
   */
  def toAttributes(struct: StructType): Seq[Attribute]
}
