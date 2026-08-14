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

package org.apache.spark.sql.hudi.blob

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * Marker expression for lazy blob data reading.
 *
 * This expression is detected by [[ReadBlobRule]] and transformed to use
 * batched I/O for efficient blob reading during physical execution.
 *
 * Example: `SELECT id, read_blob(image_data) FROM table`
 *
 * @param child Expression representing the blob column (matching the definition in {@link org.apache.hudi.common.schema.HoodieSchema.Blob})
 */
case class ReadBlobExpression(child: Expression)
    extends UnaryExpression
    with Unevaluable {

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): ReadBlobExpression = {
    copy(child = newChild)
  }

  override def toString: String = s"read_blob($child)"
}
