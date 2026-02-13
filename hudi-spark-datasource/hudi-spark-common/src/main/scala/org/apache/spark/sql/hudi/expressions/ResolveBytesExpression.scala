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

package org.apache.spark.sql.hudi.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BinaryType, DataType}

/**
 * Marker expression for blob reference resolution.
 *
 * This expression is NOT evaluated directly. Instead, it's detected by the
 * [[ResolveBlobReferencesRule]] which transforms the entire query plan to use
 * [[BatchedByteRangeReader]] for efficient batched I/O.
 *
 * The expression serves as a marker during the analysis phase, signaling that
 * the child expression (which should be a struct containing file_path, offset, length)
 * needs to be resolved to binary data.
 *
 * <h3>Usage in SQL:</h3>
 * {{{
 * SELECT id, name, resolve_bytes(file_info) as data FROM table
 * }}}
 *
 * <h3>Design Pattern:</h3>
 * This follows the marker expression pattern where:
 * <ul>
 *   <li>Expression creation happens during parsing/initial analysis</li>
 *   <li>A logical plan rule detects these markers post-analysis</li>
 *   <li>The rule applies efficient transformations (batched I/O)</li>
 *   <li>Markers are replaced with actual data references</li>
 * </ul>
 *
 * @param child Expression representing the blob reference column (struct with file_path, offset, length)
 */
case class ResolveBytesExpression(child: Expression)
    extends UnaryExpression
    with Unevaluable {

  override def dataType: DataType = BinaryType

  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): ResolveBytesExpression = {
    copy(child = newChild)
  }

  override def toString: String = s"resolve_bytes($child)"
}
