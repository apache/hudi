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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

/**
 * Registry of scalar functions for Hudi SQL integration.
 *
 * These functions are registered as built-in functions that can be used
 * in SQL queries. They integrate with Spark's function registry and are
 * available in both SQL and DataFrame API contexts.
 *
 * <h3>Function Registration:</h3>
 * Functions are registered via [[SparkAdapter.injectScalarFunctions]] which is
 * called during [[HoodieSparkSessionExtension]] initialization.
 *
 * <h3>Adding New Functions:</h3>
 * To add a new scalar function:
 * <ol>
 *   <li>Create a marker expression class (extends Unevaluable)</li>
 *   <li>Add function definition tuple to [[funcs]] below</li>
 *   <li>Create a logical plan rule to handle the expression</li>
 *   <li>Register the rule in [[HoodieAnalysis.customPostHocResolutionRules]]</li>
 * </ol>
 */
object ScalarFunctions {

  private val READ_BLOB_FUNC_NAME = "read_blob"

  /**
   * Function definitions as tuples of:
   * <ul>
   *   <li>FunctionIdentifier - function name</li>
   *   <li>ExpressionInfo - metadata for DESCRIBE FUNCTION</li>
   *   <li>Builder function - (Seq[Expression] => Expression)</li>
   * </ul>
   */
  val funcs: Seq[(FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)] = Seq(
    (
      FunctionIdentifier(READ_BLOB_FUNC_NAME),
      new ExpressionInfo(
        classOf[ReadBlobExpression].getCanonicalName,
        READ_BLOB_FUNC_NAME,
        """
          |Usage: read_blob(blob_column) - Reads blob data from storage
          |
          |Reads byte ranges from files referenced in a blob column. The column must have
          |metadata hudi_blob=true.
          |
          |This function uses batched I/O operations for optimal performance.
          |For best results, ensure data is sorted by (reference.file, reference.position).
          |
          |Example:
          |  SELECT id, name, read_blob(file_ref) as data FROM table
          |
          |Arguments:
          |  blob_column - Struct column with HoodieSchema.Blob structure:
          |    - type (string): "out_of_line" or "inline"
          |    - data (binary, nullable): inline blob data or null
          |    - reference (struct, nullable): {file, offset, length, managed}
          |
          |Returns:
          |  Binary data read from the file
          |
          |Performance:
          |  - Configure batching: hoodie.blob.batching.max.gap.bytes (default 4096)
          |  - Configure lookahead: hoodie.blob.batching.lookahead.size (default 50)
        """.stripMargin
      ),
      (args: Seq[Expression]) => {
        require(args.length == 1, s"read_blob expects exactly 1 argument, got ${args.length}")
        ReadBlobExpression(args.head)
      }
    )
  )
}
