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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * An interface representing a stored procedure available for execution.
 */
trait Procedure {
  /**
   * Returns the input parameters of this procedure.
   */
  def parameters: Array[ProcedureParameter]

  /**
   * Returns the type of rows produced by this procedure.
   */
  def outputType: StructType

  /**
   * Executes this procedure.
   * <p>
   * Spark will align the provided arguments according to the input parameters
   * defined in {@link #parameters ( )} either by position or by name before execution.
   * <p>
   * Implementations may provide a summary of execution by returning one or many rows
   * as a result. The schema of output rows must match the defined output type
   * in {@link #outputType ( )}.
   *
   * @param args input arguments
   * @return the result of executing this procedure with the given arguments
   */
  def call(args: ProcedureArgs): Seq[Row]

  /**
   * Returns the description of this procedure.
   */
  def description: String = this.getClass.toString
}
