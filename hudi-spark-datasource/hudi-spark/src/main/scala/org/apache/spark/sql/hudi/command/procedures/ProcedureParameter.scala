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

import org.apache.spark.sql.types.DataType

/**
 * An input parameter of a {@link Procedure stored procedure}.
 */
abstract class ProcedureParameter {
  def index: Int

  /**
   * Returns the name of this parameter.
   */
  def name: String

  /**
   * Returns the type of this parameter.
   */
  def dataType: DataType

  /**
   * Returns true if this parameter is required.
   */
  def required: Boolean

  /**
   * this parameter's default value.
   */
  def default: Any
}

object ProcedureParameter {
  /**
   * Creates a required input parameter.
   *
   * @param name     the name of the parameter
   * @param dataType the type of the parameter
   * @return the constructed stored procedure parameter
   */
  def required(index: Int, name: String, dataType: DataType, default: Any): ProcedureParameterImpl = {
    ProcedureParameterImpl(index, name, dataType, default, required = true)
  }

  /**
   * Creates an optional input parameter.
   *
   * @param name     the name of the parameter.
   * @param dataType the type of the parameter.
   * @return the constructed optional stored procedure parameter
   */
  def optional(index: Int, name: String, dataType: DataType, default: Any): ProcedureParameterImpl = {
    ProcedureParameterImpl(index, name, dataType, default, required = false)
  }
}
