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

import java.util.Objects

case class ProcedureParameterImpl(index: Int, name: String, dataType: DataType, default: Any, required: Boolean)
  extends ProcedureParameter {

  override def equals(other: Any): Boolean = {
    val that = other.asInstanceOf[ProcedureParameterImpl]
    val rtn = if (this == other) {
      true
    } else if (other == null || (getClass ne other.getClass)) {
      false
    } else {
      index == that.index && required == that.required && default == that.default && Objects.equals(name, that.name) && Objects.equals(dataType, that.dataType)
    }
    rtn
  }

  override def hashCode: Int = Seq(index, name, dataType, required, default).hashCode()

  override def toString: String = s"ProcedureParameter(index='$index',name='$name', type=$dataType, required=$required, default=$default)"
}
