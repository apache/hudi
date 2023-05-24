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

package org.apache.hudi;

import org.apache.spark.sql.types.{StructField, StructType}

object StructUtils {

  /**
   * Return StructField from StructType given the field name. The API also returns a nested field from the struct.
   * The nested field name should be accessed using dot separator. For example given a struct,
   *    val innerStruct = StructType(StructField("f3", BooleanType, false) :: Nil)
   *    val struct = StructType(StructField("a", innerStruct, true) :: Nil)
   * The nested field f3 can be accessed by specifying fieldName as a.f3
   */
  def getField(schema: StructType, fieldName: String): Option[StructField] = {
    var fName = fieldName
    var struct = schema
    while (fName.nonEmpty) {
      if (struct == null) {
        return Option.empty
      }

      val baseFieldName = fName.replaceAll("\\..*", "")
      val field = struct.apply(baseFieldName)
      if (!fName.contains(".")) {
        return Option.apply(field)
      }

      fName = fName.replaceFirst(".*\\.", "")
      struct = field.dataType match {
        case _: StructType => field.dataType.asInstanceOf[StructType]
        case _ => null
      }
    }

    Option.empty
  }
}
