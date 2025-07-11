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

package org.apache.hudi.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.junit.jupiter.api.Assertions.assertEquals

object InternalRowTestUtils {



//  def assertRowMatchesSchema(schema: DataType, row: Object) : Unit = {
//    schema match {
//      case structType: StructType =>
//        row match {
//          case internalRow: InternalRow =>
//            assertEquals(internalRow.numFields, structType.fields.length)
//            val fields = structType.fields
//            fields.zipWithIndex.foreach { case (field, i) => assertRowMatchesSchema(field.dataType, internalRow.get(i, field.dataType)) }
//          case _ =>
//            throw new AssertionError()
//        }
//      case arrayType: ArrayType =>
//        row match {
//          case internalRow: InternalRow =>
//            assertEquals(internalRow.numFields, 1)
//            assertRowMatchesSchema(arrayType.elementType, internalRow.get(0, arrayType.elementType))
//          case _ =>
//            throw new AssertionError()
//        }
//      case mapType: MapType =>
//    }
//  }

}
