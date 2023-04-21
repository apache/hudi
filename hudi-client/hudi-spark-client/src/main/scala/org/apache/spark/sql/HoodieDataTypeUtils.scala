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

package org.apache.spark.sql

import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object HoodieDataTypeUtils {

  /**
   * Checks whether provided schema contains Hudi's meta-fields
   *
   * NOTE: This method validates presence of just one field [[HoodieRecord.RECORD_KEY_METADATA_FIELD]],
   * however assuming that meta-fields should either be omitted or specified in full
   */
  def hasMetaFields(structType: StructType): Boolean =
    structType.getFieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD).isDefined

  // TODO scala-doc
  def addMetaFields(schema: StructType): StructType = {
    val metaFieldNames = HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq
    val dataFields = schema.fields.filterNot(f => metaFieldNames.contains(f.name))
    StructType(metaFieldNames.map(StructField(_, StringType)) ++ dataFields)
  }

  /**
   * Parses provided [[jsonSchema]] into [[StructType]].
   *
   * Throws [[RuntimeException]] in case it's unable to parse it as such.
   */
  def parseStructTypeFromJson(jsonSchema: String): StructType =
    StructType.fromString(jsonSchema)

  /**
   * Checks whether provided {@link DataType} contains {@link DecimalType} whose scale is less than
   * {@link Decimal# MAX_LONG_DIGITS ( )}
   */
  def hasSmallPrecisionDecimalType(sparkType: DataType): Boolean = {
    sparkType match {
      case st: StructType =>
        st.exists(f => hasSmallPrecisionDecimalType(f.dataType))

      case map: MapType =>
        hasSmallPrecisionDecimalType(map.keyType) ||
          hasSmallPrecisionDecimalType(map.valueType)

      case at: ArrayType =>
        hasSmallPrecisionDecimalType(at.elementType)

      case dt: DecimalType =>
        dt.precision < Decimal.MAX_LONG_DIGITS

      case _ => false
    }
  }
}
