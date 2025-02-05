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

package org.apache.hudi

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String, VariantVal}

case class Spark4HoodiePartitionValues(values: InternalRow) extends HoodiePartitionValues {
  override def numFields: Int = {
    values.numFields
  }

  override def setNullAt(i: Int): Unit = {
    values.setNullAt(i)
  }

  override def update(i: Int, value: Any): Unit = {
    values.update(i, value)
  }

  override def copy(): InternalRow = {
    Spark4HoodiePartitionValues(values.copy())
  }

  override def isNullAt(ordinal: Int): Boolean = {
    values.isNullAt(ordinal)
  }

  override def getBoolean(ordinal: Int): Boolean = {
    values.getBoolean(ordinal)
  }

  override def getByte(ordinal: Int): Byte = {
    values.getByte(ordinal)
  }

  override def getShort(ordinal: Int): Short = {
    values.getShort(ordinal)
  }

  override def getInt(ordinal: Int): Int = {
    values.getInt(ordinal)
  }

  override def getLong(ordinal: Int): Long = {
    values.getLong(ordinal)
  }

  override def getFloat(ordinal: Int): Float = {
    values.getFloat(ordinal)
  }

  override def getDouble(ordinal: Int): Double = {
    values.getDouble(ordinal)
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    values.getDecimal(ordinal, precision, scale)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    values.getUTF8String(ordinal)
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    values.getBinary(ordinal)
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    values.getInterval(ordinal)
  }

  override def getVariant(ordinal: Int): VariantVal = {
    values.getVariant(ordinal)
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    values.getStruct(ordinal, numFields)
  }

  override def getArray(ordinal: Int): ArrayData = {
    values.getArray(ordinal)
  }

  override def getMap(ordinal: Int): MapData = {
    values.getMap(ordinal)
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    values.get(ordinal, dataType)
  }
}
