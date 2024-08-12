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

import org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType
import org.apache.hudi.avro.HoodieAvroUtils.{createFullName, toJavaDate}
import org.apache.hudi.exception.HoodieException

import org.apache.avro.Schema
import org.apache.hbase.thirdparty.com.google.common.base.Supplier
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.HoodieUnsafeRowUtils.{NestedFieldPath, composeNestedFieldPath}
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.Decimal.ROUND_HALF_UP
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}
import java.util.{ArrayDeque => JArrayDeque, Collections => JCollections, Deque => JDeque, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object HoodieInternalRowUtils {

  private type RenamedColumnMap = JMap[String, String]
  private type UnsafeRowWriter = InternalRow => UnsafeRow

  // NOTE: [[UnsafeProjection]] objects cache have to stay [[ThreadLocal]] since these are not thread-safe
  private val unsafeWriterThreadLocal: ThreadLocal[mutable.HashMap[(StructType, StructType, RenamedColumnMap), UnsafeRowWriter]] =
    ThreadLocal.withInitial(new Supplier[mutable.HashMap[(StructType, StructType, RenamedColumnMap), UnsafeRowWriter]] {
      override def get(): mutable.HashMap[(StructType, StructType, RenamedColumnMap), UnsafeRowWriter] =
        new mutable.HashMap[(StructType, StructType, RenamedColumnMap), UnsafeRowWriter]
    })

  // NOTE: [[UnsafeRowWriter]] objects cache have to stay [[ThreadLocal]] since these are not thread-safe
  private val unsafeProjectionThreadLocal: ThreadLocal[mutable.HashMap[(StructType, StructType), UnsafeProjection]] =
    ThreadLocal.withInitial(new Supplier[mutable.HashMap[(StructType, StructType), UnsafeProjection]] {
      override def get(): mutable.HashMap[(StructType, StructType), UnsafeProjection] =
        new mutable.HashMap[(StructType, StructType), UnsafeProjection]
    })

  private val schemaMap = new ConcurrentHashMap[Schema, StructType]
  private val orderPosListMap = new ConcurrentHashMap[(StructType, String), Option[NestedFieldPath]]

  /**
   * Provides cached instance of [[UnsafeProjection]] transforming provided [[InternalRow]]s from
   * one [[StructType]] and into another [[StructType]]
   *
   * For more details regarding its semantic, please check corresponding scala-doc for
   * [[HoodieCatalystExpressionUtils.generateUnsafeProjection]]
   */
  def getCachedUnsafeProjection(from: StructType, to: StructType): UnsafeProjection = {
    unsafeProjectionThreadLocal.get()
      .getOrElseUpdate((from, to), generateUnsafeProjection(from, to))
  }

  /**
   * Provides cached instance of [[UnsafeRowWriter]] transforming provided [[InternalRow]]s from
   * one [[StructType]] and into another [[StructType]]
   *
   * Unlike [[UnsafeProjection]] requiring that [[from]] has to be a proper subset of [[to]] schema,
   * [[UnsafeRowWriter]] is able to perform whole spectrum of schema-evolution transformations including:
   *
   * <ul>
   *   <li>Transforming nested structs/maps/arrays</li>
   *   <li>Handling type promotions (int -> long, etc)</li>
   *   <li>Handling (field) renames</li>
   * </ul>
   */
  def getCachedUnsafeRowWriter(from: StructType, to: StructType, renamedColumnsMap: JMap[String, String] = JCollections.emptyMap()): UnsafeRowWriter = {
    unsafeWriterThreadLocal.get()
      .getOrElseUpdate((from, to, renamedColumnsMap), genUnsafeRowWriter(from, to, renamedColumnsMap))
  }

  def getCachedPosList(structType: StructType, field: String): Option[NestedFieldPath] = {
    val nestedFieldPathOpt = orderPosListMap.get((structType, field))
    // NOTE: This specifically designed to do 2 lookups (in case of cache-miss) to avoid
    //       allocating the closure when using [[computeIfAbsent]] on more frequent cache-hit path
    if (nestedFieldPathOpt != null) {
      nestedFieldPathOpt
    } else {
      orderPosListMap.computeIfAbsent((structType, field), new JFunction[(StructType, String), Option[NestedFieldPath]] {
        override def apply(t: (StructType, String)): Option[NestedFieldPath] =
          composeNestedFieldPath(structType, field)
      })
    }
  }

  def getCachedSchema(schema: Schema): StructType = {
    val structType = schemaMap.get(schema)
    // NOTE: This specifically designed to do 2 lookups (in case of cache-miss) to avoid
    //       allocating the closure when using [[computeIfAbsent]] on more frequent cache-hit path
    if (structType != null) {
      structType
    } else {
      schemaMap.computeIfAbsent(schema, new JFunction[Schema, StructType] {
        override def apply(t: Schema): StructType =
          convertAvroSchemaToStructType(schema)
      })
    }
  }

  private[sql] def genUnsafeRowWriter(prevSchema: StructType,
                                      newSchema: StructType,
                                      renamedColumnsMap: JMap[String, String]): UnsafeRowWriter = {
    val writer = newWriterRenaming(prevSchema, newSchema, renamedColumnsMap, new JArrayDeque[String]())
    val unsafeProjection = generateUnsafeProjection(newSchema, newSchema)
    val phonyUpdater = new CatalystDataUpdater {
      var value: InternalRow = _

      override def set(ordinal: Int, value: Any): Unit =
        this.value = value.asInstanceOf[InternalRow]
    }

    oldRow => {
      writer(phonyUpdater, 0, oldRow)
      unsafeProjection(phonyUpdater.value)
    }
  }

  private type RowFieldUpdater = (CatalystDataUpdater, Int, Any) => Unit

  private def genUnsafeStructWriter(prevStructType: StructType,
                                    newStructType: StructType,
                                    renamedColumnsMap: JMap[String, String],
                                    fieldNamesStack: JDeque[String]): (CatalystDataUpdater, Any) => Unit = {
    // TODO need to canonicalize schemas (casing)
    val fieldWriters = ArrayBuffer.empty[RowFieldUpdater]
    val positionMap = ArrayBuffer.empty[Int]

    for (newField <- newStructType.fields) {
      fieldNamesStack.push(newField.name)

      val (fieldWriter, prevFieldPos): (RowFieldUpdater, Int) =
        prevStructType.getFieldIndex(newField.name) match {
          case Some(prevFieldPos) =>
            val prevField = prevStructType(prevFieldPos)
            (newWriterRenaming(prevField.dataType, newField.dataType, renamedColumnsMap, fieldNamesStack), prevFieldPos)

          case None =>
            val newFieldQualifiedName = createFullName(fieldNamesStack)
            val prevFieldName: String = lookupRenamedField(newFieldQualifiedName, renamedColumnsMap)

            // Handle rename
            prevStructType.getFieldIndex(prevFieldName) match {
              case Some(prevFieldPos) =>
                val prevField = prevStructType.fields(prevFieldPos)
                (newWriterRenaming(prevField.dataType, newField.dataType, renamedColumnsMap, fieldNamesStack), prevFieldPos)

              case None =>
                val updater: RowFieldUpdater = (fieldUpdater, ordinal, _) => fieldUpdater.setNullAt(ordinal)
                (updater, -1)
            }
        }

      fieldWriters += fieldWriter
      positionMap += prevFieldPos

      fieldNamesStack.pop()
    }

    (fieldUpdater, row) => {
      var pos = 0
      while (pos < fieldWriters.length) {
        val prevPos = positionMap(pos)
        val prevValue = if (prevPos >= 0) {
          row.asInstanceOf[InternalRow].get(prevPos, prevStructType.fields(prevPos).dataType)
        } else {
          null
        }

        if(prevValue == null)
          fieldUpdater.setNullAt(pos)
        else
          fieldWriters(pos)(fieldUpdater, pos, prevValue)
        pos += 1
      }
    }
  }

  private def newWriterRenaming(prevDataType: DataType,
                                newDataType: DataType,
                                renamedColumnsMap: JMap[String, String],
                                fieldNameStack: JDeque[String]): RowFieldUpdater = {
    (newDataType, prevDataType) match {
      case (newType, prevType) if prevType.sql == newType.sql =>
        (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, value)

      case (newStructType: StructType, prevStructType: StructType) =>
        val writer = genUnsafeStructWriter(prevStructType, newStructType, renamedColumnsMap, fieldNameStack)

        val newRow = new SpecificInternalRow(newStructType.fields.map(_.dataType))
        val rowUpdater = new RowUpdater(newRow)

        (fieldUpdater, ordinal, value) => {
          // Here new row is built in 2 stages:
          //    - First, we pass mutable row (used as buffer/scratchpad) created above wrapped into [[RowUpdater]]
          //      into generated row-writer
          //    - Upon returning from row-writer, we call back into parent row's [[fieldUpdater]] to set returned
          //      row as a value in it
          writer(rowUpdater, value)
          fieldUpdater.set(ordinal, newRow)
        }

      case (ArrayType(newElementType, _), ArrayType(prevElementType, containsNull)) =>
        fieldNameStack.push("element")
        val elementWriter = newWriterRenaming(prevElementType, newElementType, renamedColumnsMap, fieldNameStack)
        fieldNameStack.pop()

        (fieldUpdater, ordinal, value) => {
          val prevArrayData = value.asInstanceOf[ArrayData]
          val prevArray = prevArrayData.toObjectArray(prevElementType)

          val newArrayData = createArrayData(newElementType, prevArrayData.numElements())
          val elementUpdater = new ArrayDataUpdater(newArrayData)

          var i = 0
          while (i < prevArray.length) {
            val element = prevArray(i)
            if (element == null) {
              if (!containsNull) {
                throw new HoodieException(
                  s"Array value at path '${fieldNameStack.asScala.mkString(".")}' is not allowed to be null")
              } else {
                elementUpdater.setNullAt(i)
              }
            } else {
              elementWriter(elementUpdater, i, element)
            }
            i += 1
          }

          fieldUpdater.set(ordinal, newArrayData)
        }

      case (MapType(_, newValueType, _), MapType(_, prevValueType, valueContainsNull)) =>
        fieldNameStack.push("value")
        val valueWriter = newWriterRenaming(prevValueType, newValueType, renamedColumnsMap, fieldNameStack)
        fieldNameStack.pop()

        (updater, ordinal, value) =>
          val mapData = value.asInstanceOf[MapData]
          val prevKeyArrayData = mapData.keyArray
          val prevValueArrayData = mapData.valueArray
          val prevValueArray = prevValueArrayData.toObjectArray(prevValueType)

          val newValueArray = createArrayData(newValueType, mapData.numElements())
          val valueUpdater = new ArrayDataUpdater(newValueArray)
          var i = 0
          while (i < prevValueArray.length) {
            val value = prevValueArray(i)
            if (value == null) {
              if (!valueContainsNull) {
                throw new HoodieException(s"Map value at path ${fieldNameStack.asScala.mkString(".")} is not allowed to be null")
              } else {
                valueUpdater.setNullAt(i)
              }
            } else {
              valueWriter(valueUpdater, i, value)
            }
            i += 1
          }

          // NOTE: Key's couldn't be transformed and have to always be of [[StringType]]
          updater.set(ordinal, new ArrayBasedMapData(prevKeyArrayData, newValueArray))

      case (newDecimal: DecimalType, _) =>
        prevDataType match {
          case IntegerType | LongType | FloatType | DoubleType | StringType =>
            (fieldUpdater, ordinal, value) =>
              val scale = newDecimal.scale
              // TODO this has to be revisited to avoid loss of precision (for fps)
              fieldUpdater.setDecimal(ordinal, Decimal.fromDecimal(BigDecimal(value.toString).setScale(scale, ROUND_HALF_UP)))

          case _: DecimalType =>
            (fieldUpdater, ordinal, value) =>
              fieldUpdater.setDecimal(ordinal, Decimal.fromDecimal(value.asInstanceOf[Decimal].toBigDecimal.setScale(newDecimal.scale)))

          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: ShortType, _) =>
        prevDataType match {
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setShort(ordinal, value.asInstanceOf[Byte].toShort)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: IntegerType, _) =>
        prevDataType match {
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setInt(ordinal, value.asInstanceOf[Short].toInt)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setInt(ordinal, value.asInstanceOf[Byte].toInt)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: LongType, _) =>
        prevDataType match {
          case _: IntegerType => (fieldUpdater, ordinal, value) => fieldUpdater.setLong(ordinal, value.asInstanceOf[Int].toLong)
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setLong(ordinal, value.asInstanceOf[Short].toLong)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setLong(ordinal, value.asInstanceOf[Byte].toLong)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: FloatType, _) =>
        prevDataType match {
          case _: LongType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Long].toFloat)
          case _: IntegerType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Int].toFloat)
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Short].toFloat)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setFloat(ordinal, value.asInstanceOf[Byte].toFloat)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: DoubleType, _) =>
        prevDataType match {
          case _: FloatType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Float].toDouble)
          case _: LongType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Long].toDouble)
          case _: IntegerType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Int].toDouble)
          case _: ShortType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Short].toDouble)
          case _: ByteType => (fieldUpdater, ordinal, value) => fieldUpdater.setDouble(ordinal, value.asInstanceOf[Byte].toDouble)
          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (_: BinaryType, _: StringType) =>
        (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, value.asInstanceOf[UTF8String].getBytes)

      // TODO revisit this (we need to align permitted casting w/ Spark)
      // NOTE: This is supported to stay compatible w/ [[HoodieAvroUtils.rewriteRecordWithNewSchema]]
      case (_: StringType, _) =>
        prevDataType match {
          case BinaryType => (fieldUpdater, ordinal, value) =>
            fieldUpdater.set(ordinal, UTF8String.fromBytes(value.asInstanceOf[Array[Byte]]))
          case DateType => (fieldUpdater, ordinal, value) =>
            fieldUpdater.set(ordinal, UTF8String.fromString(toJavaDate(value.asInstanceOf[Integer]).toString))
          case IntegerType | LongType | FloatType | DoubleType | _: DecimalType =>
            (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, UTF8String.fromString(value.toString))

          case _ =>
            throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
        }

      case (DateType, StringType) =>
        (fieldUpdater, ordinal, value) =>
          fieldUpdater.set(ordinal, CatalystTypeConverters.convertToCatalyst(java.sql.Date.valueOf(value.toString)))

      case (_, _) =>
        throw new IllegalArgumentException(s"$prevDataType and $newDataType are incompatible")
    }
  }

  private def lookupRenamedField(newFieldQualifiedName: String, renamedColumnsMap: JMap[String, String]) = {
    val prevFieldQualifiedName = renamedColumnsMap.getOrDefault(newFieldQualifiedName, "")
    val prevFieldQualifiedNameParts = prevFieldQualifiedName.split("\\.")
    val prevFieldName = prevFieldQualifiedNameParts(prevFieldQualifiedNameParts.length - 1)

    prevFieldName
  }

  private def createArrayData(elementType: DataType, length: Int): ArrayData = elementType match {
    case BooleanType => UnsafeArrayData.fromPrimitiveArray(new Array[Boolean](length))
    case ByteType => UnsafeArrayData.fromPrimitiveArray(new Array[Byte](length))
    case ShortType => UnsafeArrayData.fromPrimitiveArray(new Array[Short](length))
    case IntegerType => UnsafeArrayData.fromPrimitiveArray(new Array[Int](length))
    case LongType => UnsafeArrayData.fromPrimitiveArray(new Array[Long](length))
    case FloatType => UnsafeArrayData.fromPrimitiveArray(new Array[Float](length))
    case DoubleType => UnsafeArrayData.fromPrimitiveArray(new Array[Double](length))
    case _ => new GenericArrayData(new Array[Any](length))
  }

  sealed trait CatalystDataUpdater {
    def set(ordinal: Int, value: Any): Unit
    def setNullAt(ordinal: Int): Unit = set(ordinal, null)
    def setBoolean(ordinal: Int, value: Boolean): Unit = set(ordinal, value)
    def setByte(ordinal: Int, value: Byte): Unit = set(ordinal, value)
    def setShort(ordinal: Int, value: Short): Unit = set(ordinal, value)
    def setInt(ordinal: Int, value: Int): Unit = set(ordinal, value)
    def setLong(ordinal: Int, value: Long): Unit = set(ordinal, value)
    def setDouble(ordinal: Int, value: Double): Unit = set(ordinal, value)
    def setFloat(ordinal: Int, value: Float): Unit = set(ordinal, value)
    def setDecimal(ordinal: Int, value: Decimal): Unit = set(ordinal, value)
  }

  final class RowUpdater(row: InternalRow) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit =
      row.setDecimal(ordinal, value, value.precision)
  }

  final class ArrayDataUpdater(array: ArrayData) extends CatalystDataUpdater {
    override def set(ordinal: Int, value: Any): Unit = array.update(ordinal, value)
    override def setNullAt(ordinal: Int): Unit = array.setNullAt(ordinal)
    override def setBoolean(ordinal: Int, value: Boolean): Unit = array.setBoolean(ordinal, value)
    override def setByte(ordinal: Int, value: Byte): Unit = array.setByte(ordinal, value)
    override def setShort(ordinal: Int, value: Short): Unit = array.setShort(ordinal, value)
    override def setInt(ordinal: Int, value: Int): Unit = array.setInt(ordinal, value)
    override def setLong(ordinal: Int, value: Long): Unit = array.setLong(ordinal, value)
    override def setDouble(ordinal: Int, value: Double): Unit = array.setDouble(ordinal, value)
    override def setFloat(ordinal: Int, value: Float): Unit = array.setFloat(ordinal, value)
    override def setDecimal(ordinal: Int, value: Decimal): Unit = array.update(ordinal, value)
  }

}
