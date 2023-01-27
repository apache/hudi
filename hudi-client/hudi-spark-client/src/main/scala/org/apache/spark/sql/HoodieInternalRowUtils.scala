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

import org.apache.avro.Schema
import org.apache.hbase.thirdparty.com.google.common.base.Supplier
import org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType
import org.apache.hudi.avro.HoodieAvroUtils.createFullName
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.HoodieUnsafeRowUtils.{NestedFieldPath, composeNestedFieldPath}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeArrayData, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util.{ArrayDeque => JArrayDeque, Deque => JDeque, Map => JMap, Collections => JCollections}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParHashMap
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object HoodieInternalRowUtils {

  private type RenamedColumnMap = JMap[String, String]
  private type RowWriter = InternalRow => InternalRow

  // Projection are all thread local. Projection is not thread-safe
  private val unsafeWriterAndProjectionThreadLocal: ThreadLocal[mutable.HashMap[(StructType, StructType, RenamedColumnMap), (RowWriter, UnsafeProjection)]] =
    ThreadLocal.withInitial(new Supplier[mutable.HashMap[(StructType, StructType, RenamedColumnMap), (RowWriter, UnsafeProjection)]] {
      override def get(): mutable.HashMap[(StructType, StructType, RenamedColumnMap), (RowWriter, UnsafeProjection)] =
        new mutable.HashMap[(StructType, StructType, RenamedColumnMap), (RowWriter, UnsafeProjection)]
    })

  private val schemaMap = new ParHashMap[Schema, StructType]
  private val orderPosListMap = new ParHashMap[(StructType, String), NestedFieldPath]

  def genUnsafeRowWriterRenaming(prevSchema: StructType, newSchema: StructType, renamedColumnsMap: JMap[String, String]): RowWriter = {
    val writer = newWriterRenaming(prevSchema, newSchema, renamedColumnsMap, new JArrayDeque[String]())
    val phonyUpdater = new CatalystDataUpdater {
      var value: InternalRow = _

      override def set(ordinal: Int, value: Any): Unit =
        this.value = value.asInstanceOf[InternalRow]
    }

    oldRow => {
      writer(phonyUpdater, 0, oldRow)
      phonyUpdater.value
    }
  }

  def genUnsafeRowWriter(prevSchema: StructType, newSchema: StructType): RowWriter = {
    val writer = newWriter(prevSchema, newSchema)
    val phonyUpdater = new CatalystDataUpdater {
      var value: InternalRow = _

      override def set(ordinal: Int, value: Any): Unit =
        this.value = value.asInstanceOf[InternalRow]
    }

    oldRow => {
      writer(phonyUpdater, 0, oldRow)
      phonyUpdater.value
    }
  }

  def getCachedUnsafeProjection(from: StructType, to: StructType): UnsafeProjection = {
    val (_, projection) = getCachedUnsafeRowWriterAndUnsafeProjection(from, to)
    projection
  }

  def getCachedUnsafeRowWriterAndUnsafeProjection(from: StructType,
                                                  to: StructType,
                                                  renamedColumnsMap: JMap[String, String] = JCollections.emptyMap()): (RowWriter, UnsafeProjection) = {
    unsafeWriterAndProjectionThreadLocal.get()
      .getOrElseUpdate((from, to, renamedColumnsMap), (genUnsafeRowWriterRenaming(from, to, renamedColumnsMap), generateUnsafeProjection(from, to)))
  }

  def getCachedPosList(structType: StructType, field: String): Option[NestedFieldPath] =
    Option(orderPosListMap.getOrElse((structType, field), composeNestedFieldPath(structType, field)))

  def getCachedSchema(schema: Schema): StructType =
    schemaMap.getOrElse(schema, convertAvroSchemaToStructType(schema))

  /*
  // TODO cleanup
  private def rewritePrimaryTypeWithDiffSchemaType(oldValue: Any, oldSchema: DataType, newSchema: DataType): Any = {
    val value = newSchema match {
      case NullType | BooleanType =>
      case DateType if oldSchema.equals(StringType) =>
        CatalystTypeConverters.convertToCatalyst(java.sql.Date.valueOf(oldValue.toString))
      case LongType =>
        oldSchema match {
          case IntegerType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Int].longValue())
          case _ =>
        }
      case FloatType =>
        oldSchema match {
          case IntegerType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Int].floatValue())
          case LongType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Long].floatValue())
          case _ =>
        }
      case DoubleType =>
        oldSchema match {
          case IntegerType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Int].doubleValue())
          case LongType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[Long].doubleValue())
          case FloatType => CatalystTypeConverters.convertToCatalyst(java.lang.Double.valueOf(oldValue.asInstanceOf[Float] + ""))
          case _ =>
        }
      case BinaryType =>
        oldSchema match {
          case StringType => CatalystTypeConverters.convertToCatalyst(oldValue.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
          case _ =>
        }
      case StringType =>
        oldSchema match {
          case BinaryType => CatalystTypeConverters.convertToCatalyst(new String(oldValue.asInstanceOf[Array[Byte]]))
          case DateType => CatalystTypeConverters.convertToCatalyst(toJavaDate(oldValue.asInstanceOf[Integer]).toString)
          case IntegerType | LongType | FloatType | DoubleType | DecimalType() => CatalystTypeConverters.convertToCatalyst(oldValue.toString)
          case _ =>
        }
      case DecimalType() =>
        oldSchema match {
          case IntegerType | LongType | FloatType | DoubleType | StringType =>
            val scale = newSchema.asInstanceOf[DecimalType].scale

            Decimal.fromDecimal(BigDecimal(oldValue.toString).setScale(scale))
          case _ =>
        }
      case _ =>
    }

    if (value == None) {
      throw new HoodieException(String.format("cannot support rewrite value for schema type: %s since the old schema type is: %s", newSchema, oldSchema))
    } else {
      CatalystTypeConverters.convertToCatalyst(value)
    }
  }
   */
  
  private type RowFieldUpdater = (CatalystDataUpdater, Int, Any) => Unit

  private def genUnsafeRowWriterRenaming(prevStructType: StructType, newStructType: StructType, renamedColumnsMap: JMap[String, String], fieldNames: JDeque[String]): (CatalystDataUpdater, Any) => Unit = {
    // TODO need to canonicalize schemas (casing)
    val fieldWriters = ArrayBuffer.empty[RowFieldUpdater]
    val positionMap = ArrayBuffer.empty[Int]

    for (newField <- newStructType.fields) {
      fieldNames.push(newField.name)

      val (fieldWriter, prevFieldPos): (RowFieldUpdater, Int) =
        prevStructType.getFieldIndex(newField.name) match {
          case Some(prevFieldPos) =>
            val prevField = prevStructType(prevFieldPos)
            (newWriterRenaming(prevField.dataType, newField.dataType, renamedColumnsMap, fieldNames), prevFieldPos)

          case None =>
            val newFieldQualifiedName = createFullName(fieldNames)
            val prevFieldName: String = lookupRenamedField(newFieldQualifiedName, renamedColumnsMap)

            // Handle rename
            prevStructType.getFieldIndex(prevFieldName) match {
              case Some(prevFieldPos) =>
                val prevField = prevStructType.fields(prevFieldPos)
                (newWriterRenaming(prevField.dataType, newField.dataType, renamedColumnsMap, fieldNames), prevFieldPos)

              case None =>
                // TODO handle defaults
                val updater: RowFieldUpdater = (fieldUpdater, ordinal, _) => fieldUpdater.setNullAt(ordinal)
                (updater, -1)
            }
        }

      fieldWriters += fieldWriter
      positionMap += prevFieldPos

      fieldNames.pop()
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

        fieldWriters(pos)(fieldUpdater, pos, prevValue)
        pos += 1
      }
    }
  }

  private def newWriterRenaming(prevDataType: DataType,
                                newDataType: DataType,
                                renamedColumnsMap: JMap[String, String],
                                fieldNames: JDeque[String]): RowFieldUpdater = {
    (prevDataType, newDataType) match {
      case (prevType, newType) if prevType == newType =>
        (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, value)

      case (prevStructType: StructType, newStructType: StructType) =>
        val writer = genUnsafeRowWriterRenaming(prevStructType, newStructType, renamedColumnsMap, fieldNames)
        val newRow = new SpecificInternalRow(newStructType.fields.map(_.dataType))
        val rowUpdater = new RowUpdater(newRow)

        (fieldUpdater, ordinal, value) => {
          // TODO elaborate
          writer(rowUpdater, value)
          fieldUpdater.set(ordinal, newRow)
        }

      case (ArrayType(prevElementType, containsNull), ArrayType(newElementType, _)) =>
        fieldNames.push("element")
        val elementWriter = newWriterRenaming(prevElementType, newElementType, renamedColumnsMap, fieldNames)
        fieldNames.pop()

        (fieldUpdater, ordinal, value) => {
          val array = value.asInstanceOf[Array[Any]]
          val result = createArrayData(newElementType, array.length)
          val elementUpdater = new ArrayDataUpdater(result)

          var i = 0
          while (i < array.length) {
            val element = array(i)
            if (element == null) {
              if (!containsNull) {
                throw new HoodieException(
                  s"Array value at path '${fieldNames.asScala.mkString(".")}' is not allowed to be null")
              } else {
                elementUpdater.setNullAt(i)
              }
            } else {
              elementWriter(elementUpdater, i, element)
            }
            i += 1
          }

          fieldUpdater.set(ordinal, result)
        }

      case (MapType(_, prevValueType, valueContainsNull), MapType(newKeyType, newValueType, _)) =>
        //val newKeyArray = new GenericArrayData(Array.fill(oldMap.keyArray().numElements())(null).asInstanceOf[Array[Any]])
        //val newValueArray = new GenericArrayData(Array.fill(oldMap.valueArray().numElements())(null).asInstanceOf[Array[Any]])
        //val newMap = new ArrayBasedMapData(newKeyArray, newValueArray)
        //oldMap.keyArray().toSeq[Any](prevKeyType).zipWithIndex.foreach { case (value, i) => newKeyArray.update(i, rewritePrimaryType(value, prevKeyType, prevKeyType)) }
        //oldMap.valueArray().toSeq[Any](prevValueType).zipWithIndex.foreach { case (value, i) => newValueArray.update(i, rewriteRecordWithNewSchema(value.asInstanceOf[AnyRef], prevValueType, newValueType, renamedColumnsMap, fieldNames)) }
        //
        //newMap

        fieldNames.push("value")
        val valueWriter = newWriterRenaming(prevValueType, newValueType, renamedColumnsMap, fieldNames)
        fieldNames.pop()

        (updater, ordinal, value) =>
          val map = value.asInstanceOf[Map[AnyRef, AnyRef]]
          val keyArray = createArrayData(newKeyType, map.size)
          val keyUpdater = new ArrayDataUpdater(keyArray)
          val valueArray = createArrayData(newValueType, map.size)
          val valueUpdater = new ArrayDataUpdater(valueArray)
          val iter = map.iterator
          var i = 0
          while (iter.hasNext) {
            val (key, value) = iter.next()
            checkArgument(key != null)
            keyUpdater.set(i, key)

            if (value == null) {
              if (!valueContainsNull) {
                throw new HoodieException(s"Map value at path ${fieldNames.asScala.mkString(".")} is not allowed to be null")
              } else {
                valueUpdater.setNullAt(i)
              }
            } else {
              valueWriter(valueUpdater, i, value)
            }
            i += 1
          }

          updater.set(ordinal, new ArrayBasedMapData(keyArray, valueArray))

      case (_, _) => newWriter(prevDataType, newDataType)
    }
  }

  private def genUnsafeRowWriterInternal(prevStructType: StructType, newStructType: StructType): (CatalystDataUpdater, Any) => Unit = {
    // TODO need to canonicalize schemas (casing)
    val fieldWriters = ArrayBuffer.empty[RowFieldUpdater]
    val positionMap = ArrayBuffer.empty[Int]

    for (newField <- newStructType.fields) {
      val prevFieldPos = prevStructType.getFieldIndex(newField.name).getOrElse(-1)
      val fieldWriter: RowFieldUpdater =
        if (prevFieldPos >= 0) {
          val prevField = prevStructType.fields(prevFieldPos)
          newWriter(prevField.dataType, newField.dataType)
        } else {
          // TODO handle defaults
          (setter, ordinal, _) => setter.setNullAt(ordinal)
        }

      positionMap += prevFieldPos
      fieldWriters += fieldWriter
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

        fieldWriters(pos)(fieldUpdater, pos, prevValue)
        pos += 1
      }
    }
  }

  private def newWriter(prevDataType: DataType, newDataType: DataType): RowFieldUpdater = {
    // TODO support map/array
    (newDataType, prevDataType) match {
      case (newType, prevType) if newType == prevType =>
        (fieldUpdater, ordinal, value) => fieldUpdater.set(ordinal, value)

      case (newStructType: StructType, prevStructType: StructType) =>
        val writer = genUnsafeRowWriterInternal(prevStructType, newStructType)
        val newRow = new SpecificInternalRow(newStructType.fields.map(_.dataType))
        val rowUpdater = new RowUpdater(newRow)
        (fieldUpdater, ordinal, value) => {
          // TODO elaborate
          writer(rowUpdater, value)
          fieldUpdater.set(ordinal, newRow)
        }

      case (newDecimal: DecimalType, _: DecimalType) =>
        // TODO validate decimal type is expanding
        (fieldUpdater, ordinal, value) =>
          fieldUpdater.setDecimal(ordinal, Decimal.fromDecimal(value.asInstanceOf[Decimal].toBigDecimal.setScale(newDecimal.scale)))

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
