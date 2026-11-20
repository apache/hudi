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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.HoodieSchemaConversionUtils
import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/**
 * The upstream nested-pruning projection keeps the distinct values and nulls of BLOBs nested in arrays and maps.
 */
class TestBlobFileGroupReaderOutputType {

  private val wholeBlob: StructType =
    HoodieSchemaConversionUtils.convertHoodieSchemaToDataType(HoodieSchema.createBlob()).asInstanceOf[StructType]

  /** The BLOB struct with its reference alone, as Spark's nested schema pruning requests it. */
  private val referenceOnly: StructType = StructType(Seq(wholeBlob(HoodieSchema.Blob.EXTERNAL_REFERENCE)))

  /** A whole OUT_OF_LINE BLOB value whose reference names `path`. */
  private def blobValue(path: String): InternalRow = {
    val referenceType = wholeBlob(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType.asInstanceOf[StructType]
    val reference = new GenericInternalRow(referenceType.fields.map(_.name match {
      case "external_path" => UTF8String.fromString(path)
      case "offset" => 0L
      case "length" => 100L
      case "managed" => false
    }).asInstanceOf[Array[Any]])
    new GenericInternalRow(wholeBlob.fields.map { field =>
      if (field.name == HoodieSchema.Blob.EXTERNAL_REFERENCE) reference
      else if (field.dataType == StringType) UTF8String.fromString("OUT_OF_LINE")
      else null
    }.asInstanceOf[Array[Any]])
  }

  /** The external paths `aligned` holds in the order of its elements, read from its unsafe projection as the scan does. */
  private def paths(aligned: InternalRow, to: StructType, isMap: Boolean): Seq[String] = {
    val unsafe = UnsafeProjection.create(to)(aligned)
    val elements = if (isMap) unsafe.getMap(0).valueArray() else unsafe.getArray(0)
    (0 until elements.numElements()).map(i => elements.getStruct(i, 1).getStruct(0, 4).getUTF8String(0).toString)
  }

  @Test
  def alignedArrayOfPrunedBlobsKeepsEachElement(): Unit = {
    val from = StructType(Seq(StructField("a", ArrayType(wholeBlob, containsNull = true))))
    val to = StructType(Seq(StructField("a", ArrayType(referenceOnly, containsNull = true))))
    val row = new GenericInternalRow(Array[Any](new GenericArrayData(Array[Any](blobValue("/p0"), blobValue("/p1")))))
    assertEquals(Seq("/p0", "/p1"), paths(SparkSchemaTransformUtils.generateNestedPruningProjection(from, to)(row), to, isMap = false))
  }

  @Test
  def alignedMapOfPrunedBlobsKeepsEachValue(): Unit = {
    val from = StructType(Seq(StructField("m", MapType(StringType, wholeBlob, valueContainsNull = true))))
    val to = StructType(Seq(StructField("m", MapType(StringType, referenceOnly, valueContainsNull = true))))
    val keys = new GenericArrayData(Array[Any](UTF8String.fromString("x"), UTF8String.fromString("y")))
    val row = new GenericInternalRow(Array[Any](new ArrayBasedMapData(keys,
      new GenericArrayData(Array[Any](blobValue("/px"), blobValue("/py"))))))
    assertEquals(Seq("/px", "/py"), paths(SparkSchemaTransformUtils.generateNestedPruningProjection(from, to)(row), to, isMap = true))
  }

  @Test
  def alignedNullsOfPrunedBlobsStayNull(): Unit = {
    val from = StructType(Seq(StructField("a", ArrayType(wholeBlob, containsNull = true)),
      StructField("m", MapType(StringType, wholeBlob, valueContainsNull = true)), StructField("b", wholeBlob),
      StructField("c", ArrayType(wholeBlob, containsNull = true)),
      StructField("n", MapType(StringType, wholeBlob, valueContainsNull = true))))
    val to = StructType(Seq(StructField("a", ArrayType(referenceOnly, containsNull = true)),
      StructField("m", MapType(StringType, referenceOnly, valueContainsNull = true)), StructField("b", referenceOnly),
      StructField("c", ArrayType(referenceOnly, containsNull = true)),
      StructField("n", MapType(StringType, referenceOnly, valueContainsNull = true))))
    val keys = new GenericArrayData(Array[Any](UTF8String.fromString("x"), UTF8String.fromString("y")))
    val row = new GenericInternalRow(Array[Any](new GenericArrayData(Array[Any](blobValue("/p0"), null)),
      new ArrayBasedMapData(keys, new GenericArrayData(Array[Any](null, blobValue("/py")))), null, null, null))
    val unsafe = UnsafeProjection.create(to)(SparkSchemaTransformUtils.generateNestedPruningProjection(from, to)(row))
    val array = unsafe.getArray(0)
    assertEquals(2, array.numElements())
    assertEquals("/p0", array.getStruct(0, 1).getStruct(0, 4).getUTF8String(0).toString)
    assertTrue(array.isNullAt(1))
    val values = unsafe.getMap(1).valueArray()
    assertEquals(2, values.numElements())
    assertTrue(values.isNullAt(0))
    assertFalse(values.isNullAt(1))
    assertEquals("/py", values.getStruct(1, 1).getStruct(0, 4).getUTF8String(0).toString)
    assertTrue(unsafe.isNullAt(2))
    assertTrue(unsafe.isNullAt(3))
    assertTrue(unsafe.isNullAt(4))
  }

  @Test
  def alignmentRefusesARequestedFieldTheReadTypeLacks(): Unit = {
    val from = StructType(Seq(StructField("a", wholeBlob)))
    val to = StructType(Seq(StructField("a", referenceOnly), StructField("z", IntegerType)))
    assertThrows(classOf[NoSuchElementException], () => { SparkSchemaTransformUtils.generateNestedPruningProjection(from, to); () })
  }

  @Test
  def equalSchemasPreserveTheStoredReference(): Unit = {
    val schema = StructType(Seq(StructField("b", wholeBlob)))
    val row = new GenericInternalRow(Array[Any](blobValue("/unchanged")))
    val projected = SparkSchemaTransformUtils.generateNestedPruningProjection(schema, schema)(row)
    assertEquals("/unchanged", projected.getStruct(0, wholeBlob.length).getStruct(2, 4).getUTF8String(0).toString)
  }

}
