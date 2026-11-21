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

package org.apache.hudi.dataframe

import org.apache.hudi.{AvroConversionUtils, HoodieSchemaConversionUtils, SparkAdapterSupport}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.keygen.{KeyGenUtils, SparkKeyGeneratorInterface}
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Evaluates the table's key generator on the input row and yields
 * {@code struct<record_key: string, partition_path: string>}, so key generation is a projection
 * of the query plan and its output is the one the key generator defines.
 *
 * <p>The expression is nondeterministic because auto-generated record keys depend on the Spark
 * partition and on the position of the row within it.
 *
 * @param children  the input columns, in the order of {@code inputSchema}.
 * @param keyGenProps properties the key generator is created from.
 * @param instantTime instant of the write, the prefix of auto-generated record keys.
 * @param autoGenerateRecordKeys whether the table has no record key fields.
 */
case class HoodieKeyGeneratorExpression(children: Seq[Expression],
                                        inputSchema: StructType,
                                        keyGenProps: TypedProperties,
                                        instantTime: String,
                                        autoGenerateRecordKeys: Boolean)
  extends Expression with Nondeterministic with CodegenFallback {

  @transient private var generateKeys: InternalRow => InternalRow = _

  override def nullable: Boolean = false

  override def dataType: DataType = HoodieKeyGeneratorExpression.OUTPUT_TYPE

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    val props = TypedProperties.copy(keyGenProps)
    if (autoGenerateRecordKeys) {
      props.setProperty(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, String.valueOf(partitionIndex))
      props.setProperty(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, instantTime)
    }
    generateKeys = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props) match {
      case rowKeyGenerator: SparkKeyGeneratorInterface =>
        row => InternalRow(
          rowKeyGenerator.getRecordKey(row, inputSchema),
          rowKeyGenerator.getPartitionPath(row, inputSchema))
      case avroKeyGenerator =>
        // A key generator that only reads Avro records gets the row converted for it.
        val avroSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
          inputSchema, HoodieKeyGeneratorExpression.AVRO_RECORD_NAME, HoodieKeyGeneratorExpression.AVRO_RECORD_NAMESPACE)
        val toAvro = AvroConversionUtils.createInternalRowToAvroConverter(inputSchema, avroSchema, nullable = false)
        row => {
          val key = avroKeyGenerator.getKey(toAvro(row))
          InternalRow(UTF8String.fromString(key.getRecordKey), UTF8String.fromString(key.getPartitionPath))
        }
    }
  }

  override protected def evalInternal(input: InternalRow): Any = {
    val values = new Array[Any](children.length)
    var i = 0
    while (i < values.length) {
      values(i) = children(i).eval(input)
      i += 1
    }
    generateKeys(new GenericInternalRow(values))
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

object HoodieKeyGeneratorExpression extends SparkAdapterSupport {

  private val AVRO_RECORD_NAME = "hoodie_source"
  private val AVRO_RECORD_NAMESPACE = "hoodie.source"

  val RECORD_KEY_FIELD = "record_key"
  val PARTITION_PATH_FIELD = "partition_path"

  val OUTPUT_TYPE: StructType = StructType(Seq(
    StructField(RECORD_KEY_FIELD, StringType, nullable = false),
    StructField(PARTITION_PATH_FIELD, StringType, nullable = false)))

  /** The key generator column over all columns of `input`. */
  def column(input: DataFrame,
             keyGenProps: TypedProperties,
             instantTime: String,
             autoGenerateRecordKeys: Boolean): Column = {
    val children = input.columns.toSeq.map(name => sparkAdapter.getExpressionFromColumn(input(name)))
    sparkAdapter.createColumnFromExpression(
      HoodieKeyGeneratorExpression(children, input.schema, keyGenProps, instantTime, autoGenerateRecordKeys))
  }
}
