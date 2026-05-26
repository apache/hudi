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

package org.apache.hudi

import org.apache.hudi.common.index.vector.{RaBitQEncoder, VectorIndexOptions}
import org.apache.hudi.common.model.HoodieIndexDefinition
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType, HoodieSchemaUtils => HoodieCommonSchemaUtils}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.JavaConverters._

/**
 * Utilities for deriving row-local hidden columns used by vector indexes.
 *
 * <p>The current integration scope is intentionally narrow:
 * only RaBitQ-backed vector indexes are materialized, and the hidden columns
 * are appended as regular write-time columns so they travel through the normal
 * Hudi schema/write machinery.
 */
object VectorIndexHiddenColumnUtils {

  def augmentDataFrameWithHiddenColumns(df: DataFrame,
                                        sourceSchema: HoodieSchema,
                                        latestTableSchemaOpt: Option[HoodieSchema],
                                        metaClient: HoodieTableMetaClient): DataFrame = {
    getVectorIndexDefinitions(metaClient).foldLeft(df) { (currentDf, indexDefinition) =>
      val sourceField = indexDefinition.getSourceFields.get(0)
      val quantizerType = VectorIndexOptions.getQuantizer(indexDefinition.getIndexOptions)
      val resolvedVectorSchemaOpt = latestTableSchemaOpt
        .flatMap(schema => HoodieCommonSchemaUtils.getNestedField(schema, sourceField).toScala)
        .map(_.getRight.schema.getNonNullType)
        .orElse(HoodieCommonSchemaUtils.getNestedField(sourceSchema, sourceField).toScala.map(_.getRight.schema.getNonNullType))

      if (quantizerType != VectorIndexOptions.DEFAULT_QUANTIZER || resolvedVectorSchemaOpt.isEmpty) {
        currentDf
      } else {
        val resolvedVectorSchema = resolvedVectorSchemaOpt.get
        if (resolvedVectorSchema.getType != HoodieSchemaType.VECTOR) {
          currentDf
        } else {
          val vectorSchema = resolvedVectorSchema.asInstanceOf[HoodieSchema.Vector]
          val assumeNormalized = VectorIndexOptions.isRaBitQAssumeNormalized(indexDefinition.getIndexOptions)
          val seed = VectorIndexOptions.getRaBitQSeed(indexDefinition.getIndexOptions)
          val encoder = new RaBitQEncoder(vectorSchema.getDimension, seed, assumeNormalized)
          val binaryColumn = getBinaryCodeColumnName(indexDefinition)
          val scalarColumn = getScalarColumnName(indexDefinition)

          val codeUdf = udf((value: Any) => encodeBinaryCode(value, vectorSchema, encoder))
          val scalarUdf = udf((value: Any) => encodeScalar(value, vectorSchema, encoder))

          val withBinaryCode = currentDf.withColumn(binaryColumn, codeUdf(col(sourceField)))
          if (assumeNormalized) {
            withBinaryCode
          } else {
            withBinaryCode.withColumn(scalarColumn, scalarUdf(col(sourceField)))
          }
        }
      }
    }
  }

  def getVectorIndexDefinitions(metaClient: HoodieTableMetaClient): Seq[HoodieIndexDefinition] = {
    metaClient.getIndexMetadata.toScala
      .map(_.getIndexDefinitions.values.asScala.toSeq
        .filter(indexDefinition => indexDefinition.getIndexName.startsWith(PARTITION_NAME_VECTOR_INDEX_PREFIX)))
      .getOrElse(Seq.empty)
      .sortBy(_.getIndexName)
  }

  def getLogicalIndexName(indexDefinition: HoodieIndexDefinition): String = {
    indexDefinition.getIndexName.stripPrefix(PARTITION_NAME_VECTOR_INDEX_PREFIX)
  }

  def getBinaryCodeColumnName(indexDefinition: HoodieIndexDefinition): String = {
    s"_hudi_vec_${getLogicalIndexName(indexDefinition)}_binary_code"
  }

  def getScalarColumnName(indexDefinition: HoodieIndexDefinition): String = {
    s"_hudi_vec_${getLogicalIndexName(indexDefinition)}_scalar"
  }

  private[hudi] def encodeBinaryCode(value: Any,
                                     vectorSchema: HoodieSchema.Vector,
                                     encoder: RaBitQEncoder): Array[Byte] = {
    if (value == null) {
      null
    } else {
      encoder.encode(toFloatArray(value, vectorSchema)).code
    }
  }

  private[hudi] def encodeScalar(value: Any,
                                 vectorSchema: HoodieSchema.Vector,
                                 encoder: RaBitQEncoder): java.lang.Float = {
    if (value == null) {
      null
    } else {
      java.lang.Float.valueOf(encoder.encode(toFloatArray(value, vectorSchema)).scalar)
    }
  }

  private[hudi] def toFloatArray(value: Any, vectorSchema: HoodieSchema.Vector): Array[Float] = {
    val dimension = vectorSchema.getDimension
    val values = value match {
      case seq: Seq[_] => seq
      case array: Array[_] => array.toSeq
      case wrapped if wrapped != null && wrapped.getClass.getName.contains("WrappedArray") =>
        wrapped.asInstanceOf[Seq[_]]
      case other =>
        throw new IllegalArgumentException(s"Unsupported Spark vector value type: ${other.getClass.getName}")
    }

    require(values.size == dimension,
      s"Expected VECTOR($dimension) but found ${values.size} elements")

    vectorSchema.getVectorElementType match {
      case HoodieSchema.Vector.VectorElementType.FLOAT =>
        values.map {
          case number: Number => number.floatValue()
          case other => throw new IllegalArgumentException(s"Vector element must be numeric: $other")
        }.toArray
      case HoodieSchema.Vector.VectorElementType.DOUBLE =>
        values.map {
          case number: Number => number.doubleValue().toFloat
          case other => throw new IllegalArgumentException(s"Vector element must be numeric: $other")
        }.toArray
      case HoodieSchema.Vector.VectorElementType.INT8 =>
        values.map {
          case number: Number => number.byteValue().toFloat
          case other => throw new IllegalArgumentException(s"Vector element must be numeric: $other")
        }.toArray
      case other =>
        throw new IllegalArgumentException(s"Unsupported vector element type: $other")
    }
  }

  private implicit class RichHoodieOption[T](value: org.apache.hudi.common.util.Option[T]) {
    def toScala: Option[T] = if (value.isPresent) Some(value.get) else None
  }
}
