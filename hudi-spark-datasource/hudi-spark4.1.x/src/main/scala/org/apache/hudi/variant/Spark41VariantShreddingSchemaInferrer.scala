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

package org.apache.hudi.variant

import org.apache.hudi.HoodieSchemaConversionUtils
import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer
import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer.VariantSample
import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.parquet.InferVariantShreddingSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

import java.{util => ju}

import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Infers per-file variant shredding schemas by delegating to Spark 4.1's
 * [[InferVariantShreddingSchema]] (SPARK-53659), so Hudi inherits Spark's merge and
 * finalization heuristics verbatim (field-frequency dropping, type widening, width/depth caps).
 *
 * Loaded reflectively from hudi-common via classpath detection; lives in the spark4.1 module
 * because the Spark class does not exist before 4.1.
 */
class Spark41VariantShreddingSchemaInferrer extends VariantShreddingSchemaInferrer {

  private val typedValueField = HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD
  // Avro identifier shape; the Hudi schema the result splices into is Avro-backed.
  private val avroNamePattern = "[A-Za-z_][A-Za-z0-9_]*".r.pattern
  // Addressed by key because the SQLConf entries behind them are spark-private.
  private val shreddingCapKeys = Seq(
    "spark.sql.variant.shredding.maxSchemaWidth", // SQLConf.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH
    "spark.sql.variant.shredding.maxSchemaDepth") // SQLConf.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH

  override def inferTypedValueSchemas(columnNames: ju.List[String],
                                      rowSamples: ju.List[Array[VariantSample]]): ju.Map[String, HoodieSchema] = {
    val names = columnNames.asScala.toSeq
    val inputSchema = StructType(names.map(name => StructField(name, VariantType, nullable = true)))
    val rows: Seq[InternalRow] = rowSamples.asScala.map { row =>
      val values = new Array[Any](row.length)
      var i = 0
      while (i < row.length) {
        if (row(i) != null) {
          values(i) = new VariantVal(row(i).getValue, row(i).getMetadata)
        }
        i += 1
      }
      new GenericInternalRow(values): InternalRow
    }.toSeq

    // One call covers all variant columns of the file: Spark's max-width budget is global
    // across the schema, and per-column calls would skew it. Resolve the caps before pinning
    // them: inside the block `SQLConf.get` is `capConf` itself.
    val capConf = new SQLConf()
    shreddingCapKeys.foreach(key => resolveShreddingCap(key).foreach(capConf.setConfString(key, _)))
    val inferred = SQLConf.withExistingConf(capConf) {
      new InferVariantShreddingSchema(inputSchema).inferSchema(rows)
    }

    val result = new ju.HashMap[String, HoodieSchema]()
    names.zipWithIndex.foreach { case (name, i) =>
      inferred.fields(i).dataType match {
        case shredded: StructType =>
          // A column the inference declined (all null, mixed root types) has no typed_value member.
          shredded.fields.find(_.name == typedValueField).map(_.dataType)
            .flatMap(sanitizeTypedValue)
            .foreach { typedValue =>
              result.put(name, HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
                typedValue, typedValueField, "hoodie.variant." + name))
            }
        case _ => // defensive: updateSchema always substitutes a StructType
      }
    }
    result
  }

  /**
   * Resolves a shredding cap override for the [[InferVariantShreddingSchema]] constructor, which
   * reads it off `SQLConf.get`: an offline compaction task runs outside any SQL execution scope, so
   * no session conf reached it and the cap silently falls back to Spark's default. The SparkConf
   * fallback mirrors `HoodieRowParquetWriteSupport#resolveSessionLocalTimeZone`; a missing or
   * non-int value leaves the key unset, which restores Spark's own default.
   */
  private def resolveShreddingCap(key: String): Option[String] = {
    Option(SQLConf.get.getConfString(key, null))
      .orElse(Option(SparkEnv.get).flatMap(env => Option(env.conf.get(key, null))))
      .filter(_.trim.toIntOption.isDefined)
  }

  /**
   * Drops object fields whose names are not valid Avro identifiers (arbitrary JSON keys are
   * legal in a StructType but not in the Avro-backed Hudi schema); dropped fields legally fall
   * back to the residual value column at write time. Returns None when nothing survives, which
   * declines typing at that level entirely.
   */
  private def sanitizeTypedValue(typedValue: DataType): Option[DataType] = typedValue match {
    case obj: StructType =>
      // Object shredding: field names are user JSON keys, each field a {value[, typed_value]} wrapper.
      val kept = obj.fields.iterator
        .filter(field => avroNamePattern.matcher(field.name).matches())
        .map { field =>
          field.dataType match {
            case wrapper: StructType => field.copy(dataType = sanitizeWrapper(wrapper))
            case _ => field
          }
        }.toArray
      if (kept.isEmpty) None else Some(StructType(kept))
    case ArrayType(wrapper: StructType, containsNull) =>
      Some(ArrayType(sanitizeWrapper(wrapper), containsNull))
    case scalar => Some(scalar)
  }

  /** A wrapper whose typed_value sanitizes to nothing drops it and falls back to value-only. */
  private def sanitizeWrapper(wrapper: StructType): StructType = {
    wrapper.fields.find(_.name == typedValueField) match {
      case Some(innerTypedValue) =>
        sanitizeTypedValue(innerTypedValue.dataType) match {
          case Some(clean) if clean eq innerTypedValue.dataType => wrapper
          case Some(clean) => StructType(wrapper.fields.map(field =>
            if (field.name == typedValueField) field.copy(dataType = clean) else field))
          case None => StructType(wrapper.fields.filterNot(_.name == typedValueField))
        }
      case None => wrapper
    }
  }
}
