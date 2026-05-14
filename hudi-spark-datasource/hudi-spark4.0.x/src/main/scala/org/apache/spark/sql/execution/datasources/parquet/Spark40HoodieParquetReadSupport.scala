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

import org.apache.hudi.common.util.{Option => HOption}

import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type, Types}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.types.{StructType, VariantType}

import java.time.ZoneId

import scala.collection.JavaConverters._

// TODO: Delete this file when the hudi-spark4.0.x module is removed. Spark 4.1+ reads
//  variant fields by name via SPARK-54410, so the reorder workaround below is no longer
//  needed there. Spark 4.0.x's ParquetUnshreddedVariantConverter builds its converters
//  array in hardcoded [value, metadata] order, then indexes by schema position. If the
//  Parquet schema has [metadata, value] order (per spec), the positional mismatch causes
//  MALFORMED_VARIANT. Workaround: reorder variant group fields to [value, metadata] in
//  the requested schema. parquet-mr reconciles requested vs file schema by field name,
//  so bytes flow correctly. Tracked in issue #18334.
class Spark40HoodieParquetReadSupport(
                                       convertTz: Option[ZoneId],
                                       enableVectorizedReader: Boolean,
                                       enableTimestampFieldRepair: Boolean,
                                       datetimeRebaseSpec: RebaseSpec,
                                       int96RebaseSpec: RebaseSpec,
                                       tableSchemaOpt: HOption[MessageType] = HOption.empty())
  extends HoodieParquetReadSupport(
    convertTz, enableVectorizedReader, enableTimestampFieldRepair,
    datetimeRebaseSpec, int96RebaseSpec, tableSchemaOpt) {

  override def init(context: InitContext): ReadContext = {
    val baseContext = super.init(context)
    // Resolve the Spark catalyst requested schema so the reorder is gated on
    // VariantType — a user struct that happens to be <value: binary, metadata: binary>
    // shouldn't be silently reshuffled.
    val sparkRequestedSchema = Option(context.getConfiguration.get(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
      .map(StructType.fromString)
    val reorderedSchema = Spark40HoodieParquetReadSupport.reorderVariantFields(
      baseContext.getRequestedSchema, sparkRequestedSchema)
    new ReadContext(reorderedSchema, baseContext.getReadSupportMetadata)
  }
}

object Spark40HoodieParquetReadSupport {
  /**
   * Reorders variant group fields in the requested schema so that "value" precedes "metadata".
   * This works around Spark 4.0.x's ParquetUnshreddedVariantConverter, which builds its
   * converters array in hardcoded [value, metadata] order and indexes by schema position.
   * parquet-mr reconciles the requested schema against the file schema by field name,
   * so the correct bytes still flow to the correct converters regardless of file order.
   *
   * When a Spark catalyst schema is supplied, reorder only the top-level fields that are
   * actually typed `VariantType` in catalyst; this prevents reshuffling a user-defined
   * `struct<value: binary, metadata: binary>` that happens to match the parquet shape.
   */
  def reorderVariantFields(schema: MessageType, sparkSchema: Option[StructType] = None): MessageType = {
    val variantFieldNames: Set[String] = sparkSchema match {
      case Some(s) => s.fields.collect { case f if f.dataType.isInstanceOf[VariantType] => f.name }.toSet
      case None => null
    }
    val reordered = schema.getFields.asScala.map { f =>
      if (variantFieldNames == null || variantFieldNames.contains(f.getName)) {
        reorderVariantType(f)
      } else f
    }.toArray[Type]
    Types.buildMessage().addFields(reordered: _*).named(schema.getName)
  }

  private def reorderVariantType(t: Type): Type = {
    t match {
      case group: GroupType if isVariantGroup(group) =>
        // Rebuild with [value, metadata] order for Spark compatibility
        val valueField = group.getType("value")
        val metadataField = group.getType("metadata")
        group.withNewFields(java.util.Arrays.asList(valueField, metadataField))
      case group: GroupType =>
        // Recurse into nested groups
        val children = group.getFields.asScala.map(reorderVariantType).asJava
        group.withNewFields(children)
      case _ => t
    }
  }

  private def isVariantGroup(group: GroupType): Boolean = {
    group.containsField("value") &&
      group.containsField("metadata") &&
      group.getType("value").isPrimitive &&
      group.getType("metadata").isPrimitive &&
      group.getType("value").asPrimitiveType().getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY &&
      group.getType("metadata").asPrimitiveType().getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
  }
}
