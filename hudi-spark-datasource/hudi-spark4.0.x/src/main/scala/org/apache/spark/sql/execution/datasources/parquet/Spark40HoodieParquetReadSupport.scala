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

import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.exception.HoodieException

import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type, Types}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType, VariantType}

import java.time.ZoneId

import scala.collection.JavaConverters._

// TODO(#18935): Delete this file when the hudi-spark4.0.x module is removed. Spark 4.1+ reads
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
    // VariantType -- a user struct that happens to be <value: binary, metadata: binary>
    // shouldn't be silently reshuffled.
    val sparkRequestedSchema = Option(context.getConfiguration.get(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
      .map(StructType.fromString)
    Spark40HoodieParquetReadSupport.rejectShreddedVariants(
      baseContext.getRequestedSchema, sparkRequestedSchema)
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
   *
   * Shredded groups are left untouched here; rejecting them is [[rejectShreddedVariants]]'s
   * job, which walks the whole schema rather than just the top level.
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
      case group: GroupType if isVariantGroup(group) && group.containsField("typed_value") =>
        // A shredded group: rebuilding it as [value, metadata] would drop typed_value, so leave
        // it exactly as it is. rejectShreddedVariants has already failed the read by this point.
        group
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

  /**
   * Fails the read when the requested schema carries a shredded variant (typed_value present).
   * Spark 4.0's ParquetUnshreddedVariantConverter reads only [value, metadata], so the typed
   * rows would come back partial or null; there is no reorder that makes them readable.
   *
   * With a Spark catalyst schema the walk is anchored on it and recurses through structs,
   * arrays and maps: only what catalyst types as `VariantType` is rejected, and a variant
   * nested inside a struct is just as unreadable as a top-level one. A column Spark's
   * PushVariantIntoScan rewrote into a struct of ordinal-named extraction fields is rejected on
   * the parquet shape alone, since none of those field names exist in the file. Without a
   * catalyst schema (callers that have none) it falls back to a shape-only walk over every group.
   */
  def rejectShreddedVariants(schema: MessageType, sparkSchema: Option[StructType]): Unit = {
    sparkSchema match {
      case Some(catalyst) =>
        catalyst.fields.foreach { field =>
          if (schema.containsField(field.name)) {
            rejectShreddedVariant(schema.getType(schema.getFieldIndex(field.name)), field.dataType, field.name)
          }
        }
      case None => rejectShreddedVariantsByShape(schema, "")
    }
  }

  private def rejectShreddedVariant(parquetType: Type, dataType: DataType, path: String): Unit = {
    (parquetType, dataType) match {
      case (group: GroupType, _: VariantType) =>
        if (group.containsField("typed_value")) {
          throw shreddedVariantException(path)
        }
      case (group: GroupType, struct: StructType) if SparkInternalSchemaConverter.isVariantRewriteStruct(struct) =>
        // PushVariantIntoScan replaced the variant with a struct of ordinal-named extraction
        // fields ("0", "1"), none of which exist in the parquet group, so the generic struct arm
        // below would walk nothing and let a shredded group through. On that path the
        // file-group-reader format forces row reads, which leaves this read support as the last
        // stop before Spark's own converter.
        if (group.containsField("typed_value")) {
          throw shreddedVariantException(path)
        }
      case (group: GroupType, struct: StructType) =>
        struct.fields.foreach { field =>
          if (group.containsField(field.name)) {
            rejectShreddedVariant(group.getType(field.name), field.dataType, path + "." + field.name)
          }
        }
      case (group: GroupType, array: ArrayType) =>
        ParquetSchemaEvolutionUtils.parquetListElement(group)
          .foreach(rejectShreddedVariant(_, array.elementType, path + ".element"))
      case (group: GroupType, map: MapType) =>
        ParquetSchemaEvolutionUtils.parquetMapValue(group)
          .foreach(rejectShreddedVariant(_, map.valueType, path + ".value"))
      case _ =>
    }
  }

  /** Shape-only fallback: any variant-shaped group carrying typed_value, at any depth. */
  private def rejectShreddedVariantsByShape(group: GroupType, path: String): Unit = {
    group.getFields.asScala.foreach { field =>
      if (!field.isPrimitive) {
        val child = field.asGroupType()
        val childPath = if (path.isEmpty) field.getName else path + "." + field.getName
        if (isVariantGroup(child) && child.containsField("typed_value")) {
          throw shreddedVariantException(childPath)
        }
        rejectShreddedVariantsByShape(child, childPath)
      }
    }
  }

  private def shreddedVariantException(path: String): HoodieException = {
    new HoodieException(String.format(
      "Column '%s' is a shredded variant (typed_value present); Hudi's Spark 4.0 reader does "
        + "not support shredded variants. Read the table with Spark 4.1+, or rewrite it "
        + "unshredded (e.g. cluster with hoodie.parquet.variant.write.shredding.enabled=false).", path))
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
