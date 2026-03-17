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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util.ValidationUtils

import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, SchemaRepair, Type, Types}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec

import java.time.ZoneId

import scala.collection.JavaConverters._

class HoodieParquetReadSupport(
                                convertTz: Option[ZoneId],
                                enableVectorizedReader: Boolean,
                                val enableTimestampFieldRepair: Boolean,
                                datetimeRebaseSpec: RebaseSpec,
                                int96RebaseSpec: RebaseSpec,
                                tableSchemaOpt: org.apache.hudi.common.util.Option[org.apache.parquet.schema.MessageType] = org.apache.hudi.common.util.Option.empty())
  extends ParquetReadSupport(convertTz, enableVectorizedReader, datetimeRebaseSpec, int96RebaseSpec) with SparkAdapterSupport {

  override def init(context: InitContext): ReadContext = {
    val readContext = super.init(context)
    // repair is needed here because this is the schema that is used by the reader to decide what
    // conversions are necessary
    val requestedParquetSchema = if (enableTimestampFieldRepair) {
      SchemaRepair.repairLogicalTypes(readContext.getRequestedSchema, tableSchemaOpt)
    } else {
      readContext.getRequestedSchema
    }
    val trimmedParquetSchema = HoodieParquetReadSupport.trimParquetSchema(requestedParquetSchema, context.getFileSchema)
    // TODO: Remove this workaround once Spark is bumped to 4.1+, which reads variant fields by
    //  name via SPARK-54410. Spark 4.0.x's ParquetUnshreddedVariantConverter builds its converters
    //  array in hardcoded [value, metadata] order, then indexes by schema position. If the Parquet
    //  schema has [metadata, value] order (per spec), the positional mismatch causes
    //  MALFORMED_VARIANT. Workaround: reorder variant group fields to [value, metadata] in the
    //  requested schema. parquet-mr reconciles requested vs file schema by field name, so bytes
    //  flow correctly. This is tracked in issue #18334
    val reorderedSchema = HoodieParquetReadSupport.reorderVariantFields(trimmedParquetSchema)
    new ReadContext(reorderedSchema, readContext.getReadSupportMetadata)
  }
}

object HoodieParquetReadSupport {
  /**
   * Removes any fields from the parquet schema that do not have any child fields in the actual file schema after the
   * schema is trimmed down to the requested fields. This can happen when the table schema evolves and only a subset of
   * the nested fields are required by the query.
   *
   * @param requestedSchema the initial parquet schema requested by Spark
   * @param fileSchema the actual parquet schema of the file
   * @return a potentially updated schema with empty struct fields removed
   */
  def trimParquetSchema(requestedSchema: MessageType, fileSchema: MessageType): MessageType = {
    val trimmedFields = requestedSchema.getFields.asScala.map(field => {
      if (fileSchema.containsField(field.getName)) {
        trimParquetType(field, fileSchema.asGroupType().getType(field.getName))
      } else {
        Some(field)
      }
    }).filter(_.isDefined).map(_.get).toArray[Type]
    Types.buildMessage().addFields(trimmedFields: _*).named(requestedSchema.getName)
  }

  /**
   * Reorders variant group fields in the requested schema so that "value" precedes "metadata".
   * This works around Spark 4.0.x's ParquetUnshreddedVariantConverter, which builds its
   * converters array in hardcoded [value, metadata] order and indexes by schema position.
   * parquet-mr reconciles the requested schema against the file schema by field name,
   * so the correct bytes still flow to the correct converters regardless of file order.
   */
  def reorderVariantFields(schema: MessageType): MessageType = {
    val reordered = schema.getFields.asScala.map(reorderVariantType).toArray[Type]
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

  private def trimParquetType(requestedType: Type, fileType: Type): Option[Type] = {
    if (requestedType.equals(fileType)) {
      Some(requestedType)
    } else {
      requestedType match {
        case groupType: GroupType =>
          ValidationUtils.checkState(!fileType.isPrimitive,
            "Group type provided by requested schema but existing type in the file is a primitive")
          val fileTypeGroup = fileType.asGroupType()
          var hasMatchingField = false
          val fields = groupType.getFields.asScala.map(field => {
            if (fileTypeGroup.containsField(field.getName)) {
              hasMatchingField = true
              trimParquetType(field, fileType.asGroupType().getType(field.getName))
            } else {
              Some(field)
            }
          }).filter(_.isDefined).map(_.get).asJava
          if (hasMatchingField && !fields.isEmpty) {
            Some(groupType.withNewFields(fields))
          } else {
            None
          }
        case _ => Some(requestedType)
      }
    }
  }
}
