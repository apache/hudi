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
import org.apache.parquet.schema.{GroupType, MessageType, SchemaRepair, Type, Types}
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
    // Same condition as Spark's own intersectParquetGroups: only the row-based reader wants a
    // requested schema restricted to what the file has; the vectorized reader null-fills a missing
    // column itself and matches columns by position.
    val trimmedParquetSchema = HoodieParquetReadSupport.trimParquetSchema(requestedParquetSchema,
      context.getFileSchema, dropMissingTopLevelFields = !enableVectorizedReader)
    new ReadContext(trimmedParquetSchema, readContext.getReadSupportMetadata)
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
   * @param dropMissingTopLevelFields whether a top-level field the file does not have is dropped as
   *                                  well (the row-based reader) or kept (the vectorized reader)
   * @return a potentially updated schema with empty struct fields removed
   */
  def trimParquetSchema(requestedSchema: MessageType,
                        fileSchema: MessageType,
                        dropMissingTopLevelFields: Boolean): MessageType = {
    val trimmedFields = requestedSchema.getFields.asScala.map(field => {
      if (fileSchema.containsField(field.getName)) {
        trimParquetType(field, fileSchema.asGroupType().getType(field.getName))
      } else if (dropMissingTopLevelFields) {
        // A top-level column the file does not have (added by DDL, or by a later write) is dropped
        // from the parquet read schema like a nested one is below: ParquetRowConverter builds its
        // converters per PARQUET field and leaves the catalyst columns it never sees null, which is
        // what Spark's own reader gets from intersectParquetGroups when nested schema pruning is on
        // (Hudi's readers switch it off). Keeping the field instead hands the converter a group
        // synthesised from the catalyst type, and for a Spark 4.1 variant column requested as the
        // PushVariantIntoScan projection struct that is a plain group the variant converter rejects
        // (INVALID_VARIANT_SHREDDING_SCHEMA, #20135).
        None
      } else {
        Some(field)
      }
    }).filter(_.isDefined).map(_.get).toArray[Type]
    Types.buildMessage().addFields(trimmedFields: _*).named(requestedSchema.getName)
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
              // Field exists in the requested schema but not in the file.
              // Exclude it from the Parquet read schema; Spark's schema evolution
              // will fill it with null in the output row.
              None
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
