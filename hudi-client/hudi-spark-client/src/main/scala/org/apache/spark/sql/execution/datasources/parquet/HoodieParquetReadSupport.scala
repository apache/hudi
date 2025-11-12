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
    val trimmedParquetSchema = HoodieParquetReadSupport.trimParquetSchema(requestedParquetSchema, context.getFileSchema)
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
