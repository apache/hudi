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

import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, Type, Types}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec

import java.time.ZoneId

import scala.collection.JavaConverters._

class HoodieParquetReadSupport(
                                convertTz: Option[ZoneId],
                                enableVectorizedReader: Boolean,
                                datetimeRebaseSpec: RebaseSpec,
                                int96RebaseSpec: RebaseSpec)
  extends ParquetReadSupport(convertTz, enableVectorizedReader, datetimeRebaseSpec, int96RebaseSpec) {

  override def init(context: InitContext): ReadContext = {
    val readContext = super.init(context)
    val requestedParquetSchema = readContext.getRequestedSchema
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

  private def trimParquetType(parquetType: Type, fileType: Type): Option[Type] = {
    if (parquetType.equals(fileType)) {
      Some(parquetType)
    } else {
      parquetType match {
        case groupType: GroupType =>
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
          if (groupType.getLogicalTypeAnnotation == LogicalTypeAnnotation.mapType() && !fields.isEmpty) {
            if (fields.get(0).asGroupType().getFields.size() == 2) {
              // Map type must have exactly 2 fields: key and value
              Some(groupType.withNewFields(fields))
            } else {
              None
            }
          } else if (hasMatchingField && !fields.isEmpty) {
            Some(groupType.withNewFields(fields))
          } else {
            None
          }
        case _ => Some(parquetType)
      }
    }
  }
}
