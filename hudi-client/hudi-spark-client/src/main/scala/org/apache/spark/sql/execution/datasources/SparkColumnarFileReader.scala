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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StorageConfiguration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

trait SparkColumnarFileReader extends Serializable {
  /**
   * Read an individual parquet file
   *
   * @param file              parquet file to read
   * @param requiredSchema    desired output schema of the data
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  def read(file: PartitionedFile,
           requiredSchema: StructType,
           partitionSchema: StructType,
           internalSchemaOpt: util.Option[InternalSchema],
           filters: Seq[Filter],
           storageConf: StorageConfiguration[Configuration]): Iterator[InternalRow]
}

object SparkColumnarFileReader {
  /**
   * The requiredSchema may have columns that are not present in the actual data schema.
   * If all the requested fields of a nested record are not present in the data schema, then this field must be
   * excluded from the requiredSchema to prevent runtime errors.
   * @param requiredSchema the schema required by the query
   * @param dataSchema the current file's data schema
   * @return trimmed schema that can be safely applied to the data
   */
  def trimSchema(requiredSchema: StructType, dataSchema: StructType): StructType = {
    trimSchemaInternal(requiredSchema, dataSchema) match {
      case Some(schema) => schema
      case None => throw new IllegalStateException("Trimmed schema cannot be empty")
    }
  }

  def trimSchemaInternal(requiredSchema: StructType, dataSchema: StructType): Option[StructType] = {
    if (requiredSchema.equals(dataSchema)) {
      Some(requiredSchema)
    } else {
      val fields = requiredSchema.fields.map(field => {
        val dataSchemaFieldOpt = dataSchema.find(_.name.equals(field.name))
        if (dataSchemaFieldOpt.isEmpty) {
         None
        } else {
          val dataSchemaField = dataSchemaFieldOpt.get
          field.dataType match {
            case structType: StructType =>
              val trimmed = trimSchemaInternal(structType, dataSchemaField.dataType.asInstanceOf[StructType])
              trimmed.map(structType => field.copy(dataType = structType))
            case arrayType: ArrayType =>
              val elementType = arrayType.elementType
              val dataElementType = dataSchemaField.dataType.asInstanceOf[ArrayType].elementType
              elementType match {
                case structElementType: StructType if dataElementType.isInstanceOf[StructType] =>
                  val trimmed = trimSchemaInternal(structElementType, dataElementType.asInstanceOf[StructType])
                  trimmed.map(structType => field.copy(dataType = ArrayType(structType, arrayType.containsNull)))
                case _ => Some(field)
              }
            case mapType: MapType =>
              val valueType = mapType.valueType
              val dataValueType = dataSchemaField.dataType.asInstanceOf[MapType].valueType
              valueType match {
                case structValueType: StructType if dataValueType.isInstanceOf[StructType] =>
                  val trimmed = trimSchemaInternal(structValueType, dataValueType.asInstanceOf[StructType])
                  trimmed.map(structType => field.copy(dataType = MapType(mapType.keyType, structType, mapType.valueContainsNull)))
                case _ => Some(field)
              }
            case _ => Some(field)
          }
        }
      }).filter(_.isDefined).map(_.get)
      if (fields.length > 0) {
        Some(StructType(fields))
      } else {
        None
      }
    }
  }
}
