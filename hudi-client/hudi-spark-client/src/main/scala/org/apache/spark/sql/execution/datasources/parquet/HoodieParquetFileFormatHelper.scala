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

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.sql.HoodieSchemaUtils
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.types.{DataType, StructField, StructType}

object HoodieParquetFileFormatHelper {
  
  def buildImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                    parquetFileMetaData: FileMetaData,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    // Convert Parquet schema to Spark StructType
    val convert = new ParquetToSparkSchemaConverter(hadoopConf)
    val fileStruct = convert.convert(parquetFileMetaData.getSchema)
    SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileStruct, requiredSchema)
  }

  def generateUnsafeProjection(fullSchema: Seq[Attribute],
                               timeZoneId: Option[String],
                               typeChangeInfos: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]],
                               requiredSchema: StructType,
                               partitionSchema: StructType,
                               schemaUtils: HoodieSchemaUtils): UnsafeProjection = {
    if (typeChangeInfos.isEmpty) {
      GenerateUnsafeProjection.generate(fullSchema, fullSchema)
    } else {
      // find type changed.
      val newSchema = new StructType(requiredSchema.fields.zipWithIndex.map { case (f, i) =>
        if (typeChangeInfos.containsKey(i)) {
          StructField(f.name, typeChangeInfos.get(i).getRight, f.nullable, f.metadata)
        } else f
      })
      val newFullSchema = schemaUtils.toAttributes(newSchema) ++ schemaUtils.toAttributes(partitionSchema)
      val castSchema = newFullSchema.zipWithIndex.map { case (attr, i) =>
        if (typeChangeInfos.containsKey(i)) {
          val srcType = typeChangeInfos.get(i).getRight
          val dstType = typeChangeInfos.get(i).getLeft
          // Delegate to format-agnostic utility for type casting (without padding)
          SparkSchemaTransformUtils.recursivelyCastExpressions(
            attr, srcType, dstType, timeZoneId, padNestedFields = false
          )
        } else attr
      }
      GenerateUnsafeProjection.generate(castSchema, newFullSchema)
    }
  }
}
