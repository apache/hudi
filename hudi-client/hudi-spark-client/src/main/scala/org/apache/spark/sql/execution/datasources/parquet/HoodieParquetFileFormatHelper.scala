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
import org.apache.hudi.HoodieSparkUtils
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.{MessageType, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConverters._

object HoodieParquetFileFormatHelper {

  def buildImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                    parquetFileMetaData: FileMetaData,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val originalSchema = parquetFileMetaData.getSchema

    // Spark 3.3's ParquetToSparkSchemaConverter throws "Illegal Parquet type: FIXED_LEN_BYTE_ARRAY"
    // for unannotated FLBA columns (e.g., Hudi VECTOR type). This was fixed in Spark 3.4
    // (SPARK-41096 / https://github.com/apache/spark/pull/38628) which maps bare FLBA to BinaryType.
    // On Spark 3.3 only, rewrite bare FLBA to BINARY before conversion.
    val safeSchema = if (!HoodieSparkUtils.gteqSpark3_4) {
      rewriteFixedLenByteArrayToBinary(originalSchema)
    } else {
      originalSchema
    }

    val convert = new ParquetToSparkSchemaConverter(hadoopConf)
    val fileStruct = convert.convert(safeSchema)
    SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileStruct, requiredSchema)
  }

  /**
   * Rewrites bare FIXED_LEN_BYTE_ARRAY columns (no logical type annotation) to BINARY.
   * Columns with annotations (e.g., DECIMAL, UUID) are left untouched.
   */
  private def rewriteFixedLenByteArrayToBinary(schema: MessageType): MessageType = {
    val fields = schema.getFields.asScala.map {
      case pt: PrimitiveType
        if pt.getPrimitiveTypeName == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
          && pt.getLogicalTypeAnnotation == null =>
        Types.primitive(PrimitiveTypeName.BINARY, pt.getRepetition).named(pt.getName)
      case other => other
    }
    new MessageType(schema.getName, fields.asJava)
  }
}
