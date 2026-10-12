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

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.util.collection.Pair
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.schema.{MessageType, PrimitiveType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.execution.datasources.SparkSchemaTransformUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import java.util.Collections

import scala.collection.JavaConverters._

object HoodieParquetFileFormatHelper {

  /**
   * The conf keys `ParquetToSparkSchemaConverter(Configuration)` reads in the supported Spark versions. The file schema
   * conversion depends on the parquet schema, these values and [[SCHEMA_CONVERTER_SESSION_CONF_KEYS]] only.
   */
  private[parquet] val SCHEMA_CONVERTER_CONF_KEYS: Seq[String] = Seq(
    "spark.sql.parquet.binaryAsString",
    "spark.sql.parquet.int96AsTimestamp",
    "spark.sql.caseSensitive",
    "spark.sql.parquet.inferTimestampNTZ.enabled",
    "spark.sql.legacy.parquet.nanosAsLong",
    "spark.sql.parquet.fieldId.read.enabled",
    "spark.sql.parquet.ignoreVariantAnnotation",
    "spark.sql.parquet.reader.respectUnknownTypeAnnotation.enabled")

  /**
   * The session conf keys the converter reads through `SQLConf.get` in the supported Spark versions: Spark 4.1+ checks
   * whether shredded variants may be read when it converts a variant group. An unset or unregistered key reads as
   * null, so the same list serves every Spark version.
   */
  private[parquet] val SCHEMA_CONVERTER_SESSION_CONF_KEYS: Seq[String] = Seq(
    "spark.sql.variant.allowReadingShredded")

  private case class ImplicitSchemaChangeKey(fileSchema: MessageType, requiredSchema: StructType, converterConf: Seq[String])

  /** The implicit type changes by requested field index, and the schema to read the file with. */
  type ImplicitSchemaChange = (java.util.Map[Integer, Pair[DataType, DataType]], StructType)

  /** Bounds the cache by the parquet columns of the file schemas plus the leaf fields of the requested schemas. */
  private val MAX_CACHED_SCHEMA_FIELDS = 20000

  /** Bounds the number of entries, whatever their size: each entry weighs at least this much. */
  private[parquet] val MIN_ENTRY_WEIGHT: Int = MAX_CACHED_SCHEMA_FIELDS / 256

  /**
   * Reconciliations of file schemas with requested schemas in this JVM. The files of a table share a few schemas, so
   * the files of a scan mostly hit the same entries. An entry heavier than the whole bound is not kept, so a file schema
   * with more than 20000 columns is reconciled per file, as before.
   */
  private val implicitSchemaChangeCache: Cache[ImplicitSchemaChangeKey, ImplicitSchemaChange] =
    Caffeine.newBuilder()
      .maximumWeight(MAX_CACHED_SCHEMA_FIELDS)
      .weigher[ImplicitSchemaChangeKey, ImplicitSchemaChange]((key, _) => entryWeight(key.fileSchema, key.requiredSchema))
      .build()

  private[parquet] def entryWeight(fileSchema: MessageType, requiredSchema: StructType): Int =
    Math.max(MIN_ENTRY_WEIGHT, fileSchema.getColumns.size() + leafCount(requiredSchema))

  /**
   * Returns the implicit type changes between the file's schema and `requiredSchema`, and the schema to read the file
   * with. Cached by the parquet file schema, the requested schema and the conf values the schema conversion reads; the
   * returned map is shared, so it is read-only. A result that relied on a failed adapter check is not cached.
   */
  def buildImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                    parquetFileMetaData: FileMetaData,
                                    requiredSchema: StructType): ImplicitSchemaChange = {
    val sessionConf = SQLConf.get
    val key = ImplicitSchemaChangeKey(parquetFileMetaData.getSchema, requiredSchema,
      SCHEMA_CONVERTER_CONF_KEYS.map(key => hadoopConf.get(key))
        ++ SCHEMA_CONVERTER_SESSION_CONF_KEYS.map(key => sessionConf.getConfString(key, null)))
    val cached = implicitSchemaChangeCache.getIfPresent(key)
    if (cached != null) {
      cached
    } else {
      val adapterFailures = SparkSchemaTransformUtils.adapterCheckFailureCount
      val result = computeImplicitSchemaChangeInfo(hadoopConf, key.fileSchema, key.requiredSchema)
      if (SparkSchemaTransformUtils.adapterCheckFailureCount == adapterFailures) {
        implicitSchemaChangeCache.put(key, result)
      }
      result
    }
  }

  private def leafCount(dataType: DataType): Int = dataType match {
    case struct: StructType => struct.fields.map(field => leafCount(field.dataType)).sum
    case array: ArrayType => leafCount(array.elementType)
    case map: MapType => leafCount(map.keyType) + leafCount(map.valueType)
    case _ => 1
  }

  private def computeImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                             fileSchema: MessageType,
                                             requiredSchema: StructType): ImplicitSchemaChange = {
    val fileStruct = convertParquetSchemaToSparkSchema(hadoopConf, fileSchema)
    val (implicitTypeChangeInfo, sparkRequestSchema) = SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileStruct, requiredSchema)
    (Collections.unmodifiableMap(implicitTypeChangeInfo), sparkRequestSchema)
  }

  def convertParquetSchemaToSparkSchema(hadoopConf: Configuration, schema: MessageType): StructType = {
    // Spark 3.3's ParquetToSparkSchemaConverter throws "Illegal Parquet type: FIXED_LEN_BYTE_ARRAY"
    // for unannotated FLBA columns (e.g., Hudi VECTOR type). This was fixed in Spark 3.4
    // (SPARK-41096 / https://github.com/apache/spark/pull/38628) which maps bare FLBA to BinaryType.
    // On Spark 3.3 only, rewrite bare FLBA to BINARY before conversion.
    val safeSchema = if (!HoodieSparkUtils.gteqSpark3_4) {
      rewriteFixedLenByteArrayToBinary(schema)
    } else {
      schema
    }

    val convert = new ParquetToSparkSchemaConverter(hadoopConf)
    convert.convert(safeSchema)
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
