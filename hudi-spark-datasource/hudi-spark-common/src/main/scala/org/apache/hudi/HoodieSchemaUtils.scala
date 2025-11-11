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

package org.apache.hudi

import org.apache.hudi.HoodieSparkSqlWriter.{CANONICALIZE_SCHEMA, SQL_MERGE_INTO_WRITES}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.util.HoodieSchemaUtil

import org.apache.avro.Schema
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Util methods for Schema evolution in Hudi
 */
object HoodieSchemaUtils {
  private val log = LoggerFactory.getLogger(getClass)

  def getSchemaForField(schema: StructType, fieldName: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    getSchemaForField(schema, fieldName, StringUtils.EMPTY_STRING)
  }

  def getSchemaForField(schema: StructType, fieldName: String, prefix: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    if (!(fieldName.contains("."))) {
      org.apache.hudi.common.util.collection.Pair.of(prefix + schema.fields(schema.fieldIndex(fieldName)).name, schema.fields(schema.fieldIndex(fieldName)))
    }
    else {
      val rootFieldIndex: Int = fieldName.indexOf(".")
      val rootField: StructField = schema.fields(schema.fieldIndex(fieldName.substring(0, rootFieldIndex)))
      if (rootField == null) {
        throw new HoodieException("Failed to find " + fieldName + " in the table schema ")
      }
      getSchemaForField(rootField.dataType.asInstanceOf[StructType], fieldName.substring(rootFieldIndex + 1), prefix + fieldName.substring(0, rootFieldIndex + 1))
    }
  }

  /**
   * Deduces writer's schema based on
   * <ul>
   *   <li>Source's schema</li>
   *   <li>Target table's schema (including Hudi's [[InternalSchema]] representation)</li>
   * </ul>
   */
  def deduceWriterSchema(sourceSchema: Schema,
                         latestTableSchemaOpt: Option[Schema],
                         internalSchemaOpt: Option[InternalSchema],
                         opts: Map[String, String]): Schema = {
    // Before validating whether schemas are compatible, we need to "canonicalize" source's schema
    // relative to the table's one, by doing a (minor) reconciliation of the nullability constraints:
    // for ex, if in incoming schema column A is designated as non-null, but it's designated as nullable
    // in the table's one we want to proceed aligning nullability constraints w/ the table's schema
    // Also, we promote types to the latest table schema if possible.
    val shouldCanonicalizeSchema = opts.getOrElse(CANONICALIZE_SCHEMA.key, CANONICALIZE_SCHEMA.defaultValue.toString).toBoolean
    // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
    //       w/ the table's one and allow schemas to diverge. This is required in cases where
    //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
    //       only incoming dataset's projection has to match the table's schema, and not the whole one
    val mergeIntoWrites = opts.getOrElse(SQL_MERGE_INTO_WRITES.key(), SQL_MERGE_INTO_WRITES.defaultValue.toString).toBoolean
    HoodieSchemaUtil.deduceWriterSchema(sourceSchema, HoodieConversionUtils.toJavaOption(latestTableSchemaOpt),
      HoodieConversionUtils.toJavaOption(internalSchemaOpt), mergeIntoWrites, shouldCanonicalizeSchema, opts.asJava)
  }

  def deduceWriterSchema(sourceSchema: Schema,
                         latestTableSchemaOpt: org.apache.hudi.common.util.Option[Schema],
                         internalSchemaOpt: org.apache.hudi.common.util.Option[InternalSchema],
                         props: TypedProperties): Schema = {
    deduceWriterSchema(sourceSchema,
      HoodieConversionUtils.toScalaOption(latestTableSchemaOpt),
      HoodieConversionUtils.toScalaOption(internalSchemaOpt),
      HoodieConversionUtils.fromProperties(props))
  }

  /**
   * Check if the partition schema fields order matches the table schema fields order.
   *
   * @param tableSchema      The table schema
   * @param partitionFields  The partition fields
   */
  def checkPartitionSchemaOrder(tableSchema: StructType, partitionFields: Seq[String]): Unit = {
    val tableSchemaFields = tableSchema.fields.map(_.name)
    // It is not allowed to specify partition columns when the table schema is not defined.
    // https://spark.apache.org/docs/latest/sql-error-conditions.html#specify_partition_is_not_allowed
    if (tableSchemaFields.isEmpty && partitionFields.nonEmpty) {
      throw new IllegalArgumentException("It is not allowed to specify partition columns when the table schema is not defined.")
    }
    // Filter the table schema fields to get the partition field names in order
    val tableSchemaPartitionFields = tableSchemaFields.filter(partitionFields.contains).toSeq
    if (tableSchemaPartitionFields != partitionFields) {
      throw new IllegalArgumentException(s"Partition schema fields order does not match the table schema fields order," +
        s" tableSchemaFields: $tableSchemaPartitionFields, partitionFields: $partitionFields.")
    }
  }
}
