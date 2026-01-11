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
import org.apache.hudi.avro.AvroSchemaUtils.{isSchemaCompatible}
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieConfig, TypedProperties}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaCompatibility, HoodieSchemaUtils => HoodieCommonSchemaUtils}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.{ConfigUtils, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.{HoodieException, SchemaCompatibilityException}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils.reconcileSchemaRequirements

import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.slf4j.LoggerFactory

import java.util.Properties

import scala.collection.JavaConverters._

/**
 * Util methods for Schema evolution in Hudi
 */
object HoodieSchemaUtils {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Constants for Parquet-style accessor patterns used in nested MAP and ARRAY navigation.
   * These patterns are specifically used for column stats generation and differ from
   * InternalSchema constants which are used in schema evolution contexts.
   */
  private final val ARRAY_LIST = "list"
  private final val ARRAY_ELEMENT = "element"
  private final val ARRAY_SPARK = "array" // Spark writer uses this
  private final val MAP_KEY_VALUE = "key_value"
  private final val MAP_KEY = "key"
  private final val MAP_VALUE = "value"

  private final val ARRAY_LIST_ELEMENT = ARRAY_LIST + "." + ARRAY_ELEMENT
  private final val MAP_KEY_VALUE_KEY = MAP_KEY_VALUE + "." + MAP_KEY
  private final val MAP_KEY_VALUE_VALUE = MAP_KEY_VALUE + "." + MAP_VALUE

  /**
   * Advances offset past a component name in the path, handling end-of-path and dot separator.
   *
   * @param path      the full path string
   * @param offset    current position in path
   * @param component the component name to match (e.g., "element", "key", "value")
   * @return new offset after component and dot, or path.length() if at end, or -1 if no match
   */
  private def getNextOffset(path: String, offset: Int, component: String): Int = {
    if (!path.regionMatches(offset, component, 0, component.length)) {
      -1
    } else {
      val next = offset + component.length
      if (next == path.length) {
        next
      } else {
        if (path.charAt(next) == '.') next + 1 else -1
      }
    }
  }

  /**
   * Handles navigation into ARRAY types using the Avro ".list.element" pattern.
   *
   * @param arrayType  the ArrayType to navigate into
   * @param fullPath   the full field path string
   * @param offset     current position in fullPath (should point to start of "list")
   * @param prefix     the accumulated field path prefix
   * @param rootFieldName the name of the array field
   * @return Pair of canonical field name and the StructField
   */
  private def handleArrayNavigationAvro(arrayType: ArrayType, fullPath: String, offset: Int,
                                        prefix: String, rootFieldName: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    val elementType = arrayType.elementType
    val listOffset = getNextOffset(fullPath, offset, ARRAY_LIST)

    if (listOffset == -1) {
      throw new HoodieException(s"Array field requires .$ARRAY_LIST.$ARRAY_ELEMENT accessor pattern: $fullPath")
    }

    val elementOffset = getNextOffset(fullPath, listOffset, ARRAY_ELEMENT)
    if (elementOffset == -1) {
      throw new HoodieException(s"Array field requires .$ARRAY_LIST.$ARRAY_ELEMENT accessor pattern: $fullPath")
    }

    if (elementOffset == fullPath.length) {
      // Terminal case: just accessing the element type
      val elementField = StructField(ARRAY_ELEMENT, elementType, arrayType.containsNull)
      org.apache.hudi.common.util.collection.Pair.of(prefix + rootFieldName + "." + ARRAY_LIST_ELEMENT, elementField)
    } else if (elementType.isInstanceOf[StructType]) {
      // Recursive case: accessing fields within array elements
      getSchemaForFieldInternal(elementType.asInstanceOf[StructType], fullPath, elementOffset,
        prefix + rootFieldName + "." + ARRAY_LIST_ELEMENT + ".")
    } else {
      throw new HoodieException(s"Invalid array navigation pattern: $fullPath")
    }
  }

  /**
   * Handles navigation into ARRAY types using the Spark ".array" pattern.
   *
   * @param arrayType  the ArrayType to navigate into
   * @param fullPath   the full field path string
   * @param offset     current position in fullPath (should point to start of "array")
   * @param prefix     the accumulated field path prefix
   * @param rootFieldName the name of the array field
   * @return Pair of canonical field name and the StructField
   */
  private def handleArrayNavigationSpark(arrayType: ArrayType, fullPath: String, offset: Int,
                                         prefix: String, rootFieldName: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    val elementType = arrayType.elementType
    val sparkArrayOffset = getNextOffset(fullPath, offset, ARRAY_SPARK)

    if (sparkArrayOffset == -1) {
      throw new HoodieException(s"Array field requires .$ARRAY_SPARK accessor pattern: $fullPath")
    }

    if (sparkArrayOffset == fullPath.length) {
      // Terminal case: just accessing the element type
      val elementField = StructField(ARRAY_SPARK, elementType, arrayType.containsNull)
      org.apache.hudi.common.util.collection.Pair.of(prefix + rootFieldName + "." + ARRAY_SPARK, elementField)
    } else if (elementType.isInstanceOf[StructType]) {
      // Recursive case: accessing fields within array elements
      getSchemaForFieldInternal(elementType.asInstanceOf[StructType], fullPath, sparkArrayOffset,
        prefix + rootFieldName + "." + ARRAY_SPARK + ".")
    } else {
      throw new HoodieException(s"Invalid array navigation pattern: $fullPath")
    }
  }

  /**
   * Handles navigation into MAP types using the Parquet-style ".key_value.key" or ".key_value.value" patterns.
   *
   * @param mapType    the MapType to navigate into
   * @param fullPath   the full field path string
   * @param offset     current position in fullPath (should point to start of "key_value")
   * @param prefix     the accumulated field path prefix
   * @param rootFieldName the name of the map field
   * @return Pair of canonical field name and the StructField
   */
  private def handleMapNavigation(mapType: MapType, fullPath: String, offset: Int,
                                   prefix: String, rootFieldName: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    val keyValueOffset = getNextOffset(fullPath, offset, MAP_KEY_VALUE)
    if (keyValueOffset == -1) {
      throw new HoodieException(s"Invalid map navigation pattern: $fullPath. Expected .$MAP_KEY_VALUE_KEY or .$MAP_KEY_VALUE_VALUE")
    }

    // Check for .key pattern
    val keyOffset = getNextOffset(fullPath, keyValueOffset, MAP_KEY)
    if (keyOffset != -1) {
      // Accessing map keys (always the key type)
      val keyField = StructField(MAP_KEY, mapType.keyType, false)
      if (keyOffset == fullPath.length) {
        org.apache.hudi.common.util.collection.Pair.of(prefix + rootFieldName + "." + MAP_KEY_VALUE_KEY, keyField)
      } else {
        throw new HoodieException(s"Cannot navigate beyond map key: $fullPath")
      }
    } else {
      // Check for .value pattern
      val valueOffset = getNextOffset(fullPath, keyValueOffset, MAP_VALUE)
      if (valueOffset == -1) {
        throw new HoodieException(s"Invalid map navigation pattern: $fullPath. Expected .$MAP_KEY_VALUE_KEY or .$MAP_KEY_VALUE_VALUE")
      }

      val valueType = mapType.valueType
      if (valueOffset == fullPath.length) {
        // Terminal case: just accessing the value type
        val valueField = StructField(MAP_VALUE, valueType, mapType.valueContainsNull)
        org.apache.hudi.common.util.collection.Pair.of(prefix + rootFieldName + "." + MAP_KEY_VALUE_VALUE, valueField)
      } else if (valueType.isInstanceOf[StructType]) {
        // Recursive case: accessing fields within map values
        getSchemaForFieldInternal(valueType.asInstanceOf[StructType], fullPath, valueOffset,
          prefix + rootFieldName + "." + MAP_KEY_VALUE_VALUE + ".")
      } else {
        throw new HoodieException(s"Invalid map value navigation pattern: $fullPath")
      }
    }
  }

  /**
   * Internal helper method for retrieving nested fields using offset-based navigation.
   * This method uses offset arithmetic to navigate through the path string instead of
   * creating intermediate substring objects, improving performance and memory efficiency.
   *
   * @param schema   the schema to search in
   * @param fullPath the full field path string
   * @param offset   current position in fullPath
   * @param prefix   the accumulated field path prefix
   * @return Pair of canonical field name and the StructField
   * @throws HoodieException if field is not found or navigation pattern is invalid
   */
  private def getSchemaForFieldInternal(schema: StructType, fullPath: String, offset: Int, prefix: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    val nextDot = fullPath.indexOf('.', offset)

    // Terminal case: no more dots in this segment
    if (nextDot == -1) {
      val fieldName = fullPath.substring(offset)
      val fieldIndex = schema.fieldIndex(fieldName)
      val field = schema.fields(fieldIndex)
      org.apache.hudi.common.util.collection.Pair.of(prefix + field.name, field)
    } else {
      // Recursive case: more nesting to explore
      val rootFieldName = fullPath.substring(offset, nextDot)
      val rootFieldIndex = schema.fieldIndex(rootFieldName)
      val rootField = schema.fields(rootFieldIndex)
      if (rootField == null) {
        throw new HoodieException("Failed to find " + fullPath + " in the table schema ")
      }
      val nextOffset = nextDot + 1
      // Handle ARRAY type with .array pattern (Spark) or .list.element pattern (Avro)
      if (rootField.dataType.isInstanceOf[ArrayType]) {
        val arrayType = rootField.dataType.asInstanceOf[ArrayType]
        // Try Spark pattern first: .array
        val sparkArrayOffset = getNextOffset(fullPath, nextOffset, ARRAY_SPARK)
        if (sparkArrayOffset != -1) {
          handleArrayNavigationSpark(arrayType, fullPath, nextOffset, prefix, rootFieldName)
        } else {
          // Try Avro pattern: .list.element
          handleArrayNavigationAvro(arrayType, fullPath, nextOffset, prefix, rootFieldName)
        }
      }
      // Handle MAP type with .key_value.key or .key_value.value pattern
      else if (rootField.dataType.isInstanceOf[MapType]) {
        val mapType = rootField.dataType.asInstanceOf[MapType]
        handleMapNavigation(mapType, fullPath, nextOffset, prefix, rootFieldName)
      }
      // Handle standard STRUCT type
      else if (rootField.dataType.isInstanceOf[StructType]) {
        getSchemaForFieldInternal(rootField.dataType.asInstanceOf[StructType], fullPath, nextOffset, prefix + rootFieldName + ".")
      }
      else {
        throw new HoodieException(s"Unsupported field type for navigation: ${rootField.dataType} for field $fullPath")
      }
    }
  }

  def getSchemaForField(schema: StructType, fieldName: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    getSchemaForField(schema, fieldName, StringUtils.EMPTY_STRING)
  }

  def getSchemaForField(schema: StructType, fieldName: String, prefix: String): org.apache.hudi.common.util.collection.Pair[String, StructField] = {
    getSchemaForFieldInternal(schema, fieldName, 0, prefix)
  }

  /**
   * get latest internalSchema from table
   *
   * @param config          instance of {@link HoodieConfig}
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Option of InternalSchema. Will always be empty if schema on read is disabled
   */
  def getLatestTableInternalSchema(config: HoodieConfig,
                                   tableMetaClient: HoodieTableMetaClient): Option[InternalSchema] = {
    getLatestTableInternalSchema(config.getProps, tableMetaClient)
  }

  /**
   * get latest internalSchema from table
   *
   * @param props           instance of {@link Properties}
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Option of InternalSchema. Will always be empty if schema on read is disabled
   */
  def getLatestTableInternalSchema(props: Properties,
                                   tableMetaClient: HoodieTableMetaClient): Option[InternalSchema] = {
    if (!ConfigUtils.getBooleanWithAltKeys(props, DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
      None
    } else {
      try {
        val tableSchemaResolver = new TableSchemaResolver(tableMetaClient)
        val internalSchemaOpt = tableSchemaResolver.getTableInternalSchemaFromCommitMetadata
        if (internalSchemaOpt.isPresent) Some(internalSchemaOpt.get()) else None
      } catch {
        case _: Exception => None
      }
    }
  }

  /**
   * Deduces writer's schema based on
   * <ul>
   *   <li>Source's schema</li>
   *   <li>Target table's schema (including Hudi's [[InternalSchema]] representation)</li>
   * </ul>
   */
  def deduceWriterSchema(sourceSchema: HoodieSchema,
                         latestTableSchemaOpt: Option[HoodieSchema],
                         internalSchemaOpt: Option[InternalSchema],
                         opts: Map[String, String]): HoodieSchema = {
    latestTableSchemaOpt match {
      // If table schema is empty, then we use the source schema as a writer's schema.
      case None => InternalSchemaConverter.fixNullOrdering(sourceSchema)
      // Otherwise, we need to make sure we reconcile incoming and latest table schemas
      case Some(latestTableSchemaWithMetaFields) =>
        // NOTE: Meta-fields will be unconditionally injected by Hudi writing handles, for the sake of deducing proper writer schema
        //       we're stripping them to make sure we can perform proper analysis
        // add call to fix null ordering to ensure backwards compatibility
        val latestTableSchema = InternalSchemaConverter.fixNullOrdering(
          HoodieCommonSchemaUtils.removeMetadataFields(latestTableSchemaWithMetaFields))
        // Before validating whether schemas are compatible, we need to "canonicalize" source's schema
        // relative to the table's one, by doing a (minor) reconciliation of the nullability constraints:
        // for ex, if in incoming schema column A is designated as non-null, but it's designated as nullable
        // in the table's one we want to proceed aligning nullability constraints w/ the table's schema
        // Also, we promote types to the latest table schema if possible.
        val shouldCanonicalizeSchema = opts.getOrElse(CANONICALIZE_SCHEMA.key, CANONICALIZE_SCHEMA.defaultValue.toString).toBoolean
        val shouldReconcileSchema = opts.getOrElse(DataSourceWriteOptions.RECONCILE_SCHEMA.key(),
          DataSourceWriteOptions.RECONCILE_SCHEMA.defaultValue().toString).toBoolean
        val canonicalizedSourceSchema = if (shouldCanonicalizeSchema) {
          canonicalizeSchema(sourceSchema, latestTableSchema, opts, !shouldReconcileSchema)
        } else {
          InternalSchemaConverter.fixNullOrdering(sourceSchema)
        }

        if (shouldReconcileSchema) {
          deduceWriterSchemaWithReconcile(sourceSchema, canonicalizedSourceSchema, latestTableSchema, internalSchemaOpt, opts)
        } else {
          deduceWriterSchemaWithoutReconcile(sourceSchema, canonicalizedSourceSchema, latestTableSchema, opts)
        }
    }
  }

  /**
   * Deducing with disabled reconciliation.
   * We have to validate that the source's schema is compatible w/ the table's latest schema,
   * such that we're able to read existing table's records using [[sourceSchema]].
   */
  private def deduceWriterSchemaWithoutReconcile(sourceSchema: HoodieSchema,
                                                 canonicalizedSourceSchema: HoodieSchema,
                                                 latestTableSchema: HoodieSchema,
                                                 opts: Map[String, String]): HoodieSchema = {
    // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
    //       w/ the table's one and allow schemas to diverge. This is required in cases where
    //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
    //       only incoming dataset's projection has to match the table's schema, and not the whole one
    val mergeIntoWrites = opts.getOrElse(SQL_MERGE_INTO_WRITES.key(), SQL_MERGE_INTO_WRITES.defaultValue.toString).toBoolean
    val shouldValidateSchemasCompatibility = opts.getOrElse(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key,
      HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue).toBoolean
    val allowAutoEvolutionColumnDrop = opts.getOrElse(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key,
      HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.defaultValue).toBoolean
    val setNullForMissingColumns = opts.getOrElse(DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS.key(),
      DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS.defaultValue).toBoolean

    if (!mergeIntoWrites && !shouldValidateSchemasCompatibility && !allowAutoEvolutionColumnDrop) {
      // Default behaviour
      val reconciledSchema = if (setNullForMissingColumns) {
        HoodieSchema.fromAvroSchema(AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema.toAvroSchema(), latestTableSchema.toAvroSchema(), setNullForMissingColumns))
      } else {
        canonicalizedSourceSchema
      }
      HoodieSchemaCompatibility.checkValidEvolution(reconciledSchema, latestTableSchema)
      reconciledSchema
    } else {
      // If it's merge into writes, we don't check for projection nor schema compatibility. Writers down the line will take care of it.
      // Or it's not merge into writes, and we don't validate schema, but we allow to drop columns automatically.
      // Or it's not merge into writes, we validate schema, and schema is compatible.
      if (shouldValidateSchemasCompatibility) {
        HoodieSchemaCompatibility.checkSchemaCompatible(latestTableSchema, canonicalizedSourceSchema, true,
          allowAutoEvolutionColumnDrop, java.util.Collections.emptySet())
      }
      canonicalizedSourceSchema
    }
  }

  /**
   * Deducing with enabled reconciliation.
   * Marked as Deprecated.
   */
  private def deduceWriterSchemaWithReconcile(sourceSchema: HoodieSchema,
                                              canonicalizedSourceSchema: HoodieSchema,
                                              latestTableSchema: HoodieSchema,
                                              internalSchemaOpt: Option[InternalSchema],
                                              opts: Map[String, String]): HoodieSchema = {
    internalSchemaOpt match {
      case Some(internalSchema) =>
        // Apply schema evolution, by auto-merging write schema and read schema
        val setNullForMissingColumns = opts.getOrElse(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(),
          HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.defaultValue()).toBoolean
        val mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema.toAvroSchema(), internalSchema, setNullForMissingColumns)
        val evolvedSchema = InternalSchemaConverter.convert(mergedInternalSchema, latestTableSchema.getFullName)
        val shouldRemoveMetaDataFromInternalSchema = sourceSchema.getFields.asScala.filter(f => f.name().equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD)).isEmpty
        if (shouldRemoveMetaDataFromInternalSchema) HoodieCommonSchemaUtils.removeMetadataFields(evolvedSchema) else evolvedSchema

      case None =>
        // In case schema reconciliation is enabled we will employ (legacy) reconciliation
        // strategy to produce target writer's schema (see definition below)
        val (reconciledSchema, isCompatible) =
          reconcileSchemasLegacy(latestTableSchema, canonicalizedSourceSchema)

        // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
        //       w/ the table's one and allow schemas to diverge. This is required in cases where
        //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
        //       only incoming dataset's projection has to match the table's schema, and not the whole one
        val shouldValidateSchemasCompatibility = opts.getOrElse(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key, HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue).toBoolean
        if (!shouldValidateSchemasCompatibility || isCompatible) {
          reconciledSchema
        } else {
          log.error(
            s"""Failed to reconcile incoming batch schema with the table's one.
               |Incoming schema ${sourceSchema.toString(true)}
               |Incoming schema (canonicalized) ${canonicalizedSourceSchema.toString(true)}
               |Table's schema ${latestTableSchema.toString(true)}
               |""".stripMargin)
          throw new SchemaCompatibilityException("Failed to reconcile incoming schema with the table's one")
        }
    }
  }

  def deduceWriterSchema(sourceSchema: HoodieSchema,
                         latestTableSchemaOpt: org.apache.hudi.common.util.Option[HoodieSchema],
                         internalSchemaOpt: org.apache.hudi.common.util.Option[InternalSchema],
                         props: TypedProperties): HoodieSchema = {
    deduceWriterSchema(sourceSchema,
      HoodieConversionUtils.toScalaOption(latestTableSchemaOpt),
      HoodieConversionUtils.toScalaOption(internalSchemaOpt),
      HoodieConversionUtils.fromProperties(props))
  }

  /**
   * Canonicalizes [[sourceSchema]] by reconciling it w/ [[latestTableSchema]] in following
   *
   * <ol>
   *  <li>Nullability: making sure that nullability of the fields in the source schema is matching
   *  that of the latest table's ones</li>
   * </ol>
   *
   * TODO support casing reconciliation
   */
  private def canonicalizeSchema(sourceSchema: HoodieSchema, latestTableSchema: HoodieSchema, opts : Map[String, String],
                                 shouldReorderColumns: Boolean): HoodieSchema = {
    HoodieSchema.fromAvroSchema(
      reconcileSchemaRequirements(sourceSchema.toAvroSchema(), latestTableSchema.toAvroSchema(), shouldReorderColumns)
    )
  }


  private def reconcileSchemasLegacy(tableSchema: HoodieSchema, newSchema: HoodieSchema): (HoodieSchema, Boolean) = {
    // Legacy reconciliation implements following semantic
    //    - In case new-schema is a "compatible" projection of the existing table's one (projection allowing
    //      permitted type promotions), table's schema would be picked as (reconciled) writer's schema;
    //    - Otherwise, we'd fall back to picking new (batch's) schema as a writer's schema;
    //
    // Philosophically, such semantic aims at always choosing a "wider" schema, ie the one containing
    // the other one (schema A contains schema B, if schema B is a projection of A). This enables us,
    // to always "extend" the schema during schema evolution and hence never lose the data (when, for ex
    // existing column is being dropped in a new batch)
    //
    // NOTE: By default Hudi doesn't allow automatic schema evolution to drop the columns from the target
    //       table. However, when schema reconciliation is turned on, we would allow columns to be dropped
    //       in the incoming batch (as these would be reconciled in anyway)
    if (HoodieSchemaCompatibility.isCompatibleProjectionOf(tableSchema, newSchema)) {
      // Picking table schema as a writer schema we need to validate that we'd be able to
      // rewrite incoming batch's data (written in new schema) into it
      (tableSchema, HoodieSchemaCompatibility.isSchemaCompatible(newSchema, tableSchema))
    } else {
      // Picking new schema as a writer schema we need to validate that we'd be able to
      // rewrite table's data into it
      (newSchema, HoodieSchemaCompatibility.isSchemaCompatible(tableSchema, newSchema))
    }
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
