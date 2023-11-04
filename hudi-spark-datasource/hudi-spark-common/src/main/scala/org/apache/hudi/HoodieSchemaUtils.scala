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

import org.apache.avro.Schema
import org.apache.hudi.HoodieSparkSqlWriter.{CANONICALIZE_SCHEMA, SQL_MERGE_INTO_WRITES}
import org.apache.hudi.avro.AvroSchemaUtils.{isCompatibleProjectionOf, isSchemaCompatible, isValidEvolutionOf}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.HoodieAvroUtils.removeMetadataFields
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils.reconcileSchemaRequirements
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.{asScalaBuffer, mapAsJavaMap}

/**
 * Util methods for Schema evolution in Hudi
 */
object HoodieSchemaUtils {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * get latest internalSchema from table
   *
   * @param config          instance of {@link HoodieConfig}
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Option of InternalSchema. Will always be empty if schema on read is disabled
   */
  def getLatestTableInternalSchema(config: HoodieConfig,
                                   tableMetaClient: HoodieTableMetaClient): Option[InternalSchema] = {
    if (!config.getBooleanOrDefault(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
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
  def deduceWriterSchema(sourceSchema: Schema,
                         latestTableSchemaOpt: Option[Schema],
                         internalSchemaOpt: Option[InternalSchema],
                         opts: Map[String, String]): Schema = {
    val setNullForMissingColumns = opts.getOrDefault(DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS.key(),
      DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS.defaultValue).toBoolean
    val shouldReconcileSchema = opts(DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean
    val shouldValidateSchemasCompatibility = opts.getOrDefault(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key,
      HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue).toBoolean

    latestTableSchemaOpt match {
      // In case table schema is empty we're just going to use the source schema as a
      // writer's schema.
      case None => AvroInternalSchemaConverter.fixNullOrdering(sourceSchema)
      // Otherwise, we need to make sure we reconcile incoming and latest table schemas
      case Some(latestTableSchemaWithMetaFields) =>
        // NOTE: Meta-fields will be unconditionally injected by Hudi writing handles, for the sake of
        //       deducing proper writer schema we're stripping them to make sure we can perform proper
        //       analysis
        //add call to fix null ordering to ensure backwards compatibility
        val latestTableSchema = AvroInternalSchemaConverter.fixNullOrdering(removeMetadataFields(latestTableSchemaWithMetaFields))
        // Before validating whether schemas are compatible, we need to "canonicalize" source's schema
        // relative to the table's one, by doing a (minor) reconciliation of the nullability constraints:
        // for ex, if in incoming schema column A is designated as non-null, but it's designated as nullable
        // in the table's one we want to proceed aligning nullability constraints w/ the table's schema
        // Also, we promote types to the latest table schema if possible.
        val shouldCanonicalizeSchema = opts.getOrDefault(CANONICALIZE_SCHEMA.key,
          CANONICALIZE_SCHEMA.defaultValue.toString).toBoolean
        val mergeIntoWrites = opts.getOrDefault(SQL_MERGE_INTO_WRITES.key(),
          SQL_MERGE_INTO_WRITES.defaultValue.toString).toBoolean

        val canonicalizedSourceSchema = if (shouldCanonicalizeSchema) {
          canonicalizeSchema(sourceSchema, latestTableSchema, opts)
        } else {
          AvroInternalSchemaConverter.fixNullOrdering(sourceSchema)
        }

        val allowAutoEvolutionColumnDrop = opts.getOrDefault(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key,
          HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.defaultValue).toBoolean

        if (shouldReconcileSchema) {
          internalSchemaOpt match {
            case Some(internalSchema) =>
              // Apply schema evolution, by auto-merging write schema and read schema
              val mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema, internalSchema)
              val evolvedSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, latestTableSchema.getFullName)
              val shouldRemoveMetaDataFromInternalSchema = sourceSchema.getFields().filter(f => f.name().equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD)).isEmpty
              if (shouldRemoveMetaDataFromInternalSchema) HoodieAvroUtils.removeMetadataFields(evolvedSchema) else evolvedSchema
            case None =>
              // In case schema reconciliation is enabled we will employ (legacy) reconciliation
              // strategy to produce target writer's schema (see definition below)
              val (reconciledSchema, isCompatible) =
                reconcileSchemasLegacy(latestTableSchema, canonicalizedSourceSchema)

              // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
              //       w/ the table's one and allow schemas to diverge. This is required in cases where
              //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
              //       only incoming dataset's projection has to match the table's schema, and not the whole one
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
        } else {
          // In case reconciliation is disabled, we have to validate that the source's schema
          // is compatible w/ the table's latest schema, such that we're able to read existing table's
          // records using [[sourceSchema]].
          //
          // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
          //       w/ the table's one and allow schemas to diverge. This is required in cases where
          //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
          //       only incoming dataset's projection has to match the table's schema, and not the whole one

          if (mergeIntoWrites) {
            // if its merge into writes, do not check for projection nor schema compatibility. Writers down the line will
            // take care of it.
            canonicalizedSourceSchema
          } else {
            if (!shouldValidateSchemasCompatibility) {
              // if no validation is enabled, check for col drop
              if (allowAutoEvolutionColumnDrop) {
                canonicalizedSourceSchema
              } else {
                val reconciledSchema = if (setNullForMissingColumns) {
                  AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema, latestTableSchema)
                } else {
                  canonicalizedSourceSchema
                }
                if (isValidEvolutionOf(reconciledSchema, latestTableSchema)) {
                  reconciledSchema
                } else {
                  log.error(
                    s"""Incoming batch schema is not compatible with the table's one.
                       |Incoming schema ${sourceSchema.toString(true)}
                       |Incoming schema (canonicalized) ${reconciledSchema.toString(true)}
                       |Table's schema ${latestTableSchema.toString(true)}
                       |""".stripMargin)
                  throw new SchemaCompatibilityException("Incoming batch schema is not compatible with the table's one")
                }
              }
            } else if (isSchemaCompatible(latestTableSchema, canonicalizedSourceSchema, allowAutoEvolutionColumnDrop)) {
              canonicalizedSourceSchema
            } else {
              log.error(
                s"""Incoming batch schema is not compatible with the table's one.
                   |Incoming schema ${sourceSchema.toString(true)}
                   |Incoming schema (canonicalized) ${canonicalizedSourceSchema.toString(true)}
                   |Table's schema ${latestTableSchema.toString(true)}
                   |""".stripMargin)
              throw new SchemaCompatibilityException("Incoming batch schema is not compatible with the table's one")
            }
          }
        }
    }
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
  private def canonicalizeSchema(sourceSchema: Schema, latestTableSchema: Schema, opts : Map[String, String]): Schema = {
    reconcileSchemaRequirements(sourceSchema, latestTableSchema, opts)
  }


  private def reconcileSchemasLegacy(tableSchema: Schema, newSchema: Schema): (Schema, Boolean) = {
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
    if (isCompatibleProjectionOf(tableSchema, newSchema)) {
      // Picking table schema as a writer schema we need to validate that we'd be able to
      // rewrite incoming batch's data (written in new schema) into it
      (tableSchema, isSchemaCompatible(newSchema, tableSchema))
    } else {
      // Picking new schema as a writer schema we need to validate that we'd be able to
      // rewrite table's data into it
      (newSchema, isSchemaCompatible(tableSchema, newSchema))
    }
  }
}
