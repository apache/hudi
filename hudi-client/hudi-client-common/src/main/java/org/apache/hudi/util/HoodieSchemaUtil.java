/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.avro.AvroSchemaUtils.checkSchemaCompatible;
import static org.apache.hudi.avro.AvroSchemaUtils.checkValidEvolution;
import static org.apache.hudi.avro.AvroSchemaUtils.isCompatibleProjectionOf;
import static org.apache.hudi.avro.AvroSchemaUtils.isSchemaCompatible;
import static org.apache.hudi.avro.HoodieAvroUtils.removeMetadataFields;
import static org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils.reconcileSchemaRequirements;

/**
 * Util methods for Schema evolution in Hudi
 */
public class HoodieSchemaUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSchemaUtil.class);

  /**
   * Get latest internalSchema from table
   *
   * @param props           instance of Properties
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Optional of InternalSchema. Will always be empty if schema on read is disabled
   */
  public static Option<InternalSchema> getLatestTableInternalSchema(Properties props,
                                                                    HoodieTableMetaClient tableMetaClient) {
    if (!ConfigUtils.getBooleanWithAltKeys(props, HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE)) {
      return Option.empty();
    } else {
      try {
        TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(tableMetaClient);
        Option<InternalSchema> internalSchemaOpt = tableSchemaResolver.getTableInternalSchemaFromCommitMetadata();
        return internalSchemaOpt.isPresent() ? internalSchemaOpt : Option.empty();
      } catch (Exception e) {
        return Option.empty();
      }
    }
  }

  /**
   * get latest internalSchema from table
   *
   * @param config          instance of {@link HoodieConfig}
   * @param tableMetaClient instance of HoodieTableMetaClient
   * @return Option of InternalSchema. Will always be empty if schema on read is disabled
   */
  public static Option<InternalSchema> getLatestTableInternalSchema(HoodieConfig config,
                                                                    HoodieTableMetaClient tableMetaClient) {
    return getLatestTableInternalSchema(config.getProps(), tableMetaClient);
  }

  /**
   * Deduces writer's schema based on
   * <ul>
   *   <li>Source's schema</li>
   *   <li>Target table's schema (including Hudi's InternalSchema representation)</li>
   * </ul>
   */
  public static Schema deduceWriterSchema(Schema sourceSchema,
                                          Option<Schema> latestTableSchemaOpt,
                                          Option<InternalSchema> internalSchemaOpt,
                                          boolean isMergeIntoWrite,
                                          boolean shouldCanonicalizeSchema,
                                          Map<String, String> opts) {
    if (latestTableSchemaOpt.isEmpty()) {
      // If table schema is empty, then we use the source schema as a writer's schema.
      return AvroInternalSchemaConverter.fixNullOrdering(sourceSchema);
    } else {
      // Otherwise, we need to make sure we reconcile incoming and latest table schemas
      // NOTE: Meta-fields will be unconditionally injected by Hudi writing handles, for the sake of deducing proper writer schema
      //       we're stripping them to make sure we can perform proper analysis
      Schema latestTableSchemaWithMetaFields = latestTableSchemaOpt.get();
      Schema latestTableSchema = AvroInternalSchemaConverter.fixNullOrdering(removeMetadataFields(latestTableSchemaWithMetaFields));

      boolean shouldReconcileSchema = Boolean.parseBoolean(
          opts.getOrDefault(HoodieCommonConfig.RECONCILE_SCHEMA.key(),
              Boolean.toString(HoodieCommonConfig.RECONCILE_SCHEMA.defaultValue())));

      // Before validating whether schemas are compatible, we need to "canonicalize" source's schema
      // relative to the table's one, by doing a (minor) reconciliation of the nullability constraints:
      // for ex, if in incoming schema column A is designated as non-null, but it's designated as nullable
      // in the table's one we want to proceed aligning nullability constraints w/ the table's schema
      // Also, we promote types to the latest table schema if possible.
      Schema canonicalizedSourceSchema;
      if (shouldCanonicalizeSchema) {
        canonicalizedSourceSchema = canonicalizeSchema(sourceSchema, latestTableSchema, !shouldReconcileSchema);
      } else {
        canonicalizedSourceSchema = AvroInternalSchemaConverter.fixNullOrdering(sourceSchema);
      }

      if (shouldReconcileSchema) {
        return deduceWriterSchemaWithReconcile(sourceSchema, canonicalizedSourceSchema, latestTableSchema, internalSchemaOpt, opts);
      } else {
        return deduceWriterSchemaWithoutReconcile(sourceSchema, canonicalizedSourceSchema, latestTableSchema, isMergeIntoWrite, opts);
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
  private static Schema canonicalizeSchema(Schema sourceSchema, Schema latestTableSchema, boolean shouldReorderColumns) {
    return reconcileSchemaRequirements(sourceSchema, latestTableSchema, shouldReorderColumns);
  }

  private static Schema deduceWriterSchemaWithReconcile(Schema sourceSchema,
                                                        Schema canonicalizedSourceSchema,
                                                        Schema latestTableSchema,
                                                        Option<InternalSchema> internalSchemaOpt,
                                                        Map<String, String> opts) {
    if (internalSchemaOpt.isPresent()) {
      InternalSchema internalSchema = internalSchemaOpt.get();
      // Apply schema evolution, by auto-merging write schema and read schema
      boolean setNullForMissingColumns = Boolean.parseBoolean(
          opts.getOrDefault(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(),
              HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.defaultValue())
      );
      InternalSchema mergedInternalSchema = AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema, internalSchema, setNullForMissingColumns);
      Schema evolvedSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, latestTableSchema.getFullName());

      boolean shouldRemoveMetaDataFromInternalSchema = sourceSchema.getFields().stream()
          .noneMatch(f -> f.name().equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD));

      if (shouldRemoveMetaDataFromInternalSchema) {
        return HoodieAvroUtils.removeMetadataFields(evolvedSchema);
      } else {
        return evolvedSchema;
      }
    } else {
      // In case schema reconciliation is enabled we will employ (legacy) reconciliation
      // strategy to produce target writer's schema (see definition below)
      Pair<Schema, Boolean> reconciledResult = reconcileSchemasLegacy(latestTableSchema, canonicalizedSourceSchema);
      Schema reconciledSchema = reconciledResult.getLeft();
      boolean isCompatible = reconciledResult.getRight();

      // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
      //       w/ the table's one and allow schemas to diverge. This is required in cases where
      //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
      //       only incoming dataset's projection has to match the table's schema, and not the whole one
      boolean shouldValidateSchemasCompatibility = Boolean.parseBoolean(
          opts.getOrDefault(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key(),
              HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue())
      );

      if (!shouldValidateSchemasCompatibility || isCompatible) {
        return reconciledSchema;
      } else {
        LOG.error("Failed to reconcile incoming batch schema with the table's one.\n"
            + "Incoming schema {}\n"
            + "Incoming schema (canonicalized) {}\n"
            + "Table's schema {}\n", sourceSchema.toString(true), canonicalizedSourceSchema.toString(true), latestTableSchema.toString(true));
        throw new SchemaCompatibilityException("Failed to reconcile incoming schema with the table's one");
      }
    }
  }

  private static Schema deduceWriterSchemaWithoutReconcile(Schema sourceSchema,
                                                           Schema canonicalizedSourceSchema,
                                                           Schema latestTableSchema,
                                                           boolean isMergeIntoWrite,
                                                           Map<String, String> opts) {
    // NOTE: In some cases we need to relax constraint of incoming dataset's schema to be compatible
    //       w/ the table's one and allow schemas to diverge. This is required in cases where
    //       partial updates will be performed (for ex, `MERGE INTO` Spark SQL statement) and as such
    //       only incoming dataset's projection has to match the table's schema, and not the whole one
    boolean shouldValidateSchemasCompatibility = Boolean.parseBoolean(
        opts.getOrDefault(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key(),
            HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.defaultValue()));
    boolean allowAutoEvolutionColumnDrop = Boolean.parseBoolean(
        opts.getOrDefault(HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.key(),
            HoodieWriteConfig.SCHEMA_ALLOW_AUTO_EVOLUTION_COLUMN_DROP.defaultValue()));
    boolean setNullForMissingColumns = Boolean.parseBoolean(
        opts.getOrDefault(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(),
            HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.defaultValue()));

    if (!isMergeIntoWrite && !shouldValidateSchemasCompatibility && !allowAutoEvolutionColumnDrop) {
      // Default behaviour
      Schema reconciledSchema;
      if (setNullForMissingColumns) {
        reconciledSchema = AvroSchemaEvolutionUtils.reconcileSchema(canonicalizedSourceSchema, latestTableSchema, true);
      } else {
        reconciledSchema = canonicalizedSourceSchema;
      }
      checkValidEvolution(reconciledSchema, latestTableSchema);
      return reconciledSchema;
    } else {
      // If it's merge into writes, we don't check for projection nor schema compatibility. Writers down the line will take care of it.
      // Or it's not merge into writes, and we don't validate schema, but we allow to drop columns automatically.
      // Or it's not merge into writes, we validate schema, and schema is compatible.
      if (shouldValidateSchemasCompatibility) {
        checkSchemaCompatible(latestTableSchema, canonicalizedSourceSchema, true,
            allowAutoEvolutionColumnDrop, Collections.emptySet());
      }
      return canonicalizedSourceSchema;
    }
  }

  private static Pair<Schema, Boolean> reconcileSchemasLegacy(Schema tableSchema, Schema newSchema) {
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
      return Pair.of(tableSchema, isSchemaCompatible(newSchema, tableSchema));
    } else {
      // Picking new schema as a writer schema we need to validate that we'd be able to
      // rewrite table's data into it
      return Pair.of(newSchema, isSchemaCompatible(tableSchema, newSchema));
    }
  }
}
