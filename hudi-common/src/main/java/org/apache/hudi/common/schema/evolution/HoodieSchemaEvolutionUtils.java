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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.types.Type;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HoodieSchema-shaped façade for write-path schema evolution: reconciling an
 * incoming write schema against the table's current schema, including missing
 * columns, added columns, type promotions, and nullability adjustments.
 *
 * <p>Mirrors the two entry points of {@link AvroSchemaEvolutionUtils} but consumes
 * and produces {@link HoodieSchema} so callers can stay off direct
 * {@code InternalSchema} usage. During the InternalSchema → HoodieSchema migration
 * this delegates to the legacy implementation via
 * {@link HoodieSchemaInternalSchemaBridge}, which preserves field ids end-to-end.
 * Phase 5 rewrites the implementation in pure HoodieSchema terms behind this stable
 * interface.</p>
 */
public final class HoodieSchemaEvolutionUtils {

  private HoodieSchemaEvolutionUtils() {
  }

  /**
   * Reconciles an incoming write schema against the existing table schema, adding
   * any new columns, promoting types where allowed, and (optionally) marking
   * missing columns as nullable.
   *
   * <p>Semantics match {@link AvroSchemaEvolutionUtils#reconcileSchema(org.apache.avro.Schema, InternalSchema, boolean)}:
   * the incoming schema is assumed to have <i>missing</i> columns rather than
   * <i>deleted</i> columns. Renames and explicit deletes are not inferred here —
   * those are handled by the explicit DDL path through
   * {@link HoodieSchemaChangeApplier}.</p>
   *
   * @param incomingSchema             incoming write schema
   * @param oldTableSchema             current table schema (with field ids)
   * @param makeMissingFieldsNullable  when true, table fields absent from the
   *                                   incoming schema are marked nullable in the
   *                                   reconciled result
   * @return reconciled HoodieSchema with field ids preserved on unchanged columns
   */
  public static HoodieSchema reconcileSchema(HoodieSchema incomingSchema,
                                             HoodieSchema oldTableSchema,
                                             boolean makeMissingFieldsNullable) {
    InternalSchema oldInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(oldTableSchema);
    InternalSchema reconciled = AvroSchemaEvolutionUtils.reconcileSchema(
        incomingSchema.getAvroSchema(), oldInternal, makeMissingFieldsNullable);
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(reconciled, oldTableSchema.getFullName());
  }

  /**
   * Avro-only sibling of {@link #reconcileSchema(HoodieSchema, HoodieSchema, boolean)}
   * that does <em>not</em> route through the InternalSchema bridge — field ids are
   * neither read from the inputs nor stamped on the output. Use this from the
   * write-path's structural reconciliation (e.g. {@code deduceWriterSchema}) where
   * carrying ids over from the table's evolution-schema would leak them into
   * commit metadata and Parquet writes that historically didn't include them.
   */
  public static HoodieSchema reconcileSchemaStructural(HoodieSchema incomingSchema,
                                                       HoodieSchema oldTableSchema,
                                                       boolean makeMissingFieldsNullable) {
    org.apache.avro.Schema reconciled = AvroSchemaEvolutionUtils.reconcileSchema(
        incomingSchema.getAvroSchema(), oldTableSchema.getAvroSchema(), makeMissingFieldsNullable);
    return HoodieSchema.fromAvroSchema(reconciled);
  }

  /**
   * Reconciles nullability and type-promotion requirements between a source
   * (incoming) schema and a target (existing) schema, adjusting the source to be
   * in line with the target's nullability and promotable types.
   *
   * <p>Semantics match
   * {@link AvroSchemaEvolutionUtils#reconcileSchemaRequirements(org.apache.avro.Schema, org.apache.avro.Schema, boolean)}.
   * If {@code shouldReorderColumns} is true, the source's fields are ordered to match
   * the target's positional layout, preserving inter-commit field ordering.</p>
   *
   * @param sourceSchema         incoming source schema to be reconciled
   * @param targetSchema         target schema to reconcile against
   * @param shouldReorderColumns if true, fields in the result follow the target's order
   * @return source-shaped HoodieSchema with nullability and types reconciled
   */
  public static HoodieSchema reconcileSchemaRequirements(HoodieSchema sourceSchema,
                                                         HoodieSchema targetSchema,
                                                         boolean shouldReorderColumns) {
    org.apache.avro.Schema reconciled = AvroSchemaEvolutionUtils.reconcileSchemaRequirements(
        sourceSchema == null ? null : sourceSchema.getAvroSchema(),
        targetSchema == null ? null : targetSchema.getAvroSchema(),
        shouldReorderColumns);
    return HoodieSchema.fromAvroSchema(reconciled);
  }

  /**
   * Collects renamed columns between two schemas — fields whose column id is the
   * same but whose full name has changed. The resulting map is keyed by the new
   * (target) full name and valued by the leaf-name of the old (source) full name,
   * matching the wire shape consumed by record rewriters.
   *
   * <p>HoodieSchema-shaped replacement for
   * {@link org.apache.hudi.internal.schema.utils.InternalSchemaUtils#collectRenameCols}.</p>
   */
  /**
   * Normalizes union ordering so {@code null} sits first within nullable union
   * branches, matching the ordering Hudi has historically written to disk. Returns
   * {@code HoodieSchema.NULL_SCHEMA} for a {@code null} input and the schema
   * unchanged for non-record types. Wraps the legacy
   * {@code InternalSchemaConverter.fixNullOrdering} during the migration.
   */
  public static HoodieSchema fixNullOrdering(HoodieSchema schema) {
    return InternalSchemaConverter.fixNullOrdering(schema);
  }

  /**
   * Collects top-level columns whose primitive type differs between two schemas,
   * keyed by the column's index in {@code schema}. The pair holds (newType,
   * oldType) so callers can build a cast plan from {@code oldType} to
   * {@code newType}. HoodieSchema-shaped replacement for
   * {@link InternalSchemaUtils#collectTypeChangedCols(InternalSchema, InternalSchema)}.
   */
  public static Map<Integer, Pair<Type, Type>> collectTypeChangedCols(HoodieSchema schema, HoodieSchema oldSchema) {
    return InternalSchemaUtils.collectTypeChangedCols(
        HoodieSchemaInternalSchemaBridge.toInternalSchema(schema),
        HoodieSchemaInternalSchemaBridge.toInternalSchema(oldSchema));
  }

  /**
   * Maps a filter column name from the query schema's namespace to the file
   * schema's namespace (or returns "" when the column was deleted on the file
   * side). HoodieSchema-direct replacement for
   * {@link InternalSchemaUtils#reBuildFilterName(String, InternalSchema, InternalSchema)}.
   */
  public static String reBuildFilterName(String name, HoodieSchema fileSchema, HoodieSchema querySchema) {
    int nameId = querySchema.findIdByName(name);
    if (nameId < 0) {
      throw new IllegalArgumentException(String.format(
          "cannot find filter col name: %s from querySchema: %s", name, querySchema));
    }
    if (fileSchema.findType(nameId) == null) {
      // column added on the query side — file does not contain it, so the filter is dead.
      return "";
    }
    String fileName = fileSchema.findFullName(nameId);
    return name.equals(fileName) ? name : fileName;
  }

  public static Map<String, String> collectRenameCols(HoodieSchema oldSchema, HoodieSchema newSchema) {
    List<String> colNamesFromWriteSchema = oldSchema.getAllColsFullName();
    return colNamesFromWriteSchema.stream()
        .filter(f -> {
          int fieldIdFromWriteSchema = oldSchema.findIdByName(f);
          // Find columns that share an id but use a different name in the new schema.
          return newSchema.getAllIds().contains(fieldIdFromWriteSchema)
              && !newSchema.findFullName(fieldIdFromWriteSchema).equalsIgnoreCase(f);
        })
        .collect(Collectors.toMap(
            e -> newSchema.findFullName(oldSchema.findIdByName(e)),
            e -> {
              int lastDotIndex = e.lastIndexOf(".");
              return e.substring(lastDotIndex == -1 ? 0 : lastDotIndex + 1);
            }));
  }
}
