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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.action.TableChangesHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.reduce;
import static org.apache.hudi.internal.schema.convert.InternalSchemaConverter.convert;

/**
 * Utility methods to support evolve old avro schema based on a given schema.
 */
public class AvroSchemaEvolutionUtils {
  private static final Set<String> META_FIELD_NAMES = Arrays.stream(HoodieRecord.HoodieMetadataField.values())
      .map(HoodieRecord.HoodieMetadataField::getFieldName).collect(Collectors.toSet());

  /**
   * Support reconcile from a new avroSchema.
   * 1) incoming data has missing columns that were already defined in the table –> null values will be injected into missing columns
   * 2) incoming data contains new columns not defined yet in the table -> columns will be added to the table schema (incoming dataframe?)
   * 3) incoming data has missing columns that are already defined in the table and new columns not yet defined in the table ->
   * new columns will be added to the table schema, missing columns will be injected with null values
   * 4) support type change
   * 5) support nested schema change.
   * Notice:
   * the incoming schema should not have delete/rename semantics.
   * for example: incoming schema:  int a, int b, int d;   oldTableSchema int a, int b, int c, int d
   * we must guarantee the column c is missing semantic, instead of delete semantic.
   *
   * @param incomingSchema            implicitly evolution of avro when hoodie write operation
   * @param oldTableSchema            old internalSchema
   * @param makeMissingFieldsNullable if true, fields missing from the incoming schema when compared to the oldTableSchema will become
   *                                  nullable in the result. Otherwise, no updates will be made to those fields.
   * @return reconcile Schema
   */
  public static InternalSchema reconcileSchema(HoodieSchema incomingSchema, InternalSchema oldTableSchema,
                                               boolean makeMissingFieldsNullable, Map<String, Type> timestampLogicalTypeOverrides) {
    /* If incoming schema is null, we fall back on table schema. */
    if (incomingSchema.isSchemaNull()) {
      return oldTableSchema;
    }
    InternalSchema inComingInternalSchema = convert(incomingSchema, oldTableSchema.getNameToPosition());
    // check column add/missing
    List<String> colNamesFromIncoming = inComingInternalSchema.getAllColsFullName();
    List<String> colNamesFromOldSchema = oldTableSchema.getAllColsFullName();
    List<String> diffFromOldSchema = colNamesFromOldSchema.stream().filter(f -> !colNamesFromIncoming.contains(f)).collect(Collectors.toList());
    List<String> diffFromEvolutionColumns = colNamesFromIncoming.stream().filter(f -> !colNamesFromOldSchema.contains(f)).collect(Collectors.toList());
    // check type change.
    List<String> typeChangeColumns = colNamesFromIncoming
        .stream()
        .filter(f -> colNamesFromOldSchema.contains(f) && !inComingInternalSchema.findType(f).equals(oldTableSchema.findType(f)))
        .collect(Collectors.toList());
    // check columns the incoming schema relaxed from required to nullable. Since the result is built from
    // oldTableSchema (to preserve column order/ids and to null-fill missing columns), an existing column
    // whose incoming counterpart became nullable would otherwise silently keep the table's REQUIRED
    // nullability, blocking a valid required -> nullable evolution. We only ever relax (never tighten).
    List<String> nullabilityRelaxColumns = colNamesFromIncoming
        .stream()
        .filter(f -> colNamesFromOldSchema.contains(f)
            && !META_FIELD_NAMES.contains(f)
            && inComingInternalSchema.findField(f).isOptional()
            && !oldTableSchema.findField(f).isOptional())
        .collect(Collectors.toList());
    if (colNamesFromIncoming.size() == colNamesFromOldSchema.size() && diffFromOldSchema.size() == 0
        && typeChangeColumns.isEmpty() && nullabilityRelaxColumns.isEmpty()) {
      return oldTableSchema;
    }

    // Remove redundancy from diffFromEvolutionSchema.
    // for example, now we add a struct col in evolvedSchema, the struct col is " user struct<name:string, age:int> "
    // when we do diff operation: user, user.name, user.age will appear in the resultSet which is redundancy, user.name and user.age should be excluded.
    // deal with add operation
    TreeMap<Integer, String> finalAddAction = new TreeMap<>();
    for (int i = 0; i < diffFromEvolutionColumns.size(); i++) {
      String name = diffFromEvolutionColumns.get(i);
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      if (!parentName.isEmpty() && diffFromEvolutionColumns.contains(parentName)) {
        // find redundancy, skip it
        continue;
      }
      finalAddAction.put(inComingInternalSchema.findIdByName(name), name);
    }

    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldTableSchema);
    finalAddAction.entrySet().stream().forEach(f -> {
      String name = f.getValue();
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      String rawName = splitPoint > 0 ? name.substring(splitPoint + 1) : name;
      // try to infer add position.
      java.util.Optional<String> inferPosition =
          colNamesFromIncoming.stream().filter(c ->
              c.lastIndexOf(".") == splitPoint
                  && c.startsWith(parentName)
                  && inComingInternalSchema.findIdByName(c) > inComingInternalSchema.findIdByName(name)
                  && oldTableSchema.findIdByName(c) > 0).sorted((s1, s2) -> oldTableSchema.findIdByName(s1) - oldTableSchema.findIdByName(s2)).findFirst();
      addChange.addColumns(parentName, rawName, inComingInternalSchema.findType(name), null);
      inferPosition.map(i -> addChange.addPositionChange(name, i, "before"));
    });

    // do type evolution.
    InternalSchema internalSchemaAfterAddColumns = SchemaChangeUtils.applyTableChanges2Schema(oldTableSchema, addChange);
    // The reconcile pre-validates timestamp precision changes per field below (against the explicit
    // overrides), so the update change is constructed permissively; non-overridden precision changes
    // are rejected here with an actionable error rather than deferred to the gate.
    TableChanges.ColumnUpdateChange typeChange = TableChanges.ColumnUpdateChange.get(
        internalSchemaAfterAddColumns, false, true);
    typeChangeColumns.stream().filter(f -> !inComingInternalSchema.findType(f).isNestedType()).forEach(col -> {
      Type tableType = oldTableSchema.findType(col);
      Type incomingType = inComingInternalSchema.findType(col);
      if (SchemaChangeUtils.isGatedTimestampChange(tableType, incomingType)) {
        // Skip-if equals the *table* type: the reconcile is producing the new table schema starting
        // from oldTableSchema, so a coerce-to-table-precision override needs no schema update — the
        // writer coerces incoming values via rewriteRecordWithNewSchema.
        applyTimestampOverrideOrThrow(col, tableType, incomingType, timestampLogicalTypeOverrides, tableType, typeChange);
      } else {
        typeChange.updateColumnType(col, incomingType);
      }
    });

    // relax existing columns to nullable when the incoming schema made them nullable (valid widening)
    nullabilityRelaxColumns.forEach(col -> typeChange.updateColumnNullability(col, true));

    if (makeMissingFieldsNullable) {
      // mark columns missing from incoming schema as nullable
      Set<String> visited = new HashSet<>();
      diffFromOldSchema.stream()
          // ignore meta fields
          .filter(col -> !META_FIELD_NAMES.contains(col))
          .sorted()
          .forEach(col -> {
            // if parent is marked as nullable, only update the parent and not all the missing children field
            String parent = TableChangesHelper.getParentName(col);
            if (!visited.contains(parent)) {
              typeChange.updateColumnNullability(col, true);
            }
            visited.add(col);
          });
    }

    InternalSchema evolvedSchema = SchemaChangeUtils.applyTableChanges2Schema(internalSchemaAfterAddColumns, typeChange);
    // If evolvedSchema is exactly the same as the oldSchema, except the version number, return the old schema
    if (evolvedSchema.equalsIgnoringVersion(oldTableSchema)) {
      return oldTableSchema;
    }
    return evolvedSchema;
  }

  public static HoodieSchema reconcileSchema(HoodieSchema incomingSchema, HoodieSchema oldTableSchema, boolean makeMissingFieldsNullable,
                                             Map<String, Type> timestampLogicalTypeOverrides) {
    return convert(reconcileSchema(incomingSchema, convert(oldTableSchema), makeMissingFieldsNullable, timestampLogicalTypeOverrides), oldTableSchema.getFullName());
  }

  /**
   * Reconciles only the timestamp logical-type precision of {@code writerSchema} against
   * {@code tableSchema}, independent of column add/drop/nullability reconciliation. This is the
   * single guard that every writer-schema deduction path must apply, including the non-reconcile
   * paths that otherwise validate via the logical-type-blind Avro reader/writer compatibility check
   * and would let an unverified micros/millis flip through silently.
   *
   * <p>For each field whose precision differs from the table: an override pins it (equal to the
   * table type coerces the incoming values, a different type applies the authorized evolution), and
   * a change with no override throws. A UTC/local zone change throws unconditionally, since no
   * override authorizes one. Non-timestamp changes are left untouched here.
   */
  public static HoodieSchema reconcileTimestampLogicalType(HoodieSchema writerSchema, HoodieSchema tableSchema,
                                                           Map<String, Type> timestampLogicalTypeOverrides) {
    if (writerSchema == null || writerSchema.getType() != HoodieSchemaType.RECORD
        || tableSchema == null || tableSchema.getType() != HoodieSchemaType.RECORD) {
      return writerSchema;
    }
    InternalSchema writerInternal = convert(writerSchema);
    InternalSchema tableInternal = convert(tableSchema);
    List<String> tableCols = tableInternal.getAllColsFullName();
    TableChanges.ColumnUpdateChange typeChange = TableChanges.ColumnUpdateChange.get(writerInternal, false, true);
    boolean changed = false;
    for (String col : writerInternal.getAllColsFullName()) {
      if (!tableCols.contains(col)) {
        continue;
      }
      Type writerType = writerInternal.findType(col);
      Type tableType = tableInternal.findType(col);
      if (writerType.isNestedType()) {
        continue;
      }
      // A zone change is never authorizable, and this is the only guard on the default
      // non-reconcile path -- the Avro reader/writer check that follows is logical-type-blind for
      // two long-backed fields, so skipping here would let the flip through silently.
      if (SchemaChangeUtils.isCrossZoneTimestampChange(tableType, writerType)) {
        throw crossZoneTimestampChangeError(col, tableType, writerType);
      }
      if (!SchemaChangeUtils.isGatedTimestampChange(tableType, writerType)) {
        continue;
      }
      // Skip-if equals the *writer* type: this method returns a modified writerSchema. When the
      // override already matches the writer field, the writer schema is what we want; no update.
      if (applyTimestampOverrideOrThrow(col, tableType, writerType, timestampLogicalTypeOverrides, writerType, typeChange)) {
        changed = true;
      }
    }
    if (!changed) {
      return writerSchema;
    }
    return convert(SchemaChangeUtils.applyTableChanges2Schema(writerInternal, typeChange), writerSchema.getFullName());
  }

  /**
   * Shared override-apply for a single field whose type is a gated timestamp precision change.
   * Called from both {@link #reconcileSchema} and {@link #reconcileTimestampLogicalType} — those
   * two paths differ only in which "current" schema they compare the override against (the table
   * type vs. the writer type), so the caller passes that in as {@code skipIfEquals}.
   *
   * @param col                the fully-qualified column name (for the error message)
   * @param tableType          the table's current type (for the error message)
   * @param incomingType       the writer/incoming type (for the error message)
   * @param overrides          the parsed per-field overrides map
   * @param skipIfEquals       compare the override against this; no schema update when equal
   * @param typeChange         the accumulator for schema updates
   * @return {@code true} if the override was applied (schema will change), {@code false} otherwise
   * @throws SchemaCompatibilityException when no override is present for this gated change
   */
  private static boolean applyTimestampOverrideOrThrow(String col, Type tableType, Type incomingType,
                                                       Map<String, Type> overrides, Type skipIfEquals,
                                                       TableChanges.ColumnUpdateChange typeChange) {
    Type overrideType = overrides.get(col);
    if (overrideType == null) {
      throw timestampPrecisionChangeError(col, tableType, incomingType);
    }
    if (overrideType.equals(skipIfEquals)) {
      return false;
    }
    typeChange.updateColumnType(col, overrideType);
    return true;
  }

  private static SchemaCompatibilityException crossZoneTimestampChangeError(String col, Type from, Type to) {
    return new SchemaCompatibilityException(String.format(
        "Refusing to change the timestamp logical type of column '%s' from '%s' to '%s': this crosses the "
            + "UTC/local boundary, which changes the instant the stored value denotes and cannot be repaired by "
            + "rescaling. '%s' authorizes precision changes only, never a zone change. Keep writing the column "
            + "with its existing zone, or add a new column and backfill it with an explicit conversion.",
        col, from, to, HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key()));
  }

  /**
   * Builds the actionable error for a gated timestamp logical-type change with no per-field override
   * in {@code hoodie.write.timestamp.logical.type.overrides}. Public so tests can assert the exact
   * message without duplicating its format.
   */
  public static SchemaCompatibilityException timestampPrecisionChangeError(String col, Type from, Type to) {
    return new SchemaCompatibilityException(String.format(
        "Refusing to change the timestamp logical type of column '%s' from '%s' to '%s' without an explicit "
            + "verdict. This precision change is not applied automatically because the correct target depends "
            + "on the stored values, not the incoming schema. Inspect the raw long values of '%s' in the existing "
            + "base files: for instants after 1990 epoch-millis is around 1e12 and epoch-micros is around 1e15, "
            + "so the ranges do not overlap (TimestampLogicalTypeClassifier implements this verdict). Then set "
            + "'%s' to the precision the values actually are, for example '%s:timestamp-micros' to keep the "
            + "current precision and coerce the incoming values, or '%s:timestamp-millis' to evolve the column. "
            + "Existing base files are not rewritten by this change; rewrite them via clustering or compaction "
            + "so that non-Hudi readers also see the corrected type.",
        col, from, to, col, HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key(), col, col));
  }

  /**
   * Reconciles nullability and datatype requirements b/w {@code source} and {@code target} schemas,
   * by adjusting these of the {@code source} schema to be in-line with the ones of the
   * {@code target} one. Source is considered to be new incoming schema, while target could refer to prev table schema.
   * For example,
   * if colA in source is non-nullable, but is nullable in target, output schema will have colA as nullable.
   * if "hoodie.datasource.write.new.columns.nullable" is set to true and if colB is not present in source, but
   * is present in target, output schema will have colB as nullable.
   * if colC has different data type in source schema compared to target schema and if its promotable, (say source is int,
   * and target is long and since int can be promoted to long), colC will be long data type in output schema.
   *
   *
   * @param sourceSchema source schema that needs reconciliation
   * @param targetSchema target schema that source schema will be reconciled against
   * @return schema (based off {@code source} one) that has nullability constraints and datatypes reconciled
   */
  public static HoodieSchema reconcileSchemaRequirements(HoodieSchema sourceSchema, HoodieSchema targetSchema, boolean shouldReorderColumns) {
    if (targetSchema.isSchemaNull() || targetSchema.getFields().isEmpty()) {
      return sourceSchema;
    }

    if (sourceSchema == null || sourceSchema.isSchemaNull() || sourceSchema.getFields().isEmpty()) {
      return targetSchema;
    }

    InternalSchema targetInternalSchema = convert(targetSchema);
    // Use existing fieldIds for consistent field ordering between commits when shouldReorderColumns is true
    InternalSchema sourceInternalSchema = convert(sourceSchema, shouldReorderColumns ? targetInternalSchema.getNameToPosition() : Collections.emptyMap());

    List<String> colNamesSourceSchema = sourceInternalSchema.getAllColsFullName();
    List<String> colNamesTargetSchema = targetInternalSchema.getAllColsFullName();

    List<String> nullableUpdateColsInSource = new ArrayList<>();
    List<String> typeUpdateColsInSource = new ArrayList<>();
    colNamesSourceSchema.forEach(field -> {
      // handle columns that needs to be made nullable
      if (colNamesTargetSchema.contains(field) && sourceInternalSchema.findField(field).isOptional() != targetInternalSchema.findField(field).isOptional()) {
        nullableUpdateColsInSource.add(field);
      }
      // handle columns that needs type to be updated
      if (colNamesTargetSchema.contains(field) && SchemaChangeUtils.shouldPromoteType(sourceInternalSchema.findType(field), targetInternalSchema.findType(field))) {
        typeUpdateColsInSource.add(field);
      }
    });

    if (nullableUpdateColsInSource.isEmpty() && typeUpdateColsInSource.isEmpty()) {
      //standardize order of unions
      return convert(sourceInternalSchema, sourceSchema.getFullName());
    }

    TableChanges.ColumnUpdateChange schemaChange = TableChanges.ColumnUpdateChange.get(sourceInternalSchema);

    // Reconcile nullability constraints (by executing phony schema change)
    if (!nullableUpdateColsInSource.isEmpty()) {
      schemaChange = reduce(nullableUpdateColsInSource, schemaChange,
          (change, field) -> change.updateColumnNullability(field, true));
    }

    // Reconcile type promotions
    if (!typeUpdateColsInSource.isEmpty()) {
      schemaChange = reduce(typeUpdateColsInSource, schemaChange,
          (change, field) -> change.updateColumnType(field, targetInternalSchema.findType(field)));
    }


    return convert(SchemaChangeUtils.applyTableChanges2Schema(sourceInternalSchema, schemaChange), sourceSchema.getFullName());
  }
}

