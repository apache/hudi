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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChanges;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChangesHelper;
import org.apache.hudi.common.schema.evolution.legacy.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.evolution.legacy.utils.SchemaChangeUtils;
import org.apache.hudi.common.schema.types.Type;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.CollectionUtils.reduce;
import static org.apache.hudi.common.schema.evolution.legacy.convert.InternalSchemaConverter.convert;

/**
 * HoodieSchema-shaped façade for write-path schema evolution: reconciling an
 * incoming write schema against the table's current schema, including missing
 * columns, added columns, type promotions, and nullability adjustments.
 *
 * <p>The reconciliation algorithm is preserved verbatim from the prior
 * {@code AvroSchemaEvolutionUtils} (now subsumed) — the underlying
 * {@code TableChanges} + {@code SchemaChangeUtils} still operate on
 * {@link InternalSchema}, and the bridge converts at the
 * {@link HoodieSchema} boundary.
 */
public final class HoodieSchemaEvolutionUtils {

  private static final Set<String> META_FIELD_NAMES =
      Arrays.stream(HoodieRecord.HoodieMetadataField.values())
          .map(HoodieRecord.HoodieMetadataField::getFieldName)
          .collect(Collectors.toSet());

  private HoodieSchemaEvolutionUtils() {
  }

  // -------------------------------------------------------------------------
  // Public reconciliation API.
  // -------------------------------------------------------------------------

  /**
   * Reconciles an incoming write schema against the existing table schema, adding
   * any new columns, promoting types where allowed, and (optionally) marking
   * missing columns as nullable.
   *
   * <p>The incoming schema is assumed to have <i>missing</i> columns rather than
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
    InternalSchema reconciled = reconcileInternal(incomingSchema.getAvroSchema(), oldInternal, makeMissingFieldsNullable);
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
    Schema reconciled = reconcileAvroSchema(
        incomingSchema.getAvroSchema(), oldTableSchema.getAvroSchema(), makeMissingFieldsNullable);
    return HoodieSchema.fromAvroSchema(reconciled);
  }

  /**
   * Reconciles nullability and type-promotion requirements between a source
   * (incoming) schema and a target (existing) schema, adjusting the source to be
   * in line with the target's nullability and promotable types.
   *
   * <p>If {@code shouldReorderColumns} is true, the source's fields are ordered to
   * match the target's positional layout, preserving inter-commit field ordering.</p>
   *
   * @param sourceSchema         incoming source schema to be reconciled
   * @param targetSchema         target schema to reconcile against
   * @param shouldReorderColumns if true, fields in the result follow the target's order
   * @return source-shaped HoodieSchema with nullability and types reconciled
   */
  public static HoodieSchema reconcileSchemaRequirements(HoodieSchema sourceSchema,
                                                         HoodieSchema targetSchema,
                                                         boolean shouldReorderColumns) {
    Schema reconciled = reconcileSchemaRequirements(
        sourceSchema == null ? null : sourceSchema.getAvroSchema(),
        targetSchema == null ? null : targetSchema.getAvroSchema(),
        shouldReorderColumns);
    return HoodieSchema.fromAvroSchema(reconciled);
  }

  /**
   * InternalSchema-typed entry point retained for the legacy-test surface and
   * any internal caller that still operates on {@link InternalSchema}. Public
   * callers should prefer {@link #reconcileSchema(HoodieSchema, HoodieSchema, boolean)}.
   */
  public static InternalSchema reconcileSchema(Schema incomingSchema,
                                                InternalSchema oldTableSchema,
                                                boolean makeMissingFieldsNullable) {
    return reconcileInternal(incomingSchema, oldTableSchema, makeMissingFieldsNullable);
  }

  /**
   * Avro-only entry point retained for the legacy-test surface. Public callers
   * should prefer {@link #reconcileSchemaStructural}.
   */
  public static Schema reconcileSchema(Schema incomingSchema,
                                       Schema oldTableSchema,
                                       boolean makeMissingFieldsNullable) {
    return reconcileAvroSchema(incomingSchema, oldTableSchema, makeMissingFieldsNullable);
  }

  /**
   * Avro-only requirements-reconciliation entry point retained for the
   * legacy-test surface and any internal caller that still hands around
   * Avro {@link Schema}. Public callers should prefer
   * {@link #reconcileSchemaRequirements(HoodieSchema, HoodieSchema, boolean)}.
   */
  public static Schema reconcileSchemaRequirements(Schema sourceSchema,
                                                   Schema targetSchema,
                                                   boolean shouldReorderColumns) {
    if (targetSchema.getType() == Schema.Type.NULL || targetSchema.getFields().isEmpty()) {
      return sourceSchema;
    }
    if (sourceSchema == null || sourceSchema.getType() == Schema.Type.NULL || sourceSchema.getFields().isEmpty()) {
      return targetSchema;
    }
    InternalSchema targetInternal = convert(HoodieSchema.fromAvroSchema(targetSchema));
    // Preserve field-id assignment ordering with the target when reorder is requested
    // — that's what keeps inter-commit field positions stable.
    InternalSchema sourceInternal = convert(HoodieSchema.fromAvroSchema(sourceSchema),
        shouldReorderColumns ? targetInternal.getNameToPosition() : Collections.emptyMap());

    List<String> colNamesSource = sourceInternal.getAllColsFullName();
    List<String> colNamesTarget = targetInternal.getAllColsFullName();

    List<String> nullableUpdates = new ArrayList<>();
    List<String> typeUpdates = new ArrayList<>();
    colNamesSource.forEach(field -> {
      if (colNamesTarget.contains(field)
          && sourceInternal.findField(field).isOptional() != targetInternal.findField(field).isOptional()) {
        nullableUpdates.add(field);
      }
      if (colNamesTarget.contains(field)
          && SchemaChangeUtils.shouldPromoteType(sourceInternal.findType(field), targetInternal.findType(field))) {
        typeUpdates.add(field);
      }
    });

    if (nullableUpdates.isEmpty() && typeUpdates.isEmpty()) {
      // No updates — but still re-emit through the converter so union ordering is canonicalized.
      return convert(sourceInternal, sourceSchema.getFullName()).toAvroSchema();
    }

    TableChanges.ColumnUpdateChange schemaChange = TableChanges.ColumnUpdateChange.get(sourceInternal);
    if (!nullableUpdates.isEmpty()) {
      schemaChange = reduce(nullableUpdates, schemaChange,
          (change, field) -> change.updateColumnNullability(field, true));
    }
    if (!typeUpdates.isEmpty()) {
      schemaChange = reduce(typeUpdates, schemaChange,
          (change, field) -> change.updateColumnType(field, targetInternal.findType(field)));
    }
    return convert(SchemaChangeUtils.applyTableChanges2Schema(sourceInternal, schemaChange),
        sourceSchema.getFullName()).toAvroSchema();
  }

  // -------------------------------------------------------------------------
  // Static helpers (HoodieSchema-direct).
  // -------------------------------------------------------------------------

  /**
   * Normalizes union ordering so {@code null} sits first within nullable union
   * branches, matching the ordering Hudi has historically written to disk.
   * Walks the HoodieSchema directly: every union has its null branches
   * promoted to the front (preserving relative order of the rest), and the
   * walker recurses into records / arrays / maps. element-id, key-id, and
   * value-id custom props are preserved on rebuilt array / map sub-trees;
   * field-id and other field props are preserved when records are rebuilt.
   *
   * <p>Returns {@code HoodieSchema.NULL_SCHEMA} for a {@code null} input and
   * the schema unchanged for the {@code NULL} type.
   */
  public static HoodieSchema fixNullOrdering(HoodieSchema schema) {
    if (schema == null) {
      return HoodieSchema.NULL_SCHEMA;
    }
    if (schema.getType() == HoodieSchemaType.NULL) {
      return schema;
    }
    HoodieSchema rewritten = rewriteNullFirst(schema);
    // Forward schema-level metadata that the walker doesn't track (it operates
    // on the type tree). The applier and SerDe round-trip these.
    if (schema.schemaId() >= 0) {
      rewritten.setSchemaId(schema.schemaId());
    }
    if (schema.maxColumnId() >= 0) {
      rewritten.setMaxColumnId(schema.maxColumnId());
    }
    rewritten.invalidateIdIndex();
    return rewritten;
  }

  /**
   * Recursively rebuilds {@code schema} so that every union has its null
   * branches first. Pure-tree rewrite — the caller is responsible for
   * forwarding schema-level metadata (schemaId, maxColumnId) on the root.
   */
  private static HoodieSchema rewriteNullFirst(HoodieSchema schema) {
    switch (schema.getType()) {
      case UNION: {
        List<HoodieSchema> originalBranches = schema.getTypes();
        List<HoodieSchema> rewrittenBranches = new ArrayList<>(originalBranches.size());
        for (HoodieSchema branch : originalBranches) {
          rewrittenBranches.add(rewriteNullFirst(branch));
        }
        boolean hasNull = false;
        for (HoodieSchema branch : rewrittenBranches) {
          if (branch.getType() == HoodieSchemaType.NULL) {
            hasNull = true;
            break;
          }
        }
        if (!hasNull) {
          // No null branch, no reordering needed; only return new instance if
          // any inner branch was actually rebuilt.
          return rewrittenBranches.equals(originalBranches) ? schema : HoodieSchema.createUnion(rewrittenBranches);
        }
        // Promote null branches to the front, preserving relative order of
        // both null and non-null branches.
        List<HoodieSchema> reordered = new ArrayList<>(rewrittenBranches.size());
        for (HoodieSchema branch : rewrittenBranches) {
          if (branch.getType() == HoodieSchemaType.NULL) {
            reordered.add(branch);
          }
        }
        for (HoodieSchema branch : rewrittenBranches) {
          if (branch.getType() != HoodieSchemaType.NULL) {
            reordered.add(branch);
          }
        }
        return HoodieSchema.createUnion(reordered);
      }
      case RECORD: {
        List<HoodieSchemaField> newFields = new ArrayList<>(schema.getFields().size());
        for (HoodieSchemaField field : schema.getFields()) {
          HoodieSchema newSubSchema = rewriteNullFirst(field.schema());
          HoodieSchemaField rebuilt = HoodieSchemaField.of(
              field.name(), newSubSchema, field.doc().orElse(null), field.defaultVal().orElse(null));
          for (Map.Entry<String, Object> e : field.getObjectProps().entrySet()) {
            rebuilt.addProp(e.getKey(), e.getValue());
          }
          newFields.add(rebuilt);
        }
        return HoodieSchema.createRecord(schema.getName(), schema.getNamespace().orElse(null),
            schema.getDoc().orElse(null), newFields);
      }
      case ARRAY: {
        HoodieSchema newElement = rewriteNullFirst(schema.getElementType());
        Object eIdRaw = schema.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP);
        HoodieSchema arr = HoodieSchema.createArray(newElement);
        if (eIdRaw instanceof Number) {
          arr.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, ((Number) eIdRaw).intValue());
        }
        return arr;
      }
      case MAP: {
        HoodieSchema newValue = rewriteNullFirst(schema.getValueType());
        Object kIdRaw = schema.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP);
        Object vIdRaw = schema.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP);
        HoodieSchema map = HoodieSchema.createMap(newValue);
        if (kIdRaw instanceof Number) {
          map.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, ((Number) kIdRaw).intValue());
        }
        if (vIdRaw instanceof Number) {
          map.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, ((Number) vIdRaw).intValue());
        }
        return map;
      }
      default:
        return schema;
    }
  }

  /**
   * Collects top-level columns whose primitive type differs between two schemas,
   * keyed by the column's index in {@code schema}. The pair holds (newType,
   * oldType) so callers can build a cast plan from {@code oldType} to
   * {@code newType}. Walks ids on the HoodieSchema accessors directly; only
   * converts to {@link Type} at the result construction (callers expect Type
   * pairs for the cast-plan downstream).
   */
  public static Map<Integer, Pair<Type, Type>> collectTypeChangedCols(HoodieSchema schema, HoodieSchema oldSchema) {
    Set<Integer> ids = schema.getAllIds();
    Set<Integer> otherIds = oldSchema.getAllIds();
    Map<Integer, Pair<Type, Type>> result = new HashMap<>();
    List<Integer> topLevelIds = schema.getFields().stream()
        .map(HoodieSchemaField::fieldId).collect(Collectors.toList());
    for (Integer id : ids) {
      if (!otherIds.contains(id)) {
        continue;
      }
      HoodieSchema thisType = schema.findType(id);
      HoodieSchema otherType = oldSchema.findType(id);
      if (thisType.equals(otherType)) {
        continue;
      }
      String[] fieldNameParts = schema.findFullName(id).split("\\.");
      String[] otherFieldNameParts = oldSchema.findFullName(id).split("\\.");
      if (fieldNameParts.length != otherFieldNameParts.length) {
        continue;
      }
      String parentName = fieldNameParts[0];
      String otherParentName = otherFieldNameParts[0];
      int parentId = schema.findIdByName(parentName);
      if (parentId != oldSchema.findIdByName(otherParentName)) {
        continue;
      }
      int position = topLevelIds.indexOf(parentId);
      if (position < 0 || result.containsKey(position)) {
        continue;
      }
      result.put(position, Pair.of(
          InternalSchemaConverter.convertToField(schema.findType(parentName)),
          InternalSchemaConverter.convertToField(oldSchema.findType(otherParentName))));
    }
    return result;
  }

  /**
   * Maps a filter column name from the query schema's namespace to the file
   * schema's namespace (or returns "" when the column was deleted on the file
   * side).
   */
  public static String reBuildFilterName(String name, HoodieSchema fileSchema, HoodieSchema querySchema) {
    int nameId = querySchema.findIdByName(name);
    if (nameId < 0) {
      throw new IllegalArgumentException(String.format(
          "cannot find filter col name: %s from querySchema: %s", name, querySchema));
    }
    if (fileSchema.findType(nameId) == null) {
      return "";
    }
    String fileName = fileSchema.findFullName(nameId);
    return name.equals(fileName) ? name : fileName;
  }

  /**
   * Collects renamed columns between two schemas — fields whose column id is the
   * same but whose full name has changed. The resulting map is keyed by the new
   * (target) full name and valued by the leaf-name of the old (source) full name,
   * matching the wire shape consumed by record rewriters.
   */
  public static Map<String, String> collectRenameCols(HoodieSchema oldSchema, HoodieSchema newSchema) {
    List<String> colNamesFromWriteSchema = oldSchema.getAllColsFullName();
    return colNamesFromWriteSchema.stream()
        .filter(f -> {
          int fieldIdFromWriteSchema = oldSchema.findIdByName(f);
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

  // -------------------------------------------------------------------------
  // Internal reconciliation algorithm. Preserved verbatim from the prior
  // legacy AvroSchemaEvolutionUtils — same TableChanges-builder approach,
  // same nested-redundancy filter, same nullability-propagation rule.
  // -------------------------------------------------------------------------

  private static InternalSchema reconcileInternal(Schema incomingSchema,
                                                  InternalSchema oldTableSchema,
                                                  boolean makeMissingFieldsNullable) {
    if (incomingSchema.getType() == Schema.Type.NULL) {
      return oldTableSchema;
    }
    InternalSchema incoming = convert(HoodieSchema.fromAvroSchema(incomingSchema), oldTableSchema.getNameToPosition());

    List<String> colNamesIncoming = incoming.getAllColsFullName();
    List<String> colNamesOld = oldTableSchema.getAllColsFullName();
    List<String> diffFromOld = colNamesOld.stream()
        .filter(f -> !colNamesIncoming.contains(f)).collect(Collectors.toList());
    List<String> diffFromIncoming = colNamesIncoming.stream()
        .filter(f -> !colNamesOld.contains(f)).collect(Collectors.toList());
    List<String> typeChangeColumns = colNamesIncoming.stream()
        .filter(f -> colNamesOld.contains(f) && !incoming.findType(f).equals(oldTableSchema.findType(f)))
        .collect(Collectors.toList());

    if (colNamesIncoming.size() == colNamesOld.size() && diffFromOld.isEmpty() && typeChangeColumns.isEmpty()) {
      return oldTableSchema;
    }

    // Filter redundant entries — adding a struct surfaces both the struct itself and
    // each of its leaves, but only the struct should be added.
    TreeMap<Integer, String> finalAddAction = new TreeMap<>();
    for (String name : diffFromIncoming) {
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      if (!parentName.isEmpty() && diffFromIncoming.contains(parentName)) {
        continue;
      }
      finalAddAction.put(incoming.findIdByName(name), name);
    }

    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldTableSchema);
    finalAddAction.forEach((id, name) -> {
      int splitPoint = name.lastIndexOf(".");
      String parentName = splitPoint > 0 ? name.substring(0, splitPoint) : "";
      String rawName = splitPoint > 0 ? name.substring(splitPoint + 1) : name;
      // Try to infer position by finding the nearest sibling that already exists in old.
      java.util.Optional<String> inferPosition = colNamesIncoming.stream()
          .filter(c -> c.lastIndexOf(".") == splitPoint
              && c.startsWith(parentName)
              && incoming.findIdByName(c) > incoming.findIdByName(name)
              && oldTableSchema.findIdByName(c) > 0)
          .min((s1, s2) -> oldTableSchema.findIdByName(s1) - oldTableSchema.findIdByName(s2));
      addChange.addColumns(parentName, rawName, incoming.findType(name), null);
      inferPosition.map(i -> addChange.addPositionChange(name, i, "before"));
    });

    InternalSchema afterAdds = SchemaChangeUtils.applyTableChanges2Schema(oldTableSchema, addChange);
    TableChanges.ColumnUpdateChange typeChange = TableChanges.ColumnUpdateChange.get(afterAdds);
    typeChangeColumns.stream()
        .filter(f -> !incoming.findType(f).isNestedType())
        .forEach(col -> typeChange.updateColumnType(col, incoming.findType(col)));

    if (makeMissingFieldsNullable) {
      // For a parent that's missing on the incoming side, only mark the parent
      // nullable; descendants are unreachable at decode time so flipping them
      // individually is wasted work and would produce surprising commits.
      Set<String> visited = new HashSet<>();
      diffFromOld.stream()
          .filter(col -> !META_FIELD_NAMES.contains(col))
          .sorted()
          .forEach(col -> {
            String parent = TableChangesHelper.getParentName(col);
            if (!visited.contains(parent)) {
              typeChange.updateColumnNullability(col, true);
            }
            visited.add(col);
          });
    }

    InternalSchema evolved = SchemaChangeUtils.applyTableChanges2Schema(afterAdds, typeChange);
    if (evolved.equalsIgnoringVersion(oldTableSchema)) {
      return oldTableSchema;
    }
    return evolved;
  }

  private static Schema reconcileAvroSchema(Schema incomingSchema, Schema oldTableSchema, boolean makeMissingFieldsNullable) {
    return convert(
        reconcileInternal(incomingSchema, convert(HoodieSchema.fromAvroSchema(oldTableSchema)), makeMissingFieldsNullable),
        oldTableSchema.getFullName())
        .toAvroSchema();
  }
}
