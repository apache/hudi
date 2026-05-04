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
import org.apache.hudi.common.schema.HoodieSchemaIdAssigner;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChanges;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChangesHelper;
import org.apache.hudi.common.schema.evolution.legacy.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.evolution.legacy.utils.SchemaChangeUtils;
import org.apache.hudi.common.schema.types.Type;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;

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
 * <p>Production callers go through {@link #reconcileSchema(HoodieSchema, HoodieSchema, boolean)},
 * which walks HoodieSchema directly via {@link HoodieSchemaChangeApplier}
 * (see {@code reconcileHoodieSchemaDirect}). The legacy {@code TableChanges}
 * + {@code SchemaChangeUtils} machinery is still on the {@link InternalSchema}-typed
 * test-surface entries; those will move once the test suite migrates off
 * {@link InternalSchema} fixtures.
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
    return reconcileHoodieSchemaDirect(incomingSchema, oldTableSchema, makeMissingFieldsNullable);
  }

  /**
   * Sibling of {@link #reconcileSchema(HoodieSchema, HoodieSchema, boolean)}
   * for the structural reconciliation use case (e.g. {@code deduceWriterSchema}):
   * field ids are stripped from both inputs and result, so id-carrying
   * evolution schemas don't leak ids into commit metadata or Parquet writes
   * that historically didn't include them.
   */
  public static HoodieSchema reconcileSchemaStructural(HoodieSchema incomingSchema,
                                                       HoodieSchema oldTableSchema,
                                                       boolean makeMissingFieldsNullable) {
    HoodieSchema reconciled = reconcileHoodieSchemaDirect(
        stripIds(incomingSchema), stripIds(oldTableSchema), makeMissingFieldsNullable);
    return stripIds(reconciled);
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
    // Mirror the legacy InternalSchemaConverter.convert()'s checkNullType pass:
    // a record field / array element / map value with bare NULL type (i.e. not a
    // [null, T] union) is rejected.
    checkNoBareNullTypedFields(schema, "");
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
   * Throws {@link HoodieNullSchemaTypeException} if {@code schema} contains a
   * record field, array element, or map value whose effective type is
   * {@link HoodieSchemaType#NULL} (i.e. not wrapped in a {@code [null, T]}
   * union). Mirrors the legacy {@code InternalSchemaConverter.checkNullType}
   * pass which fired during the {@code HoodieSchema → InternalSchema} convert.
   */
  private static void checkNoBareNullTypedFields(HoodieSchema schema, String pathPrefix) {
    HoodieSchema effective = schema.isNullable() ? schema.getNonNullType() : schema;
    switch (effective.getType()) {
      case RECORD:
        for (HoodieSchemaField field : effective.getFields()) {
          HoodieSchema fieldSchema = field.schema();
          HoodieSchema fieldEffective = fieldSchema.isNullable() ? fieldSchema.getNonNullType() : fieldSchema;
          if (fieldEffective.getType() == HoodieSchemaType.NULL) {
            throw new HoodieNullSchemaTypeException(
                "Field '" + pathPrefix + field.name() + "' has type null");
          }
          checkNoBareNullTypedFields(fieldSchema, pathPrefix + field.name() + ".");
        }
        return;
      case ARRAY: {
        HoodieSchema element = effective.getElementType();
        HoodieSchema elementEffective = element.isNullable() ? element.getNonNullType() : element;
        if (elementEffective.getType() == HoodieSchemaType.NULL) {
          throw new HoodieNullSchemaTypeException(
              "Field '" + pathPrefix + "element' has type null");
        }
        checkNoBareNullTypedFields(element, pathPrefix + "element.");
        return;
      }
      case MAP: {
        HoodieSchema value = effective.getValueType();
        HoodieSchema valueEffective = value.isNullable() ? value.getNonNullType() : value;
        if (valueEffective.getType() == HoodieSchemaType.NULL) {
          throw new HoodieNullSchemaTypeException(
              "Field '" + pathPrefix + "value' has type null");
        }
        checkNoBareNullTypedFields(value, pathPrefix + "value.");
        return;
      }
      default:
        // Primitive — including a top-level bare NULL schema, which fixNullOrdering's
        // entry point handles before calling this walker.
    }
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
          // Mirror the legacy InternalSchema → HoodieSchema convert(): every
          // nullable field gains a {@code default: null}, even if the source
          // field had no explicit default. Required fields keep their existing
          // default (or none).
          Object existingDefault = field.defaultVal().orElse(null);
          Object effectiveDefault = existingDefault;
          if (existingDefault == null && newSubSchema.isNullable()) {
            effectiveDefault = HoodieSchema.NULL_VALUE;
          }
          HoodieSchemaField rebuilt = HoodieSchemaField.of(
              field.name(), newSubSchema, field.doc().orElse(null), effectiveDefault);
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
      // Nearest later sibling already in old, ties broken by smallest old-id.
      String inferPosition = null;
      int inferPositionOldId = Integer.MAX_VALUE;
      int incomingIdOfName = incoming.findIdByName(name);
      for (String c : colNamesIncoming) {
        if (c.lastIndexOf(".") != splitPoint
            || !c.startsWith(parentName)
            || incoming.findIdByName(c) <= incomingIdOfName) {
          continue;
        }
        int cOldId = oldTableSchema.findIdByName(c);
        if (cOldId > 0 && cOldId < inferPositionOldId) {
          inferPosition = c;
          inferPositionOldId = cOldId;
        }
      }
      addChange.addColumns(parentName, rawName, incoming.findType(name), null);
      if (inferPosition != null) {
        addChange.addPositionChange(name, inferPosition, "before");
      }
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

  // -------------------------------------------------------------------------
  // HoodieSchema-direct reconciliation. Walks {@link HoodieSchema} via the
  // applier surface for adds, type promotions, and nullability flips. The
  // legacy {@code TableChanges} + {@code SchemaChangeUtils} machinery is not
  // on this path. Production callers route here via the public
  // {@link #reconcileSchema(HoodieSchema, HoodieSchema, boolean)} entry.
  // -------------------------------------------------------------------------

  private static HoodieSchema reconcileHoodieSchemaDirect(HoodieSchema incomingSchema,
                                                          HoodieSchema oldTableSchema,
                                                          boolean makeMissingFieldsNullable) {
    if (incomingSchema.getType() == HoodieSchemaType.NULL) {
      return oldTableSchema;
    }

    // Deep-copy incoming so id stamping doesn't mutate the caller's schema, and
    // the underlying Avro Schema.Field instances are independent of the source
    // (Schema.Field can't be re-parented to a second record).
    HoodieSchema incoming = HoodieSchema.parse(incomingSchema.toAvroSchema().toString());
    HoodieSchemaIdAssigner.assignFresh(incoming);

    List<String> oldNames = oldTableSchema.getAllColsFullName();
    List<String> incomingNames = incoming.getAllColsFullName();
    Set<String> oldNameSet = new HashSet<>(oldNames);
    Set<String> incomingNameSet = new HashSet<>(incomingNames);

    List<String> newNames = incomingNames.stream()
        .filter(n -> !oldNameSet.contains(n))
        .collect(Collectors.toList());
    List<String> missingNames = oldNames.stream()
        .filter(n -> !incomingNameSet.contains(n))
        .collect(Collectors.toList());
    List<String> typeChangeNames = incomingNames.stream()
        .filter(n -> oldNameSet.contains(n) && !typesEqualIgnoringIds(incoming.findType(n), oldTableSchema.findType(n)))
        .collect(Collectors.toList());

    if (newNames.isEmpty() && missingNames.isEmpty() && typeChangeNames.isEmpty()
        && oldNames.size() == incomingNames.size()) {
      return oldTableSchema;
    }

    // Filter redundant adds — keep only top-level new fields. Children of new
    // parents are added implicitly when the parent's struct/array/map is
    // adopted. Mirrors the legacy filter at the same point in reconcileInternal.
    Set<String> newNameSet = new HashSet<>(newNames);
    List<String> finalAdds = new ArrayList<>();
    for (String name : newNames) {
      String parent = parentOf(name);
      if (!parent.isEmpty() && newNameSet.contains(parent)) {
        continue;
      }
      finalAdds.add(name);
    }

    HoodieSchema afterAdds = oldTableSchema;
    for (String name : finalAdds) {
      String parent = parentOf(name);
      // Strip ids from the colType so the sub-tree's stamped ids don't
      // collide with old's ids when adopted into the rebuilt parent record.
      // assignFresh after the add round mints fresh ids for the now-id-less
      // sub-tree.
      HoodieSchema colType = stripIds(incoming.findType(name));

      int incomingIdx = incomingNames.indexOf(name);
      // Nearest later sibling already in old, ties broken by smallest old-id.
      String posCol = null;
      int posColOldId = Integer.MAX_VALUE;
      for (String c : incomingNames) {
        if (!parentOf(c).equals(parent)
            || incomingNames.indexOf(c) <= incomingIdx) {
          continue;
        }
        int cOldId = oldTableSchema.findIdByName(c);
        if (cOldId > 0 && cOldId < posColOldId) {
          posCol = c;
          posColOldId = cOldId;
        }
      }
      ColumnPositionType posType = posCol != null ? ColumnPositionType.BEFORE : ColumnPositionType.NO_OPERATION;
      afterAdds = new HoodieSchemaChangeApplier(afterAdds).applyAddChange(name, colType, null, posCol, posType);
    }
    // Mint fresh ids on any newly-adopted struct / array / map sub-trees.
    HoodieSchemaIdAssigner.assignFresh(afterAdds);

    HoodieSchema evolved = afterAdds;
    for (String col : typeChangeNames) {
      HoodieSchema newType = incoming.findType(col);
      if (!isNestedType(newType)) {
        evolved = new HoodieSchemaChangeApplier(evolved).applyColumnTypeChange(col, newType);
      }
    }

    if (makeMissingFieldsNullable) {
      // For a parent that's missing on the incoming side, only mark the parent
      // nullable; descendants are unreachable at decode time so flipping them
      // individually is wasted work and would produce surprising commits.
      Set<String> visited = new HashSet<>();
      List<String> sortedMissing = missingNames.stream().sorted().collect(Collectors.toList());
      for (String col : sortedMissing) {
        if (META_FIELD_NAMES.contains(col)) {
          continue;
        }
        String parent = parentOf(col);
        if (!visited.contains(parent)) {
          evolved = new HoodieSchemaChangeApplier(evolved).applyColumnNullabilityChange(col, true);
        }
        visited.add(col);
      }
    }

    if (structurallyEqual(evolved, oldTableSchema)) {
      return oldTableSchema;
    }
    return evolved;
  }

  private static String parentOf(String fullName) {
    int dot = fullName.lastIndexOf('.');
    return dot < 0 ? "" : fullName.substring(0, dot);
  }

  private static boolean isNestedType(HoodieSchema schema) {
    HoodieSchema effective = schema.isNullable() ? schema.getNonNullType() : schema;
    HoodieSchemaType t = effective.getType();
    return t == HoodieSchemaType.RECORD || t == HoodieSchemaType.ARRAY || t == HoodieSchemaType.MAP;
  }

  /**
   * True when {@code a} and {@code b} have equivalent Avro type structure,
   * ignoring custom props (so {@code field-id} / {@code element-id} /
   * {@code key-id} / {@code value-id} differences don't surface as type
   * mismatches).
   */
  private static boolean typesEqualIgnoringIds(HoodieSchema a, HoodieSchema b) {
    return a != null && b != null && a.getAvroSchema().equals(b.getAvroSchema());
  }

  private static boolean structurallyEqual(HoodieSchema a, HoodieSchema b) {
    return a.getAvroSchema().equals(b.getAvroSchema());
  }

  /**
   * Returns a fresh HoodieSchema tree mirroring {@code source} but with all
   * id custom props removed. The rebuild walks every node; primitives are
   * copied by reference (HoodieSchema treats them as value types), but
   * RECORD / ARRAY / MAP / UNION nodes are reconstructed so {@code field-id}
   * / {@code element-id} / {@code key-id} / {@code value-id} can be dropped
   * without mutating the source.
   *
   * <p>Used by reconciliation when adopting an incoming sub-tree into the
   * existing schema: stripping the sub-tree's ids first prevents collisions
   * with the existing schema's id population, and a follow-up
   * {@link HoodieSchemaIdAssigner#assignFresh} mints fresh ids for the
   * newly-adopted nodes.
   */
  private static HoodieSchema stripIds(HoodieSchema source) {
    return rebuildStripped(source);
  }

  private static HoodieSchema rebuildStripped(HoodieSchema source) {
    switch (source.getType()) {
      case UNION: {
        List<HoodieSchema> rewritten = new ArrayList<>(source.getTypes().size());
        for (HoodieSchema branch : source.getTypes()) {
          rewritten.add(rebuildStripped(branch));
        }
        return HoodieSchema.createUnion(rewritten);
      }
      case RECORD: {
        List<HoodieSchemaField> newFields = new ArrayList<>(source.getFields().size());
        for (HoodieSchemaField field : source.getFields()) {
          HoodieSchema newSubSchema = rebuildStripped(field.schema());
          HoodieSchemaField rebuilt = HoodieSchemaField.of(
              field.name(), newSubSchema, field.doc().orElse(null), field.defaultVal().orElse(null));
          for (Map.Entry<String, Object> e : field.getObjectProps().entrySet()) {
            if (!HoodieSchema.FIELD_ID_PROP.equals(e.getKey())) {
              rebuilt.addProp(e.getKey(), e.getValue());
            }
          }
          newFields.add(rebuilt);
        }
        return HoodieSchema.createRecord(source.getName(), source.getNamespace().orElse(null),
            source.getDoc().orElse(null), newFields);
      }
      case ARRAY:
        return HoodieSchema.createArray(rebuildStripped(source.getElementType()));
      case MAP:
        return HoodieSchema.createMap(rebuildStripped(source.getValueType()));
      default:
        return source;
    }
  }
}
