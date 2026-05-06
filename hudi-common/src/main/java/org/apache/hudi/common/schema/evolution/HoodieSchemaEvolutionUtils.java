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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HoodieSchema-shaped façade for write-path schema evolution: reconciling an
 * incoming write schema against the table's current schema, including missing
 * columns, added columns, type promotions, and nullability adjustments.
 *
 * <p>All entry points walk HoodieSchema directly via
 * {@link HoodieSchemaChangeApplier} — see {@code reconcileHoodieSchemaDirect}
 * for the diff + applier-chain core.
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
    // reconcileHoodieSchemaDirect's diff loop is id-keyed
    // (getAllColsFullName / findIdByName / findType only see id-stamped
    // fields). Stamp a working copy when old lacks ids; otherwise pass
    // through so callers that depend on identity (the no-op short-circuit
    // returns its old-parameter instance unchanged) still see it.
    HoodieSchema oldStamped = oldTableSchema.maxColumnId() < 0
        ? withFreshIds(oldTableSchema)
        : oldTableSchema;
    return reconcileHoodieSchemaDirect(incomingSchema, oldStamped, makeMissingFieldsNullable);
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
    // reconcileHoodieSchemaDirect's diff loop is id-keyed and assumes old has
    // ids stamped; structural callers may pass in id-less Avro schemas. Stamp
    // a working copy of old, then strip ids from the result per the entry's
    // "ids not on output" contract. Incoming is already deep-copied and
    // id-stamped inside the inner method.
    HoodieSchema reconciled = reconcileHoodieSchemaDirect(
        incomingSchema, withFreshIds(oldTableSchema), makeMissingFieldsNullable);
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
    if (targetSchema == null
        || targetSchema.getType() == HoodieSchemaType.NULL
        || targetSchema.getFields().isEmpty()) {
      return sourceSchema;
    }
    if (sourceSchema == null
        || sourceSchema.getType() == HoodieSchemaType.NULL
        || sourceSchema.getFields().isEmpty()) {
      return targetSchema;
    }

    // Stamp working copies so the diff loop's id-keyed accessors
    // (getAllColsFullName / findType / findIdByName) work on schemas that
    // came in without any stamped ids — Spark write paths pass structural
    // Avro schemas.
    HoodieSchema sourceFresh = withFreshIds(sourceSchema);
    HoodieSchema targetFresh = withFreshIds(targetSchema);

    HoodieSchema source = shouldReorderColumns
        ? reorderToTargetLayout(sourceFresh, targetFresh.getNameToPosition())
        : sourceFresh;

    Set<String> targetNames = new HashSet<>(targetFresh.getAllColsFullName());
    List<String> nullableUpdates = new ArrayList<>();
    List<Pair<String, HoodieSchema>> typeUpdates = new ArrayList<>();
    for (String field : source.getAllColsFullName()) {
      if (!targetNames.contains(field)) {
        continue;
      }
      HoodieSchema sourceFieldType = source.findType(field);
      HoodieSchema targetFieldType = targetFresh.findType(field);
      if (sourceFieldType.isNullable() != targetFieldType.isNullable()) {
        nullableUpdates.add(field);
      }
      if (shouldPromoteHoodieType(sourceFieldType, targetFieldType)) {
        typeUpdates.add(Pair.of(field, targetFieldType));
      }
    }

    if (nullableUpdates.isEmpty() && typeUpdates.isEmpty()) {
      // Match the legacy "no updates" branch: re-emit through the canonicalizer
      // so union ordering is consistent. Strip the ids we stamped above —
      // the legacy entry returned id-less Avro schemas.
      return stripIds(fixNullOrdering(source));
    }

    Map<String, HoodieSchema> leafTypeUpdates = new HashMap<>();
    for (Pair<String, HoodieSchema> u : typeUpdates) {
      leafTypeUpdates.put(u.getLeft(), u.getRight());
    }

    HoodieSchema result = source;
    Set<String> descentNullableFlips = new HashSet<>();
    for (String field : nullableUpdates) {
      // Paths ending in "element" / "value" target an array element or map
      // value, not a record field — the applier's field-name walker doesn't
      // model those as transformable fields. Defer them to the descent
      // walker below; record-field paths still go through the applier.
      if (endsWithArrayOrMapDescent(field)) {
        descentNullableFlips.add(field);
        continue;
      }
      // Legacy hard-codes nullable=true even when target is required; preserved
      // verbatim to avoid behavior change.
      result = new HoodieSchemaChangeApplier(result).applyColumnNullabilityChange(field, true);
    }
    if (!descentNullableFlips.isEmpty()) {
      // Match the record-field branch's hard-coded nullable=true — Spark
      // INSERT INTO produces non-null array/map elements while the catalog
      // schema declares them nullable, so the writer schema must be widened
      // to nullable for the compat check to accept the diff.
      result = applyDescentNullabilityFlips(result, "", descentNullableFlips, true);
    }
    // Apply each leaf type update at its exact path. Walking source's tree
    // and overlaying only the promoted leaves is leaf-precise: it preserves
    // every other field's type intact, including siblings under the same
    // top-level parent that didn't need promotion.
    result = overlayLeafTypes(result, "", leafTypeUpdates);
    // Strip the ids stamped on inputs above — the legacy entry returned
    // id-less Avro schemas.
    return stripIds(result);
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
    // Reject schemas with a record field / array element / map value whose
    // effective type is bare NULL (i.e. not a [null, T] union) — Avro permits
    // it but downstream readers and the merger don't handle it.
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
   * union). Avro permits these schemas but downstream readers and the
   * evolution merger don't handle them.
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
          // Every nullable field gains a {@code default: null}, even if the
          // source field had no explicit default; required fields keep their
          // existing default (or none).
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
   * oldType) — both the parent's full HoodieSchema — so callers can build a
   * cast plan from {@code oldType} to {@code newType}. Walks ids on the
   * HoodieSchema accessors directly.
   */
  public static Map<Integer, Pair<HoodieSchema, HoodieSchema>> collectTypeChangedCols(HoodieSchema schema, HoodieSchema oldSchema) {
    Set<Integer> ids = schema.getAllIds();
    Set<Integer> otherIds = oldSchema.getAllIds();
    Map<Integer, Pair<HoodieSchema, HoodieSchema>> result = new HashMap<>();
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
          schema.findType(parentName),
          oldSchema.findType(otherParentName)));
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
  // HoodieSchema-direct reconciliation. Walks {@link HoodieSchema} via the
  // applier surface for adds, type promotions, and nullability flips.
  // Production callers route here via the public
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
    HoodieSchema incoming = withFreshIds(incomingSchema);

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
      // Strip ids from the colType so the sub-tree's stamped ids don't
      // collide with old's ids when adopted into the rebuilt parent record.
      // assignFresh after the add round mints fresh ids for the now-id-less
      // sub-tree.
      HoodieSchema colType = stripIds(incoming.findType(name));
      // Append new columns at the end of their parent record, matching the
      // legacy reconcile semantics. Position-aware insertion is the caller's
      // job (via {@link HoodieSchemaChangeApplier#applyAddChange} directly).
      afterAdds = new HoodieSchemaChangeApplier(afterAdds).applyAddChange(name, colType, null, null, ColumnPositionType.NO_OPERATION);
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

  /**
   * Returns a deep-copied HoodieSchema with fresh ids stamped on every
   * field / element / key / value position. The deep copy goes through
   * Avro JSON parse so the underlying {@code Schema.Field} instances are
   * independent of the source — important because Avro forbids reusing a
   * Schema.Field across two records, and because {@link HoodieSchemaIdAssigner}
   * mutates Avro custom props in place.
   */
  private static HoodieSchema withFreshIds(HoodieSchema source) {
    HoodieSchema copy = HoodieSchema.parse(source.toAvroSchema().toString());
    HoodieSchemaIdAssigner.assignFresh(copy);
    return copy;
  }

  /**
   * True when {@code fullName}'s terminal segment is the array-element or
   * map-value descent pseudo-segment ({@code "element"} or {@code "value"}).
   * Segment-exact, so a real field name like {@code "value_count"} doesn't
   * match.
   */
  private static boolean endsWithArrayOrMapDescent(String fullName) {
    int dot = fullName.lastIndexOf('.');
    if (dot < 0) {
      return false;
    }
    String last = fullName.substring(dot + 1);
    return "element".equals(last) || "value".equals(last);
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

  // -------------------------------------------------------------------------
  // Helpers for reconcileSchemaRequirements: type-promotion check and a
  // record-tree walker that reorders source's record fields to match target's
  // positional layout (with names absent from target landing at the end).
  // -------------------------------------------------------------------------

  /**
   * True when src is a primitive that can be widened into dst. Nested types
   * and equal types return false.
   */
  private static boolean shouldPromoteHoodieType(HoodieSchema src, HoodieSchema dst) {
    HoodieSchema srcEffective = src.isNullable() ? src.getNonNullType() : src;
    HoodieSchema dstEffective = dst.isNullable() ? dst.getNonNullType() : dst;
    if (srcEffective.getAvroSchema().equals(dstEffective.getAvroSchema())) {
      return false;
    }
    if (isNestedType(srcEffective) || isNestedType(dstEffective)) {
      return false;
    }
    HoodieSchemaType srcT = srcEffective.getType();
    HoodieSchemaType dstT = dstEffective.getType();
    switch (srcT) {
      case INT:
        return dstT == HoodieSchemaType.LONG || dstT == HoodieSchemaType.FLOAT
            || dstT == HoodieSchemaType.DOUBLE || dstT == HoodieSchemaType.STRING
            || dstT == HoodieSchemaType.DECIMAL;
      case LONG:
        return dstT == HoodieSchemaType.FLOAT || dstT == HoodieSchemaType.DOUBLE
            || dstT == HoodieSchemaType.STRING || dstT == HoodieSchemaType.DECIMAL;
      case FLOAT:
        return dstT == HoodieSchemaType.DOUBLE || dstT == HoodieSchemaType.STRING
            || dstT == HoodieSchemaType.DECIMAL;
      case DOUBLE:
        return dstT == HoodieSchemaType.STRING || dstT == HoodieSchemaType.DECIMAL;
      case DATE:
      case BYTES:
      case ENUM:
        // ENUM → STRING replays the legacy InternalSchemaConverter flatten.
        return dstT == HoodieSchemaType.STRING;
      case DECIMAL:
        if (dstT == HoodieSchemaType.STRING) {
          return true;
        }
        if (dstT != HoodieSchemaType.DECIMAL) {
          return false;
        }
        return isDecimalPromotion((HoodieSchema.Decimal) srcEffective, (HoodieSchema.Decimal) dstEffective);
      case STRING:
        return dstT == HoodieSchemaType.DATE
            || dstT == HoodieSchemaType.DECIMAL
            || dstT == HoodieSchemaType.BYTES;
      default:
        return false;
    }
  }

  /**
   * Decimal-to-decimal widening matching legacy
   * {@code Types.DecimalBase.isWiderThan} + the {@code precision >= && scale ==}
   * fallback in {@code isDecimalUpdateAllowInternalBase}.
   */
  private static boolean isDecimalPromotion(HoodieSchema.Decimal src, HoodieSchema.Decimal dst) {
    if (dst.getScale() == src.getScale() && dst.getPrecision() >= src.getPrecision()) {
      return true;
    }
    return (dst.getPrecision() - dst.getScale()) >= (src.getPrecision() - src.getScale())
        && dst.getScale() > src.getScale();
  }

  /**
   * Returns a fresh HoodieSchema mirroring {@code source} but with each
   * record's fields reordered to match {@code targetPositions}. Fields whose
   * full name isn't in target land at the end. Recurses into nested records,
   * arrays (under "element" segment), and maps (under "value" segment).
   */
  private static HoodieSchema reorderToTargetLayout(HoodieSchema source, Map<String, Integer> targetPositions) {
    return reorderInner(source, "", targetPositions);
  }

  /**
   * Walks {@code source} and at every primitive leaf whose full name is in
   * {@code leafUpdates} swaps the leaf's effective type with the supplied
   * target type, preserving source's nullability wrapper. Other fields pass
   * through unchanged.
   *
   * <p>Path construction matches {@link HoodieSchemaIndex}: dot-separated
   * record fields, "element" for array element descent, "value" for map
   * value descent. Custom Avro props on rebuilt record fields are preserved
   * (so field-id survives across the rebuild).</p>
   */
  private static HoodieSchema overlayLeafTypes(HoodieSchema schema, String pathPrefix, Map<String, HoodieSchema> leafUpdates) {
    boolean nullable = schema.isNullable();
    HoodieSchema effective = nullable ? schema.getNonNullType() : schema;
    HoodieSchema rebuilt;
    switch (effective.getType()) {
      case RECORD: {
        List<HoodieSchemaField> newFields = new ArrayList<>(effective.getFields().size());
        for (HoodieSchemaField f : effective.getFields()) {
          String childPath = pathPrefix.isEmpty() ? f.name() : pathPrefix + "." + f.name();
          HoodieSchema childUpdated = overlayLeafTypes(f.schema(), childPath, leafUpdates);
          HoodieSchemaField rb = HoodieSchemaField.of(
              f.name(), childUpdated, f.doc().orElse(null), f.defaultVal().orElse(null));
          for (Map.Entry<String, Object> e : f.getObjectProps().entrySet()) {
            rb.addProp(e.getKey(), e.getValue());
          }
          newFields.add(rb);
        }
        rebuilt = HoodieSchema.createRecord(effective.getName(), effective.getNamespace().orElse(null),
            effective.getDoc().orElse(null), newFields);
        break;
      }
      case ARRAY: {
        String elementPath = pathPrefix.isEmpty() ? "element" : pathPrefix + ".element";
        HoodieSchema newElement = overlayLeafTypes(effective.getElementType(), elementPath, leafUpdates);
        rebuilt = HoodieSchema.createArray(newElement);
        Object eId = effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP);
        if (eId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, ((Number) eId).intValue());
        }
        break;
      }
      case MAP: {
        String valuePath = pathPrefix.isEmpty() ? "value" : pathPrefix + ".value";
        HoodieSchema newValue = overlayLeafTypes(effective.getValueType(), valuePath, leafUpdates);
        rebuilt = HoodieSchema.createMap(newValue);
        Object kId = effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP);
        Object vId = effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP);
        if (kId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, ((Number) kId).intValue());
        }
        if (vId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, ((Number) vId).intValue());
        }
        break;
      }
      default:
        // Primitive leaf — overlay if there's an update at this path.
        HoodieSchema target = leafUpdates.get(pathPrefix);
        if (target == null) {
          return schema;
        }
        HoodieSchema targetEffective = target.isNullable() ? target.getNonNullType() : target;
        return nullable ? HoodieSchema.createNullable(targetEffective) : targetEffective;
    }
    return nullable ? HoodieSchema.createNullable(rebuilt) : rebuilt;
  }

  /**
   * Walks {@code schema} and toggles nullability of array elements / map values
   * whose path matches an entry in {@code paths}. Path construction matches
   * {@link #overlayLeafTypes}: dot-separated record fields, "element" for array
   * descent, "value" for map descent. ELEMENT_ID / KEY_ID / VALUE_ID props on
   * rebuilt array / map sub-trees are preserved; field-id and other field props
   * are preserved when records are rebuilt.
   */
  private static HoodieSchema applyDescentNullabilityFlips(HoodieSchema schema, String pathPrefix,
                                                           Set<String> paths, boolean nullable) {
    boolean wasNullable = schema.isNullable();
    HoodieSchema effective = wasNullable ? schema.getNonNullType() : schema;
    HoodieSchema rebuilt;
    switch (effective.getType()) {
      case RECORD: {
        List<HoodieSchemaField> newFields = new ArrayList<>(effective.getFields().size());
        for (HoodieSchemaField f : effective.getFields()) {
          String childPath = pathPrefix.isEmpty() ? f.name() : pathPrefix + "." + f.name();
          HoodieSchema childUpdated = applyDescentNullabilityFlips(f.schema(), childPath, paths, nullable);
          HoodieSchemaField rb = HoodieSchemaField.of(
              f.name(), childUpdated, f.doc().orElse(null), f.defaultVal().orElse(null));
          for (Map.Entry<String, Object> e : f.getObjectProps().entrySet()) {
            rb.addProp(e.getKey(), e.getValue());
          }
          newFields.add(rb);
        }
        rebuilt = HoodieSchema.createRecord(effective.getName(), effective.getNamespace().orElse(null),
            effective.getDoc().orElse(null), newFields);
        break;
      }
      case ARRAY: {
        String elementPath = pathPrefix.isEmpty() ? "element" : pathPrefix + ".element";
        HoodieSchema newElement = applyDescentNullabilityFlips(effective.getElementType(), elementPath, paths, nullable);
        if (paths.contains(elementPath)) {
          boolean elementWasNullable = newElement.isNullable();
          HoodieSchema elementEffective = elementWasNullable ? newElement.getNonNullType() : newElement;
          newElement = nullable ? HoodieSchema.createNullable(elementEffective) : elementEffective;
        }
        rebuilt = HoodieSchema.createArray(newElement);
        Object eId = effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP);
        if (eId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, ((Number) eId).intValue());
        }
        break;
      }
      case MAP: {
        String valuePath = pathPrefix.isEmpty() ? "value" : pathPrefix + ".value";
        HoodieSchema newValue = applyDescentNullabilityFlips(effective.getValueType(), valuePath, paths, nullable);
        if (paths.contains(valuePath)) {
          boolean valueWasNullable = newValue.isNullable();
          HoodieSchema valueEffective = valueWasNullable ? newValue.getNonNullType() : newValue;
          newValue = nullable ? HoodieSchema.createNullable(valueEffective) : valueEffective;
        }
        rebuilt = HoodieSchema.createMap(newValue);
        Object kId = effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP);
        Object vId = effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP);
        if (kId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, ((Number) kId).intValue());
        }
        if (vId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, ((Number) vId).intValue());
        }
        break;
      }
      default:
        return schema;
    }
    return wasNullable ? HoodieSchema.createNullable(rebuilt) : rebuilt;
  }

  private static HoodieSchema reorderInner(HoodieSchema source, String pathPrefix, Map<String, Integer> targetPositions) {
    HoodieSchema effective = source.isNullable() ? source.getNonNullType() : source;
    HoodieSchema rebuilt;
    switch (effective.getType()) {
      case RECORD: {
        List<HoodieSchemaField> sorted = new ArrayList<>(effective.getFields());
        sorted.sort(Comparator.comparingInt(f -> targetPositions.getOrDefault(pathPrefix + f.name(), Integer.MAX_VALUE)));
        List<HoodieSchemaField> newFields = new ArrayList<>(sorted.size());
        for (HoodieSchemaField f : sorted) {
          HoodieSchema newSub = reorderInner(f.schema(), pathPrefix + f.name() + ".", targetPositions);
          HoodieSchemaField rb = HoodieSchemaField.of(
              f.name(), newSub, f.doc().orElse(null), f.defaultVal().orElse(null));
          for (Map.Entry<String, Object> e : f.getObjectProps().entrySet()) {
            rb.addProp(e.getKey(), e.getValue());
          }
          newFields.add(rb);
        }
        rebuilt = HoodieSchema.createRecord(effective.getName(), effective.getNamespace().orElse(null),
            effective.getDoc().orElse(null), newFields);
        break;
      }
      case ARRAY: {
        HoodieSchema newElement = reorderInner(effective.getElementType(), pathPrefix + "element.", targetPositions);
        rebuilt = HoodieSchema.createArray(newElement);
        Object eId = effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP);
        if (eId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, ((Number) eId).intValue());
        }
        break;
      }
      case MAP: {
        HoodieSchema newValue = reorderInner(effective.getValueType(), pathPrefix + "value.", targetPositions);
        rebuilt = HoodieSchema.createMap(newValue);
        Object kId = effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP);
        Object vId = effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP);
        if (kId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, ((Number) kId).intValue());
        }
        if (vId instanceof Number) {
          rebuilt.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, ((Number) vId).intValue());
        }
        break;
      }
      default:
        return source;
    }
    return source.isNullable() ? HoodieSchema.createNullable(rebuilt) : rebuilt;
  }
}
