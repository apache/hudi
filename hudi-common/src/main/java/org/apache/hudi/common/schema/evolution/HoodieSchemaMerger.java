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
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaIdAssigner;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-path schema merger: combines a file's stored schema with a query schema
 * (and tracks renamed fields) to produce the schema actually used to decode rows
 * from a base file or log block.
 *
 * <p>This is the read-path linchpin: {@code HoodieFileGroupReader},
 * {@code AbstractHoodieLogRecordScanner}, and the parquet readers all converge
 * on this interface. The algorithm walks the query schema and, for each field id,
 * either keeps the field as-is, projects file-side names/types onto it (renames
 * and type changes), suffix-disambiguates a query-only field that collides with
 * a name still present on the file side, or marks it nullable as an
 * "added" column.
 *
 * <p>Implementation walks {@link HoodieSchema} directly via
 * {@link HoodieSchema#findField(int)} / {@link HoodieSchema#findField(String)} /
 * {@link HoodieSchema#findFullName(int)} / {@link HoodieSchema#findIdByName(String)}.
 */
public class HoodieSchemaMerger {

  private final HoodieSchema fileSchema;
  private final HoodieSchema querySchema;
  // When the Spark update/merge API flips a column's nullability from optional
  // to required, the merger should still treat it as optional. Disabled for
  // strictly-typed callers that want the original required-ness preserved.
  private final boolean ignoreRequiredAttribute;
  // For columns whose type changed, prefer the file's type during decode (so
  // the parquet/avro reader sees the on-disk type) and let downstream record
  // rewriters apply the promotion. Disabled for log readers that promote at
  // record-rewrite time directly via reWriteRecordWithNewSchema.
  private final boolean useColumnTypeFromFileSchema;
  // For renamed columns, prefer the file's name during decode so the parquet
  // reader can locate the column. Disabled for log readers (which can read by
  // the new name and then rewrite).
  private final boolean useColNameFromFileSchema;

  private final Map<String, String> renamedFields = new HashMap<>();

  public HoodieSchemaMerger(HoodieSchema fileSchema,
                            HoodieSchema querySchema,
                            boolean ignoreRequiredAttribute,
                            boolean useColumnTypeFromFileSchema,
                            boolean useColNameFromFileSchema) {
    this.fileSchema = ensureFileSchemaHasIds(fileSchema);
    this.querySchema = querySchema;
    this.ignoreRequiredAttribute = ignoreRequiredAttribute;
    this.useColumnTypeFromFileSchema = useColumnTypeFromFileSchema;
    this.useColNameFromFileSchema = useColNameFromFileSchema;
  }

  /**
   * Replicates legacy {@code InternalSchemaConverter.fromAvro} behavior: when the
   * file schema carries no field ids at all (e.g. parquet's natural Avro schema
   * derived from the file's MessageType has no {@code field-id} custom props),
   * mint sequential ids positionally starting at 0. The querySchema's ids were
   * also assigned positionally starting at 0 at table-create time and preserved
   * across renames, so id 0..N on the file side aligns with id 0..N on the
   * query side for unchanged columns — letting the id-based merge detect renames
   * (same id, different name) instead of mis-classifying them as drop+add.
   *
   * <p>Cloned so we don't mutate the caller's HoodieSchema. Partially-IDed
   * schemas pass through unchanged: this only fires when no ids are present.</p>
   */
  private static HoodieSchema ensureFileSchemaHasIds(HoodieSchema fileSchema) {
    if (fileSchema == null || !fileSchema.getAllIds().isEmpty()) {
      return fileSchema;
    }
    HoodieSchema clone = HoodieSchema.parse(fileSchema.toAvroSchema().toString());
    HoodieSchemaIdAssigner.assign(clone, 0);
    return clone;
  }

  public HoodieSchemaMerger(HoodieSchema fileSchema,
                            HoodieSchema querySchema,
                            boolean ignoreRequiredAttribute,
                            boolean useColumnTypeFromFileSchema) {
    this(fileSchema, querySchema, ignoreRequiredAttribute, useColumnTypeFromFileSchema, true);
  }

  /**
   * Produces the merged read schema. Field ids carry through from the query schema;
   * column names and types follow the {@code useCol*FromFileSchema} flags set at
   * construction time.
   */
  public HoodieSchema mergeSchema() {
    HoodieSchema unwrapped = querySchema.isNullable() ? querySchema.getNonNullType() : querySchema;
    HoodieSchema merged = mergeRecord(unwrapped, querySchema.getFullName());
    if (querySchema.schemaId() >= 0) {
      merged.setSchemaId(querySchema.schemaId());
    }
    if (querySchema.maxColumnId() >= 0) {
      merged.setMaxColumnId(querySchema.maxColumnId());
    }
    merged.invalidateIdIndex();
    return merged;
  }

  /**
   * Same as {@link #mergeSchema()} but additionally returns the rename map
   * (query-side full name → file-side leaf name) so downstream record rewriters
   * can project correctly across renames.
   */
  public Pair<HoodieSchema, Map<String, String>> mergeSchemaGetRenamed() {
    HoodieSchema merged = mergeSchema();
    return Pair.of(merged, renamedFields);
  }

  public HoodieSchema getFileSchema() {
    return fileSchema;
  }

  public HoodieSchema getQuerySchema() {
    return querySchema;
  }

  // -------------------------------------------------------------------------
  // Tree walk over the query schema. Each leaf id is checked against the file
  // schema; the result is rebuilt structurally.
  // -------------------------------------------------------------------------

  /**
   * Recursively merges the type at a given query-side id. The id is the field
   * id at the parent record (or array element / map value id when descending
   * into nested types). For primitives, the id picks a candidate type from
   * the file schema; for compound types, recursion drives the structure.
   */
  private HoodieSchema mergeType(HoodieSchema type, int currentTypeId) {
    HoodieSchema effective = type.isNullable() ? type.getNonNullType() : type;
    switch (effective.getType()) {
      case RECORD:
        return mergeRecord(effective, effective.getFullName());
      case ARRAY:
        return mergeArray(effective);
      case MAP:
        return mergeMap(effective);
      default:
        return mergePrimitive(effective, currentTypeId);
    }
  }

  private HoodieSchema mergeRecord(HoodieSchema record, String recordName) {
    List<HoodieSchemaField> oldFields = record.getFields();
    List<HoodieSchema> newTypes = new ArrayList<>(oldFields.size());
    for (HoodieSchemaField queryField : oldFields) {
      newTypes.add(mergeType(queryField.schema(), queryField.fieldId()));
    }
    List<HoodieSchemaField> newFields = buildRecordFields(oldFields, newTypes);
    return HoodieSchema.createRecord(
        recordName == null || recordName.isEmpty() ? "hoodieSchema" : recordName,
        null, null, false, newFields);
  }

  private HoodieSchema mergeArray(HoodieSchema array) {
    HoodieSchema elementType = array.getElementType();
    int elementId = readIntProp(array.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP));
    boolean elementOptional = elementType.isNullable();
    HoodieSchema mergedElement = mergeType(elementType, elementId);
    HoodieSchema effectiveMergedElement = elementOptional ? HoodieSchema.createNullable(mergedElement) : mergedElement;
    HoodieSchema result = HoodieSchema.createArray(effectiveMergedElement);
    if (elementId >= 0) {
      result.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, elementId);
    }
    return result;
  }

  private HoodieSchema mergeMap(HoodieSchema map) {
    HoodieSchema valueType = map.getValueType();
    int keyId = readIntProp(map.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP));
    int valueId = readIntProp(map.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP));
    boolean valueOptional = valueType.isNullable();
    HoodieSchema mergedValue = mergeType(valueType, valueId);
    HoodieSchema effectiveMergedValue = valueOptional ? HoodieSchema.createNullable(mergedValue) : mergedValue;
    HoodieSchema result = HoodieSchema.createMap(effectiveMergedValue);
    if (keyId >= 0) {
      result.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, keyId);
    }
    if (valueId >= 0) {
      result.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, valueId);
    }
    return result;
  }

  /**
   * Per-id record-field rebuild. Mirrors the legacy {@code buildRecordType}
   * algorithm: same id → check name match (rename detection), id missing on
   * file side → check name collision (suffix disambiguator), id+name both
   * absent → emit as a new (added) column.
   */
  private List<HoodieSchemaField> buildRecordFields(List<HoodieSchemaField> oldFields, List<HoodieSchema> newTypes) {
    List<HoodieSchemaField> newFields = new ArrayList<>(newTypes.size());
    for (int i = 0; i < newTypes.size(); i++) {
      HoodieSchemaField oldField = oldFields.get(i);
      HoodieSchema newType = newTypes.get(i);
      int fieldId = oldField.fieldId();
      String fullName = querySchema.findFullName(fieldId);
      String fileFullName = fileSchema.findFullName(fieldId);
      boolean fileHasId = !fileFullName.isEmpty();
      if (fileHasId) {
        if (fileFullName.equals(fullName)) {
          // Same name on both sides — only the type may have changed; keep id and name.
          newFields.add(rebuildField(oldField, oldField.name(), newType, oldField.schema().isNullable()));
        } else {
          newFields.add(dealWithRename(fieldId, newType, oldField, fileFullName));
        }
      } else {
        // Field id not present in file. It's either truly new (added column) or
        // a name collision with an old, dropped-then-readded column whose name
        // already exists in the file with a different id. Disambiguate the
        // latter by appending "suffix" so the parquet reader doesn't pick up
        // the old column's bytes for the new column.
        String normalized = normalizeFullName(fullName);
        if (fileSchema.findIdByName(normalized) >= 0) {
          // Use the original sub-schema (oldField.schema()) so the suffixed
          // synthetic column carries the old shape, not the merged one.
          newFields.add(rebuildField(oldField, oldField.name() + "suffix", oldField.schema().getNonNullType(), oldField.schema().isNullable()));
        } else {
          // New column. Honor the optional override that papers over the
          // Spark update/merge nullability flip when ignoreRequiredAttribute is set.
          boolean optional = ignoreRequiredAttribute || oldField.schema().isNullable();
          newFields.add(rebuildField(oldField, oldField.name(), newType, optional));
        }
      }
    }
    return newFields;
  }

  private HoodieSchemaField dealWithRename(int fieldId, HoodieSchema newType, HoodieSchemaField oldField, String fileFullName) {
    String nameFromFileSchema = leafOf(fileFullName);
    String nameFromQuerySchema = oldField.name();
    String finalFieldName = useColNameFromFileSchema ? nameFromFileSchema : nameFromQuerySchema;
    // findType(int) returns the type at the id (may itself be a [null,T] union
    // for nullable record fields). Unwrap to mirror the legacy semantics that
    // pulled Types.Field.type() directly.
    HoodieSchema typeFromFileSchema = fileSchema.findType(fieldId);
    HoodieSchema typeFromFileNonNull = typeFromFileSchema == null
        ? null
        : (typeFromFileSchema.isNullable() ? typeFromFileSchema.getNonNullType() : typeFromFileSchema);
    if (!useColNameFromFileSchema) {
      // Use the full name as the rename key to disambiguate composite-rename
      // scenarios. Example: original schema ROW<f_row: ROW<f1 STRING, f2 BIGINT>, f3 STRING>;
      // rename f_row.f1 -> f_row.f_str AND f3 -> f_str. Keying renamedFields by
      // leaf name alone would have the second rename overwrite the first.
      renamedFields.put(querySchema.findFullName(fieldId), nameFromFileSchema);
    }
    boolean optional = oldField.schema().isNullable();
    // Nested-type changes aren't allowed in current schema-evolution rules, so
    // if the new type is nested we know it's structurally identical to the file
    // and we just project the merged sub-schema onto the renamed field name.
    if (isNested(newType)) {
      return rebuildField(oldField, finalFieldName, newType, optional);
    }
    HoodieSchema chosen = (useColumnTypeFromFileSchema && typeFromFileNonNull != null) ? typeFromFileNonNull : newType;
    return rebuildField(oldField, finalFieldName, chosen, optional);
  }

  /** Returns the last dot-separated component of a full name. */
  private static String leafOf(String fullName) {
    int dot = fullName.lastIndexOf('.');
    return dot < 0 ? fullName : fullName.substring(dot + 1);
  }

  /**
   * Walks each prefix of {@code fullName} and substitutes any parent name that
   * was renamed on the file side, so a query-side full name like
   * {@code aa.d} (where {@code aa} used to be {@code a}) resolves to
   * {@code a.d} for the file-side lookup. Without this, dropped-and-readded
   * leaves under a renamed parent wouldn't trigger the suffix disambiguator.
   */
  private String normalizeFullName(String fullName) {
    String[] nameParts = fullName.split("\\.");
    String[] normalizedNameParts = new String[nameParts.length];
    System.arraycopy(nameParts, 0, normalizedNameParts, 0, nameParts.length);
    for (int j = 0; j < nameParts.length - 1; j++) {
      StringBuilder sb = new StringBuilder();
      for (int k = 0; k <= j; k++) {
        sb.append(nameParts[k]);
      }
      String parentName = sb.toString();
      int parentFieldIdFromQuerySchema = querySchema.findIdByName(parentName);
      String parentNameFromFileSchema = fileSchema.findFullName(parentFieldIdFromQuerySchema);
      if (parentNameFromFileSchema.isEmpty()) {
        break;
      }
      if (!parentNameFromFileSchema.equalsIgnoreCase(parentName)) {
        String[] parentNameParts = parentNameFromFileSchema.split("\\.");
        System.arraycopy(parentNameParts, 0, normalizedNameParts, 0, parentNameParts.length);
      }
    }
    return StringUtils.join(normalizedNameParts, ".");
  }

  /**
   * Picks the per-cell primitive type. When the file schema has a value at
   * this id, prefer it (so the parquet reader sees the on-disk type) unless
   * the caller explicitly disabled that via {@code useColumnTypeFromFileSchema}.
   */
  private HoodieSchema mergePrimitive(HoodieSchema typeFromQuerySchema, int currentPrimitiveTypeId) {
    HoodieSchema typeFromFileSchema = fileSchema.findType(currentPrimitiveTypeId);
    if (typeFromFileSchema == null) {
      return typeFromQuerySchema;
    }
    HoodieSchema fileEffective = typeFromFileSchema.isNullable() ? typeFromFileSchema.getNonNullType() : typeFromFileSchema;
    return useColumnTypeFromFileSchema ? fileEffective : typeFromQuerySchema;
  }

  /**
   * Builds a new HoodieSchemaField at the same id as {@code oldField} but with
   * the supplied name and (possibly nullability-wrapped) sub-schema. The
   * field-id Avro property is preserved.
   */
  private static HoodieSchemaField rebuildField(HoodieSchemaField oldField,
                                                String name,
                                                HoodieSchema schema,
                                                boolean optional) {
    HoodieSchema effective = schema.isNullable() ? schema.getNonNullType() : schema;
    HoodieSchema finalSchema = optional ? HoodieSchema.createNullable(effective) : effective;
    HoodieSchemaField rebuilt = HoodieSchemaField.of(name, finalSchema, oldField.doc().orElse(null), oldField.defaultVal().orElse(null));
    if (oldField.fieldId() >= 0) {
      rebuilt.addProp(HoodieSchema.FIELD_ID_PROP, oldField.fieldId());
    }
    return rebuilt;
  }

  private static boolean isNested(HoodieSchema schema) {
    HoodieSchemaType t = (schema.isNullable() ? schema.getNonNullType() : schema).getType();
    return t == HoodieSchemaType.RECORD || t == HoodieSchemaType.ARRAY || t == HoodieSchemaType.MAP;
  }

  private static int readIntProp(Object raw) {
    return raw instanceof Number ? ((Number) raw).intValue() : -1;
  }
}
