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
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HoodieSchema-direct utilities for id-preserving prune and rename operations
 * used by the schema-on-read read path.
 */
public final class HoodieSchemaInternalSchemaBridge {

  private HoodieSchemaInternalSchemaBridge() {
  }

  private static int readIntProp(Object raw, int fallback) {
    return raw instanceof Number ? ((Number) raw).intValue() : fallback;
  }

  /**
   * Prunes {@code source} to the supplied leaf-name list, preserving field ids.
   * The returned HoodieSchema's record name is taken from {@code source}.
   *
   * <p>Each leaf name resolves to a column id via {@link HoodieSchema#findIdByName};
   * the walk then keeps a record field iff its id is in the keep-set or any of its
   * descendants are. Top-level fields are emitted in first-seen order of the
   * leaf-name list (to match the legacy semantics that callers depend on for
   * column ordering after prune). Throws if any leaf name doesn't resolve.</p>
   */
  public static HoodieSchema pruneByLeafNames(HoodieSchema source, List<String> leafNames) {
    Set<Integer> keepIds = new HashSet<>();
    for (String name : leafNames) {
      int id = source.findIdByName(name);
      if (id == -1) {
        throw new IllegalArgumentException(
            String.format("cannot prune col: %s which does not exist in hudi table", name));
      }
      keepIds.add(id);
    }
    // Top-level parent ids in first-seen order so the output preserves the
    // ordering of the input leafNames (rather than the source's declared order).
    List<Integer> topParentFieldIds = new ArrayList<>();
    for (String name : leafNames) {
      int id = source.findIdByName(name.split("\\.")[0]);
      if (!topParentFieldIds.contains(id)) {
        topParentFieldIds.add(id);
      }
    }
    HoodieSchema effective = source.isNullable() ? source.getNonNullType() : source;
    return prunedRecord(effective, keepIds, topParentFieldIds, source.getFullName(),
        source.schemaId(), source.maxColumnId());
  }

  /**
   * Prunes {@code source} down to the leaves of {@code requiredSchema}, preserving
   * field ids. Returns a HoodieSchema named after {@code requiredSchema}.
   */
  public static HoodieSchema pruneByRequiredSchema(HoodieSchema source, HoodieSchema requiredSchema) {
    List<String> leafNames = HoodieSchemaUtils.collectLeafNames(requiredSchema);
    Set<Integer> keepIds = new HashSet<>();
    for (String name : leafNames) {
      int id = source.findIdByName(name);
      if (id == -1) {
        throw new IllegalArgumentException(
            String.format("cannot prune col: %s which does not exist in hudi table", name));
      }
      keepIds.add(id);
    }
    List<Integer> topParentFieldIds = new ArrayList<>();
    for (String name : leafNames) {
      int id = source.findIdByName(name.split("\\.")[0]);
      if (!topParentFieldIds.contains(id)) {
        topParentFieldIds.add(id);
      }
    }
    HoodieSchema effective = source.isNullable() ? source.getNonNullType() : source;
    return prunedRecord(effective, keepIds, topParentFieldIds, requiredSchema.getFullName(),
        source.schemaId(), source.maxColumnId());
  }

  /**
   * Walks the source record, pruning sub-trees to match {@code keepIds}, then
   * emits the surviving top-level fields in {@code topParentFieldIds} order.
   * The returned record's schemaId / maxColumnId carry over from the source so
   * downstream code that round-trips through {@link HoodieSchemaSerDe} or the
   * cache continues to see consistent version metadata.
   */
  private static HoodieSchema prunedRecord(HoodieSchema record,
                                            Set<Integer> keepIds,
                                            List<Integer> topParentFieldIds,
                                            String recordName,
                                            long sourceSchemaId,
                                            int sourceMaxColumnId) {
    // First pass: prune each top-level field's sub-tree, indexed by field id so
    // we can re-emit in topParentFieldIds order.
    Map<Integer, HoodieSchemaField> idToPruned = new HashMap<>();
    for (HoodieSchemaField field : record.getFields()) {
      HoodieSchemaField pruned = pruneField(field, keepIds);
      if (pruned != null) {
        idToPruned.put(field.fieldId(), pruned);
      }
    }
    List<HoodieSchemaField> orderedFields = new ArrayList<>(topParentFieldIds.size());
    if (topParentFieldIds.isEmpty()) {
      // Defensive: empty leafNames → empty schema. Mirrors legacy behavior.
      orderedFields.addAll(idToPruned.values());
    } else {
      for (int id : topParentFieldIds) {
        HoodieSchemaField f = idToPruned.get(id);
        if (f == null) {
          throw new org.apache.hudi.exception.HoodieSchemaException(
              String.format("cannot find pruned id %s in currentSchema %s", id, record));
        }
        orderedFields.add(f);
      }
    }
    String simpleName;
    String namespace;
    int lastDot = recordName == null ? -1 : recordName.lastIndexOf('.');
    if (lastDot < 0) {
      simpleName = recordName == null || recordName.isEmpty() ? "hoodieSchema" : recordName;
      namespace = record.getNamespace().orElse(null);
    } else {
      namespace = recordName.substring(0, lastDot);
      simpleName = recordName.substring(lastDot + 1);
    }
    HoodieSchema result = HoodieSchema.createRecord(simpleName, namespace, record.getDoc().orElse(null), orderedFields);
    if (sourceSchemaId >= 0) {
      result.setSchemaId(sourceSchemaId);
    }
    if (sourceMaxColumnId >= 0) {
      result.setMaxColumnId(sourceMaxColumnId);
    }
    result.invalidateIdIndex();
    return result;
  }

  /**
   * Prunes a record field's sub-tree. Returns the rebuilt field, or {@code null}
   * when neither the field's own id nor any descendant is in the keep-set —
   * meaning the field has no surviving content and should be dropped.
   */
  private static HoodieSchemaField pruneField(HoodieSchemaField field, Set<Integer> keepIds) {
    HoodieSchema schema = field.schema();
    boolean optional = schema.isNullable();
    HoodieSchema effective = optional ? schema.getNonNullType() : schema;
    HoodieSchema prunedType;
    switch (effective.getType()) {
      case RECORD:
        prunedType = pruneRecordType(effective, keepIds);
        break;
      case ARRAY:
        prunedType = pruneArrayType(effective, keepIds);
        break;
      case MAP:
        prunedType = pruneMapType(effective, keepIds);
        break;
      default:
        // Primitive — survive iff the field's id is in the keep-set.
        prunedType = keepIds.contains(field.fieldId()) ? effective : null;
        break;
    }
    if (prunedType == null && !keepIds.contains(field.fieldId())) {
      return null;
    }
    if (prunedType == null) {
      // Field id is in keep-set but children prune to nothing — keep the
      // original sub-schema so the column is fully materialized.
      prunedType = effective;
    }
    HoodieSchema finalSchema = optional ? HoodieSchema.createNullable(prunedType) : prunedType;
    HoodieSchemaField rebuilt = HoodieSchemaField.of(
        field.name(), finalSchema, field.doc().orElse(null), field.defaultVal().orElse(null));
    if (field.fieldId() >= 0) {
      rebuilt.addProp(HoodieSchema.FIELD_ID_PROP, field.fieldId());
    }
    return rebuilt;
  }

  private static HoodieSchema pruneRecordType(HoodieSchema record, Set<Integer> keepIds) {
    List<HoodieSchemaField> kept = new ArrayList<>();
    for (HoodieSchemaField field : record.getFields()) {
      HoodieSchemaField pruned = pruneField(field, keepIds);
      if (pruned != null) {
        kept.add(pruned);
      }
    }
    if (kept.isEmpty()) {
      return null;
    }
    return HoodieSchema.createRecord(record.getName(), record.getNamespace().orElse(null), record.getDoc().orElse(null), kept);
  }

  private static HoodieSchema pruneArrayType(HoodieSchema array, Set<Integer> keepIds) {
    int elementId = readIntProp(array.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP), -1);
    HoodieSchema elementType = array.getElementType();
    boolean elementOptional = elementType.isNullable();
    HoodieSchema elementInner = elementOptional ? elementType.getNonNullType() : elementType;
    HoodieSchema prunedElement;
    switch (elementInner.getType()) {
      case RECORD:
        prunedElement = pruneRecordType(elementInner, keepIds);
        break;
      case ARRAY:
        prunedElement = pruneArrayType(elementInner, keepIds);
        break;
      case MAP:
        prunedElement = pruneMapType(elementInner, keepIds);
        break;
      default:
        prunedElement = keepIds.contains(elementId) ? elementInner : null;
        break;
    }
    if (prunedElement == null && !keepIds.contains(elementId)) {
      return null;
    }
    if (prunedElement == null) {
      prunedElement = elementInner;
    }
    HoodieSchema effectiveElement = elementOptional ? HoodieSchema.createNullable(prunedElement) : prunedElement;
    HoodieSchema result = HoodieSchema.createArray(effectiveElement);
    if (elementId >= 0) {
      result.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, elementId);
    }
    return result;
  }

  private static HoodieSchema pruneMapType(HoodieSchema map, Set<Integer> keepIds) {
    int keyId = readIntProp(map.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP), -1);
    int valueId = readIntProp(map.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP), -1);
    HoodieSchema valueType = map.getValueType();
    boolean valueOptional = valueType.isNullable();
    HoodieSchema valueInner = valueOptional ? valueType.getNonNullType() : valueType;
    HoodieSchema prunedValue;
    switch (valueInner.getType()) {
      case RECORD:
        prunedValue = pruneRecordType(valueInner, keepIds);
        break;
      case ARRAY:
        prunedValue = pruneArrayType(valueInner, keepIds);
        break;
      case MAP:
        prunedValue = pruneMapType(valueInner, keepIds);
        break;
      default:
        prunedValue = keepIds.contains(valueId) ? valueInner : null;
        break;
    }
    if (prunedValue == null && !keepIds.contains(valueId)) {
      return null;
    }
    if (prunedValue == null) {
      prunedValue = valueInner;
    }
    HoodieSchema effectiveValue = valueOptional ? HoodieSchema.createNullable(prunedValue) : prunedValue;
    HoodieSchema result = HoodieSchema.createMap(effectiveValue);
    if (keyId >= 0) {
      result.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, keyId);
    }
    if (valueId >= 0) {
      result.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, valueId);
    }
    return result;
  }

  /**
   * Returns a HoodieSchema with the same fields and ids as {@code source} but with
   * its record name set to {@code recordName}. Walks HoodieSchema directly: the
   * top-level record is rebuilt with a new {@link org.apache.avro.Schema} record
   * name, the field list is rebuilt via {@link HoodieSchemaUtils#createNewSchemaField}
   * (which preserves Avro custom props — including {@code field-id}), and inner
   * schemas pass through by reference so their ids and structure are preserved.
   * The record-name argument is parsed as the legacy {@code namespace.Name}
   * convention if it carries a dot, with the substring after the last dot taken
   * as the simple name.
   */
  public static HoodieSchema withRecordName(HoodieSchema source, String recordName) {
    if (source == null || source.getType() != HoodieSchemaType.RECORD) {
      return source;
    }
    String simpleName;
    String namespace;
    int lastDot = recordName == null ? -1 : recordName.lastIndexOf('.');
    if (lastDot < 0) {
      simpleName = recordName;
      namespace = source.getNamespace().orElse(null);
    } else {
      namespace = recordName.substring(0, lastDot);
      simpleName = recordName.substring(lastDot + 1);
    }
    List<HoodieSchemaField> fields = source.getFields().stream()
        .map(HoodieSchemaUtils::createNewSchemaField)
        .collect(Collectors.toList());
    HoodieSchema renamed = HoodieSchema.createRecord(simpleName, namespace, source.getDoc().orElse(null), fields);
    // Default schemaId to 0 when source has none — downstream code (HoodieSchemaSerDe.toJson,
    // isEmptySchema-based sentinel checks) treats schemaId < 0 as "empty" and refuses to
    // serialize, which would break commit metadata and the schema-on-read cache.
    long sourceSchemaId = source.schemaId();
    renamed.setSchemaId(sourceSchemaId >= 0 ? sourceSchemaId : 0L);
    if (source.maxColumnId() >= 0) {
      renamed.setMaxColumnId(source.maxColumnId());
    }
    renamed.invalidateIdIndex();
    return renamed;
  }
}
