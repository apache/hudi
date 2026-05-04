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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * HoodieSchema-shaped applier for column-level schema evolution operations
 * (ADD / DELETE / RENAME / UPDATE / REORDER).
 *
 * <p>Each method returns a new {@link HoodieSchema}; the input is not mutated.
 * Field ids are preserved end-to-end so subsequent callers can resolve renamed
 * columns by id. All seven operations walk HoodieSchema directly (no
 * InternalSchema bridge).
 */
public class HoodieSchemaChangeApplier {

  private final HoodieSchema latestSchema;

  public HoodieSchemaChangeApplier(HoodieSchema latestSchema) {
    this.latestSchema = latestSchema;
  }

  /**
   * Add a column to the table. For nested fields, use a dot-separated full path
   * in {@code colName}. Position can be FIRST (no reference), or AFTER/BEFORE a
   * sibling of the same parent. The new column is always nullable (Hudi
   * convention for added columns).
   */
  public HoodieSchema applyAddChange(String colName,
                                     HoodieSchema colType,
                                     String doc,
                                     String position,
                                     ColumnPositionType positionType) {
    if (positionType == null) {
      throw new IllegalArgumentException("positionType should be specified");
    }
    String parentName = parentOf(colName);
    String leafName = leafOf(colName);
    if (positionType == ColumnPositionType.AFTER || positionType == ColumnPositionType.BEFORE) {
      if (position == null || position.isEmpty()) {
        throw new IllegalArgumentException(
            "position should not be null/empty_string when specify positionChangeType as after/before");
      }
      if (!parentName.equals(parentOf(position))) {
        throw new IllegalArgumentException("cannot reorder two columns which has different parent");
      }
    }

    int newId = nextColumnId(latestSchema);
    HoodieSchema effectiveType = colType.isNullable() ? colType.getNonNullType() : colType;
    HoodieSchemaField newField = HoodieSchemaField.of(
        leafName, HoodieSchema.createNullable(effectiveType), doc, null);
    newField.addProp(HoodieSchema.FIELD_ID_PROP, newId);
    String refLeaf = (position != null && !position.isEmpty()) ? leafOf(position) : null;

    HoodieSchema result = rewriteRecordAt(latestSchema, parentName,
        parent -> insertFieldInRecord(parent, newField, refLeaf, positionType));
    // The new field's id raised the max — bump the schema's tracker so callers
    // that consume {@link HoodieSchema#maxColumnId()} (notably the SerDe) see
    // the up-to-date ceiling.
    if (result.maxColumnId() < newId) {
      result.setMaxColumnId(newId);
    }
    result.invalidateIdIndex();
    return result;
  }

  /**
   * Delete one or more columns. For nested fields, use dot-separated full paths.
   * Each name is resolved to its column id and the corresponding field is
   * removed wherever it appears in the schema tree (top-level, in a nested
   * record, in a record under a map value, etc.). Throws when any name
   * doesn't resolve.
   */
  public HoodieSchema applyDeleteChange(String... colNames) {
    HoodieSchema result = latestSchema;
    for (String colName : colNames) {
      int targetId = result.findIdByName(colName);
      if (targetId < 0) {
        throw new IllegalArgumentException(
            String.format("cannot delete col: %s which does not exist in schema", colName));
      }
      result = deleteFieldById(result, targetId);
    }
    return result;
  }

  /**
   * Rename a column without changing its id, type, or position.
   * {@code newName} is a leaf name — the parent path is taken from {@code colName}.
   */
  public HoodieSchema applyRenameChange(String colName, String newName) {
    return rewriteFieldByName(latestSchema, colName,
        field -> rebuildField(field, newName, field.schema(), field.doc().orElse(null)));
  }

  /**
   * Toggle a column's nullability. Only required → optional is allowed by
   * default; the inverse must be forced explicitly via the underlying applier.
   */
  public HoodieSchema applyColumnNullabilityChange(String colName, boolean nullable) {
    return rewriteFieldByName(latestSchema, colName, field -> {
      HoodieSchema fieldSchema = field.schema();
      HoodieSchema effective = fieldSchema.isNullable() ? fieldSchema.getNonNullType() : fieldSchema;
      HoodieSchema newSchema = nullable ? HoodieSchema.createNullable(effective) : effective;
      return rebuildField(field, field.name(), newSchema, field.doc().orElse(null));
    });
  }

  /**
   * Promote a column to a wider type. Type-promotion legality is decided by
   * the caller — this applier does not enforce compatibility.
   */
  public HoodieSchema applyColumnTypeChange(String colName, HoodieSchema newType) {
    return rewriteFieldByName(latestSchema, colName, field -> {
      HoodieSchema fieldSchema = field.schema();
      boolean wasNullable = fieldSchema.isNullable();
      HoodieSchema effectiveNewType = newType.isNullable() ? newType.getNonNullType() : newType;
      HoodieSchema finalSchema = wasNullable ? HoodieSchema.createNullable(effectiveNewType) : effectiveNewType;
      return rebuildField(field, field.name(), finalSchema, field.doc().orElse(null));
    });
  }

  /**
   * Update a column's documentation string.
   */
  public HoodieSchema applyColumnCommentChange(String colName, String doc) {
    return rewriteFieldByName(latestSchema, colName,
        field -> rebuildField(field, field.name(), field.schema(), doc));
  }

  /**
   * Reorder a column relative to a sibling within the same enclosing struct.
   * FIRST is allowed without a reference; AFTER/BEFORE require a reference
   * column with the same parent.
   */
  public HoodieSchema applyReOrderColPositionChange(String colName,
                                                    String referColName,
                                                    ColumnPositionType positionType) {
    if (positionType == null) {
      throw new IllegalArgumentException("positionType should be specified");
    }
    String parentName = parentOf(colName);
    String colLeaf = leafOf(colName);
    if (positionType != ColumnPositionType.FIRST) {
      if (referColName == null || referColName.isEmpty()) {
        throw new IllegalArgumentException(
            "referColName should not be null/empty when positionType is " + positionType);
      }
      if (!parentName.equals(parentOf(referColName))) {
        throw new IllegalArgumentException("cannot reorder two columns which has different parent");
      }
    }
    String refLeaf = (referColName != null && !referColName.isEmpty()) ? leafOf(referColName) : null;
    return rewriteRecordAt(latestSchema, parentName,
        parent -> moveFieldInRecord(parent, colLeaf, refLeaf, positionType));
  }

  /**
   * Returns the schema this applier was constructed with.
   */
  public HoodieSchema getLatestSchema() {
    return latestSchema;
  }

  // -------------------------------------------------------------------------
  // HoodieSchema-direct walker for single-field rewrite operations
  // (rename / nullability / type / comment). Resolves a dotted full name —
  // with "element" descending into array elements and "value" descending into
  // map values — to a target HoodieSchemaField, applies the supplied
  // transform, and rebuilds the parent records up the tree. The bridge is
  // not on this path.
  // -------------------------------------------------------------------------

  @FunctionalInterface
  private interface FieldTransform {
    HoodieSchemaField apply(HoodieSchemaField field);
  }

  private static HoodieSchema rewriteFieldByName(HoodieSchema source, String fullName, FieldTransform transform) {
    if (fullName == null || fullName.isEmpty()) {
      throw new IllegalArgumentException("col name must not be null or empty");
    }
    String[] parts = fullName.split("\\.");
    return rewriteRootRecord(source, parts, transform);
  }

  /**
   * Rewrites the root-level record of {@code source}. Distinct from
   * {@link #rewriteRecord} because the root carries schemaId / maxColumnId
   * that nested records don't, and we need to forward them to the result.
   */
  private static HoodieSchema rewriteRootRecord(HoodieSchema source, String[] parts, FieldTransform transform) {
    boolean nullable = source.isNullable();
    HoodieSchema effective = nullable ? source.getNonNullType() : source;
    HoodieSchema rewritten = rewriteRecord(effective, parts, 0, transform);
    if (source.schemaId() >= 0) {
      rewritten.setSchemaId(source.schemaId());
    }
    if (source.maxColumnId() >= 0) {
      rewritten.setMaxColumnId(source.maxColumnId());
    }
    rewritten.invalidateIdIndex();
    return nullable ? HoodieSchema.createNullable(rewritten) : rewritten;
  }

  private static HoodieSchema rewriteRecord(HoodieSchema record, String[] parts, int idx, FieldTransform transform) {
    if (record.getType() != HoodieSchemaType.RECORD) {
      throw new IllegalArgumentException("expected record at depth " + idx + " of path "
          + Arrays.toString(parts) + ", got " + record.getType());
    }
    String segment = parts[idx];
    boolean isLeaf = idx == parts.length - 1;
    List<HoodieSchemaField> newFields = new ArrayList<>(record.getFields().size());
    boolean found = false;
    for (HoodieSchemaField field : record.getFields()) {
      if (!found && field.name().equals(segment)) {
        found = true;
        if (isLeaf) {
          newFields.add(transform.apply(field));
        } else {
          HoodieSchema rewrittenChild = descendThroughField(field.schema(), parts, idx + 1, transform);
          newFields.add(rebuildField(field, field.name(), rewrittenChild, field.doc().orElse(null)));
        }
      } else {
        // Avro Schema.Field instances cannot be re-parented to a second
        // record, so untouched fields still need a clone before they go
        // into the rebuilt parent.
        newFields.add(rebuildField(field, field.name(), field.schema(), field.doc().orElse(null)));
      }
    }
    if (!found) {
      throw new IllegalArgumentException("cannot find field '" + segment + "' at depth " + idx
          + " in path " + Arrays.toString(parts));
    }
    return HoodieSchema.createRecord(record.getName(), record.getNamespace().orElse(null),
        record.getDoc().orElse(null), newFields);
  }

  /**
   * Descends through a record/array/map field's schema and returns the
   * rewritten sub-schema, preserving array {@code element-id} / map
   * {@code key-id} / {@code value-id} props. Re-wraps in a [null, T] union
   * when the descent passed through a nullable field.
   */
  private static HoodieSchema descendThroughField(HoodieSchema fieldSchema, String[] parts, int idx, FieldTransform transform) {
    boolean nullable = fieldSchema.isNullable();
    HoodieSchema effective = nullable ? fieldSchema.getNonNullType() : fieldSchema;
    HoodieSchema rewritten;
    switch (effective.getType()) {
      case RECORD:
        rewritten = rewriteRecord(effective, parts, idx, transform);
        break;
      case ARRAY: {
        if (!"element".equals(parts[idx])) {
          throw new IllegalArgumentException("expected 'element' at depth " + idx + ", got " + parts[idx]);
        }
        int elementId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP));
        HoodieSchema rewrittenElement = descendThroughField(effective.getElementType(), parts, idx + 1, transform);
        HoodieSchema arr = HoodieSchema.createArray(rewrittenElement);
        if (elementId >= 0) {
          arr.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, elementId);
        }
        rewritten = arr;
        break;
      }
      case MAP: {
        if (!"value".equals(parts[idx])) {
          throw new IllegalArgumentException("expected 'value' at depth " + idx + ", got " + parts[idx]);
        }
        int keyId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP));
        int valueId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP));
        HoodieSchema rewrittenValue = descendThroughField(effective.getValueType(), parts, idx + 1, transform);
        HoodieSchema map = HoodieSchema.createMap(rewrittenValue);
        if (keyId >= 0) {
          map.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, keyId);
        }
        if (valueId >= 0) {
          map.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, valueId);
        }
        rewritten = map;
        break;
      }
      default:
        throw new IllegalArgumentException("cannot descend into primitive at depth " + idx);
    }
    return nullable ? HoodieSchema.createNullable(rewritten) : rewritten;
  }

  /**
   * Builds a new HoodieSchemaField with the supplied {@code name} / {@code schema}
   * / {@code doc} but inheriting {@code field-id} and any other custom Avro
   * properties from {@code source}. Default value and order are preserved.
   */
  private static HoodieSchemaField rebuildField(HoodieSchemaField source, String name, HoodieSchema schema, String doc) {
    HoodieSchemaField rebuilt = HoodieSchemaField.of(name, schema, doc, source.defaultVal().orElse(null));
    for (Map.Entry<String, Object> e : source.getObjectProps().entrySet()) {
      rebuilt.addProp(e.getKey(), e.getValue());
    }
    return rebuilt;
  }

  private static int readIntProp(Object raw) {
    return raw instanceof Number ? ((Number) raw).intValue() : -1;
  }

  // -------------------------------------------------------------------------
  // Id-based delete walker. Removes any record field whose field-id matches
  // {@code targetId} from the schema tree, recursing through arrays /
  // map values to find nested occurrences. Element-id / key-id / value-id
  // props are preserved on rebuilt array / map sub-trees.
  // -------------------------------------------------------------------------

  private static HoodieSchema deleteFieldById(HoodieSchema source, int targetId) {
    boolean nullable = source.isNullable();
    HoodieSchema effective = nullable ? source.getNonNullType() : source;
    HoodieSchema rewritten = deleteFieldByIdInner(effective, targetId);
    if (source.schemaId() >= 0) {
      rewritten.setSchemaId(source.schemaId());
    }
    if (source.maxColumnId() >= 0) {
      rewritten.setMaxColumnId(source.maxColumnId());
    }
    rewritten.invalidateIdIndex();
    return nullable ? HoodieSchema.createNullable(rewritten) : rewritten;
  }

  private static HoodieSchema deleteFieldByIdInner(HoodieSchema schema, int targetId) {
    HoodieSchema effective = schema.isNullable() ? schema.getNonNullType() : schema;
    switch (effective.getType()) {
      case RECORD: {
        List<HoodieSchemaField> newFields = new ArrayList<>(effective.getFields().size());
        for (HoodieSchemaField field : effective.getFields()) {
          if (field.fieldId() == targetId) {
            // skip — this is the field being deleted
            continue;
          }
          // Field stays — but its sub-tree may contain the target id, so recurse.
          HoodieSchema newSubSchema = deleteFieldByIdInner(field.schema(), targetId);
          newFields.add(rebuildField(field, field.name(), newSubSchema, field.doc().orElse(null)));
        }
        HoodieSchema newRecord = HoodieSchema.createRecord(
            effective.getName(), effective.getNamespace().orElse(null),
            effective.getDoc().orElse(null), newFields);
        return reWrapNullability(schema, newRecord);
      }
      case ARRAY: {
        HoodieSchema elementType = effective.getElementType();
        HoodieSchema newElement = deleteFieldByIdInner(elementType, targetId);
        if (newElement == elementType) {
          return schema;
        }
        int elementId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP));
        HoodieSchema arr = HoodieSchema.createArray(newElement);
        if (elementId >= 0) {
          arr.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, elementId);
        }
        return reWrapNullability(schema, arr);
      }
      case MAP: {
        HoodieSchema valueType = effective.getValueType();
        HoodieSchema newValue = deleteFieldByIdInner(valueType, targetId);
        if (newValue == valueType) {
          return schema;
        }
        int keyId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP));
        int valueId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP));
        HoodieSchema map = HoodieSchema.createMap(newValue);
        if (keyId >= 0) {
          map.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, keyId);
        }
        if (valueId >= 0) {
          map.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, valueId);
        }
        return reWrapNullability(schema, map);
      }
      default:
        return schema;
    }
  }

  /**
   * Re-wraps {@code rebuilt} in a [null, T] union if {@code source} was
   * nullable. Used after a sub-tree rebuild so the parent's nullability
   * encoding survives the round-trip.
   */
  private static HoodieSchema reWrapNullability(HoodieSchema source, HoodieSchema rebuilt) {
    return source.isNullable() ? HoodieSchema.createNullable(rebuilt) : rebuilt;
  }

  // -------------------------------------------------------------------------
  // Parent-record walker for position-aware ops (add / reorder). The walker
  // descends a dotted parent path (with "element" / "value" segments for
  // arrays / maps) until it lands on a record schema, applies the supplied
  // transform, and rebuilds the surrounding tree. element-id / key-id /
  // value-id props are preserved on rebuilt array / map sub-trees.
  // -------------------------------------------------------------------------

  @FunctionalInterface
  private interface RecordTransform {
    HoodieSchema apply(HoodieSchema record);
  }

  private static HoodieSchema rewriteRecordAt(HoodieSchema source, String parentPath, RecordTransform transform) {
    boolean topLevel = parentPath == null || parentPath.isEmpty();
    boolean nullable = source.isNullable();
    HoodieSchema effective = nullable ? source.getNonNullType() : source;
    HoodieSchema rewritten;
    if (topLevel) {
      if (effective.getType() != HoodieSchemaType.RECORD) {
        throw new IllegalArgumentException("expected root record, got " + effective.getType());
      }
      rewritten = transform.apply(effective);
    } else {
      String[] parts = parentPath.split("\\.");
      rewritten = walkAndRewriteRecord(effective, parts, 0, transform);
    }
    if (source.schemaId() >= 0) {
      rewritten.setSchemaId(source.schemaId());
    }
    if (source.maxColumnId() >= 0) {
      rewritten.setMaxColumnId(source.maxColumnId());
    }
    rewritten.invalidateIdIndex();
    return nullable ? HoodieSchema.createNullable(rewritten) : rewritten;
  }

  /**
   * Walks {@code parts[idx..]} from a non-null {@code effective} schema and
   * applies {@code transform} to the record at the end of the path. Returns
   * the rewritten schema in non-null form; callers re-wrap nullability.
   */
  private static HoodieSchema walkAndRewriteRecord(HoodieSchema effective,
                                                   String[] parts,
                                                   int idx,
                                                   RecordTransform transform) {
    String segment = parts[idx];
    boolean isLast = idx == parts.length - 1;
    HoodieSchemaType t = effective.getType();
    if (t == HoodieSchemaType.RECORD) {
      List<HoodieSchemaField> newFields = new ArrayList<>(effective.getFields().size());
      boolean found = false;
      for (HoodieSchemaField field : effective.getFields()) {
        if (!found && field.name().equals(segment)) {
          found = true;
          HoodieSchema fieldSchema = field.schema();
          boolean fieldNullable = fieldSchema.isNullable();
          HoodieSchema fieldEffective = fieldNullable ? fieldSchema.getNonNullType() : fieldSchema;
          HoodieSchema rewrittenInner;
          if (isLast) {
            if (fieldEffective.getType() != HoodieSchemaType.RECORD) {
              throw new IllegalArgumentException(
                  "expected record at end of parent path " + Arrays.toString(parts)
                      + ", got " + fieldEffective.getType());
            }
            rewrittenInner = transform.apply(fieldEffective);
          } else {
            rewrittenInner = walkAndRewriteRecord(fieldEffective, parts, idx + 1, transform);
          }
          HoodieSchema finalInner = fieldNullable ? HoodieSchema.createNullable(rewrittenInner) : rewrittenInner;
          newFields.add(rebuildField(field, field.name(), finalInner, field.doc().orElse(null)));
        } else {
          // Avro Schema.Field instances cannot be re-parented to a second
          // record, so untouched fields still need a clone before they go
          // into the rebuilt parent.
          newFields.add(rebuildField(field, field.name(), field.schema(), field.doc().orElse(null)));
        }
      }
      if (!found) {
        throw new IllegalArgumentException(
            "cannot find field '" + segment + "' at depth " + idx + " in path " + Arrays.toString(parts));
      }
      return HoodieSchema.createRecord(effective.getName(), effective.getNamespace().orElse(null),
          effective.getDoc().orElse(null), newFields);
    }
    if (t == HoodieSchemaType.ARRAY) {
      if (!"element".equals(segment)) {
        throw new IllegalArgumentException("expected 'element' at depth " + idx + ", got " + segment);
      }
      HoodieSchema elementType = effective.getElementType();
      boolean elemNullable = elementType.isNullable();
      HoodieSchema elemEffective = elemNullable ? elementType.getNonNullType() : elementType;
      HoodieSchema rewrittenElem;
      if (isLast) {
        if (elemEffective.getType() != HoodieSchemaType.RECORD) {
          throw new IllegalArgumentException(
              "expected record at end of parent path " + Arrays.toString(parts)
                  + ", got " + elemEffective.getType());
        }
        rewrittenElem = transform.apply(elemEffective);
      } else {
        rewrittenElem = walkAndRewriteRecord(elemEffective, parts, idx + 1, transform);
      }
      HoodieSchema finalElem = elemNullable ? HoodieSchema.createNullable(rewrittenElem) : rewrittenElem;
      int eId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.ELEMENT_ID_PROP));
      HoodieSchema arr = HoodieSchema.createArray(finalElem);
      if (eId >= 0) {
        arr.getAvroSchema().addProp(HoodieSchema.ELEMENT_ID_PROP, eId);
      }
      return arr;
    }
    if (t == HoodieSchemaType.MAP) {
      if (!"value".equals(segment)) {
        throw new IllegalArgumentException("expected 'value' at depth " + idx + ", got " + segment);
      }
      HoodieSchema valueType = effective.getValueType();
      boolean valNullable = valueType.isNullable();
      HoodieSchema valEffective = valNullable ? valueType.getNonNullType() : valueType;
      HoodieSchema rewrittenVal;
      if (isLast) {
        if (valEffective.getType() != HoodieSchemaType.RECORD) {
          throw new IllegalArgumentException(
              "expected record at end of parent path " + Arrays.toString(parts)
                  + ", got " + valEffective.getType());
        }
        rewrittenVal = transform.apply(valEffective);
      } else {
        rewrittenVal = walkAndRewriteRecord(valEffective, parts, idx + 1, transform);
      }
      HoodieSchema finalVal = valNullable ? HoodieSchema.createNullable(rewrittenVal) : rewrittenVal;
      int kId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.KEY_ID_PROP));
      int vId = readIntProp(effective.getAvroSchema().getObjectProp(HoodieSchema.VALUE_ID_PROP));
      HoodieSchema map = HoodieSchema.createMap(finalVal);
      if (kId >= 0) {
        map.getAvroSchema().addProp(HoodieSchema.KEY_ID_PROP, kId);
      }
      if (vId >= 0) {
        map.getAvroSchema().addProp(HoodieSchema.VALUE_ID_PROP, vId);
      }
      return map;
    }
    throw new IllegalArgumentException("cannot descend through " + t + " at depth " + idx);
  }

  /**
   * Inserts {@code newField} into {@code record}'s field list at the position
   * determined by {@code positionType} / {@code refLeaf}. NO_OPERATION
   * appends; FIRST inserts at index 0; AFTER/BEFORE place relative to the
   * referenced sibling within the same record.
   */
  private static HoodieSchema insertFieldInRecord(HoodieSchema record,
                                                  HoodieSchemaField newField,
                                                  String refLeaf,
                                                  ColumnPositionType positionType) {
    List<HoodieSchemaField> fields = cloneFieldList(record.getFields());
    int insertIdx;
    switch (positionType) {
      case FIRST:
        insertIdx = 0;
        break;
      case BEFORE: {
        int refIdx = findFieldIndex(fields, refLeaf);
        if (refIdx < 0) {
          throw new IllegalArgumentException("reference column not found: " + refLeaf);
        }
        insertIdx = refIdx;
        break;
      }
      case AFTER: {
        int refIdx = findFieldIndex(fields, refLeaf);
        if (refIdx < 0) {
          throw new IllegalArgumentException("reference column not found: " + refLeaf);
        }
        insertIdx = refIdx + 1;
        break;
      }
      case NO_OPERATION:
      default:
        insertIdx = fields.size();
        break;
    }
    fields.add(insertIdx, newField);
    return HoodieSchema.createRecord(record.getName(), record.getNamespace().orElse(null),
        record.getDoc().orElse(null), fields);
  }

  /**
   * Moves {@code colLeaf} within {@code record} to the position implied by
   * {@code positionType} / {@code refLeaf}. The reference index is computed
   * AFTER the source field is removed, so AFTER refLeaf still places the
   * moved field immediately after refLeaf even when refLeaf was originally
   * to the right of colLeaf.
   */
  private static HoodieSchema moveFieldInRecord(HoodieSchema record,
                                                String colLeaf,
                                                String refLeaf,
                                                ColumnPositionType positionType) {
    List<HoodieSchemaField> fields = cloneFieldList(record.getFields());
    int colIdx = findFieldIndex(fields, colLeaf);
    if (colIdx < 0) {
      throw new IllegalArgumentException("column not found: " + colLeaf);
    }
    HoodieSchemaField col = fields.remove(colIdx);
    int insertIdx;
    switch (positionType) {
      case FIRST:
        insertIdx = 0;
        break;
      case BEFORE: {
        int refIdx = findFieldIndex(fields, refLeaf);
        if (refIdx < 0) {
          throw new IllegalArgumentException("reference column not found: " + refLeaf);
        }
        insertIdx = refIdx;
        break;
      }
      case AFTER: {
        int refIdx = findFieldIndex(fields, refLeaf);
        if (refIdx < 0) {
          throw new IllegalArgumentException("reference column not found: " + refLeaf);
        }
        insertIdx = refIdx + 1;
        break;
      }
      default:
        throw new IllegalArgumentException("unsupported reorder positionType: " + positionType);
    }
    fields.add(insertIdx, col);
    return HoodieSchema.createRecord(record.getName(), record.getNamespace().orElse(null),
        record.getDoc().orElse(null), fields);
  }

  /**
   * Returns a fresh list of HoodieSchemaField clones for {@code source}.
   * Used by {@link #insertFieldInRecord} / {@link #moveFieldInRecord} —
   * Avro Schema.Field instances cannot be re-parented to a second record, so
   * passing the originals into {@link HoodieSchema#createRecord} would trip
   * {@code AvroRuntimeException("Field already used")}.
   */
  private static List<HoodieSchemaField> cloneFieldList(List<HoodieSchemaField> source) {
    List<HoodieSchemaField> cloned = new ArrayList<>(source.size());
    for (HoodieSchemaField f : source) {
      cloned.add(rebuildField(f, f.name(), f.schema(), f.doc().orElse(null)));
    }
    return cloned;
  }

  private static int findFieldIndex(List<HoodieSchemaField> fields, String name) {
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).name().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  private static String parentOf(String colName) {
    int dot = colName.lastIndexOf('.');
    return dot < 0 ? "" : colName.substring(0, dot);
  }

  private static String leafOf(String colName) {
    int dot = colName.lastIndexOf('.');
    return dot < 0 ? colName : colName.substring(dot + 1);
  }

  /**
   * Returns the next free column id for {@code source}: max of (the schema's
   * tracked maxColumnId) and (the largest id observed among existing record
   * fields / array elements / map keys / map values), plus one. The double
   * check defends against schemas whose tracker has drifted from the actual
   * id population.
   */
  private static int nextColumnId(HoodieSchema source) {
    int max = source.maxColumnId();
    for (int id : source.getAllIds()) {
      if (id > max) {
        max = id;
      }
    }
    return max + 1;
  }
}
