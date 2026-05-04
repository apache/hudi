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
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChange;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChanges;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChangesHelper;
import org.apache.hudi.common.schema.evolution.legacy.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.evolution.legacy.utils.SchemaChangeUtils;
import org.apache.hudi.common.schema.types.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * HoodieSchema-shaped applier for column-level schema evolution operations
 * (ADD / DELETE / RENAME / UPDATE / REORDER).
 *
 * <p>Each method returns a new {@link HoodieSchema}; the input is not mutated.
 * Field ids are preserved end-to-end via
 * {@link HoodieSchemaInternalSchemaBridge}, so subsequent callers can still
 * resolve renamed columns by id.
 *
 * <p>Implementation orchestrates the legacy {@code TableChanges} builders +
 * {@code SchemaChangeUtils.applyTableChanges2Schema} (still on InternalSchema
 * because they're substantial classes that haven't been ported to HoodieSchema
 * yet) and converts at the bridge boundary. The inlined algorithm is identical
 * to what the prior {@code InternalSchemaChangeApplier} did — same TableChanges
 * builders, same parent/leaf splitting, same FIRST/AFTER/BEFORE position rules.
 */
public class HoodieSchemaChangeApplier {

  private final HoodieSchema latestSchema;
  private final InternalSchema latestInternal;
  private final String recordName;

  public HoodieSchemaChangeApplier(HoodieSchema latestSchema) {
    this.latestSchema = latestSchema;
    // Use the id-preserving bridge so existing field ids carried as Avro
    // custom properties survive the round trip into the legacy TableChanges builders.
    this.latestInternal = HoodieSchemaInternalSchemaBridge.toInternalSchema(latestSchema);
    this.recordName = latestSchema.getFullName();
  }

  /**
   * Add a column to the table. For nested fields, use a dot-separated full path
   * in {@code colName}. Position can be FIRST (no reference), or AFTER/BEFORE a
   * sibling of the same parent.
   */
  public HoodieSchema applyAddChange(String colName,
                                     HoodieSchema colType,
                                     String doc,
                                     String position,
                                     ColumnPositionType positionType) {
    Type internalType = InternalSchemaConverter.convertToField(colType);
    TableChanges.ColumnAddChange add = TableChanges.ColumnAddChange.get(latestInternal);
    String parentName = TableChangesHelper.getParentName(colName);
    String leafName = TableChangesHelper.getLeafName(colName);
    add.addColumns(parentName, leafName, internalType, doc);
    if (positionType == null) {
      throw new IllegalArgumentException("positionType should be specified");
    }
    TableChange.ColumnPositionChange.ColumnPositionType legacyPos = positionType.toLegacy();
    switch (legacyPos) {
      case NO_OPERATION:
        break;
      case FIRST:
        add.addPositionChange(colName, "", legacyPos);
        break;
      case AFTER:
      case BEFORE:
        if (position == null || position.isEmpty()) {
          throw new IllegalArgumentException("position should not be null/empty_string when specify positionChangeType as after/before");
        }
        String referParentName = TableChangesHelper.getParentName(position);
        if (!parentName.equals(referParentName)) {
          throw new IllegalArgumentException("cannot reorder two columns which has different parent");
        }
        add.addPositionChange(colName, position, legacyPos);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("only support first/before/after but found: %s", legacyPos));
    }
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, add));
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
   * Promote a column to a wider type. Legal promotions are defined by
   * {@link SchemaChangeUtils#isTypeUpdateAllow}; this method does not enforce
   * them — callers are expected to validate compatibility.
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
    TableChanges.ColumnUpdateChange update = TableChanges.ColumnUpdateChange.get(latestInternal);
    TableChange.ColumnPositionChange.ColumnPositionType legacyPos = positionType.toLegacy();
    String parentName = TableChangesHelper.getParentName(colName);
    String referParentName = TableChangesHelper.getParentName(referColName);
    if (legacyPos.equals(TableChange.ColumnPositionChange.ColumnPositionType.FIRST)) {
      update.addPositionChange(colName, "", legacyPos);
    } else if (parentName.equals(referParentName)) {
      update.addPositionChange(colName, referColName, legacyPos);
    } else {
      throw new IllegalArgumentException("cannot reorder two columns which has different parent");
    }
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, update));
  }

  /**
   * Returns the schema this applier was constructed with.
   */
  public HoodieSchema getLatestSchema() {
    return latestSchema;
  }

  private HoodieSchema wrap(InternalSchema result) {
    return HoodieSchemaInternalSchemaBridge.toHoodieSchema(result, recordName);
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
        newFields.add(field);
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
}
