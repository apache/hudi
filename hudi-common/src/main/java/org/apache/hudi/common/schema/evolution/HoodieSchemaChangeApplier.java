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
import org.apache.hudi.common.schema.evolution.legacy.InternalSchema;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChange;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChanges;
import org.apache.hudi.common.schema.evolution.legacy.action.TableChangesHelper;
import org.apache.hudi.common.schema.evolution.legacy.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.evolution.legacy.utils.SchemaChangeUtils;
import org.apache.hudi.common.schema.types.Type;

import java.util.Arrays;

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
   */
  public HoodieSchema applyDeleteChange(String... colNames) {
    TableChanges.ColumnDeleteChange delete = TableChanges.ColumnDeleteChange.get(latestInternal);
    Arrays.stream(colNames).forEach(delete::deleteColumn);
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, delete));
  }

  /**
   * Rename a column without changing its id, type, or position.
   * {@code newName} is a leaf name — the parent path is taken from {@code colName}.
   */
  public HoodieSchema applyRenameChange(String colName, String newName) {
    TableChanges.ColumnUpdateChange update = TableChanges.ColumnUpdateChange.get(latestInternal);
    update.renameColumn(colName, newName);
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, update));
  }

  /**
   * Toggle a column's nullability. Only required → optional is allowed by
   * default; the inverse must be forced explicitly via the underlying applier.
   */
  public HoodieSchema applyColumnNullabilityChange(String colName, boolean nullable) {
    TableChanges.ColumnUpdateChange update = TableChanges.ColumnUpdateChange.get(latestInternal);
    update.updateColumnNullability(colName, nullable);
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, update));
  }

  /**
   * Promote a column to a wider type. The legal promotions are defined by
   * {@link SchemaChangeUtils#isTypeUpdateAllow}.
   */
  public HoodieSchema applyColumnTypeChange(String colName, HoodieSchema newType) {
    Type internalType = InternalSchemaConverter.convertToField(newType);
    TableChanges.ColumnUpdateChange update = TableChanges.ColumnUpdateChange.get(latestInternal);
    update.updateColumnType(colName, internalType);
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, update));
  }

  /**
   * Update a column's documentation string.
   */
  public HoodieSchema applyColumnCommentChange(String colName, String doc) {
    TableChanges.ColumnUpdateChange update = TableChanges.ColumnUpdateChange.get(latestInternal);
    update.updateColumnComment(colName, doc);
    return wrap(SchemaChangeUtils.applyTableChanges2Schema(latestInternal, update));
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
}
