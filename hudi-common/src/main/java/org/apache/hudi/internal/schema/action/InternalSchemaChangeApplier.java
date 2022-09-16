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

package org.apache.hudi.internal.schema.action;

import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;

import java.util.Arrays;

/**
 * Manage schema change for HoodieWriteClient.
 */
public class InternalSchemaChangeApplier {
  private InternalSchema latestSchema;

  public InternalSchemaChangeApplier(InternalSchema latestSchema) {
    this.latestSchema = latestSchema;
  }

  /**
   * Add columns to table.
   *
   * @param colName col name to be added. if we want to add col to a nested filed, the fullName should be specify
   * @param colType col type to be added.
   * @param doc col doc to be added.
   * @param position col position to be added
   * @param positionType col position change type. now support three change types: first/after/before
   */
  public InternalSchema applyAddChange(
      String colName,
      Type colType,
      String doc,
      String position,
      TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    TableChanges.ColumnAddChange add = TableChanges.ColumnAddChange.get(latestSchema);
    String parentName = TableChangesHelper.getParentName(colName);
    String leafName = TableChangesHelper.getLeafName(colName);
    add.addColumns(parentName, leafName, colType, doc);
    if (positionType != null) {
      switch (positionType) {
        case NO_OPERATION:
          break;
        case FIRST:
          add.addPositionChange(colName, "", positionType);
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
          add.addPositionChange(colName, position, positionType);
          break;
        default:
          throw new IllegalArgumentException(String.format("only support first/before/after but found: %s", positionType));
      }
    } else {
      throw new IllegalArgumentException(String.format("positionType should be specified"));
    }
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, add);
  }

  /**
   * Delete columns to table.
   *
   * @param colNames col name to be deleted. if we want to delete col from a nested filed, the fullName should be specify
   */
  public InternalSchema applyDeleteChange(String... colNames) {
    TableChanges.ColumnDeleteChange delete = TableChanges.ColumnDeleteChange.get(latestSchema);
    Arrays.stream(colNames).forEach(colName -> delete.deleteColumn(colName));
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, delete);
  }

  /**
   * Rename col name for hudi table.
   *
   * @param colName col name to be renamed. if we want to rename col from a nested filed, the fullName should be specify
   * @param newName new name for current col. no need to specify fullName.
   */
  public InternalSchema applyRenameChange(String colName, String newName) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.renameColumn(colName, newName);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * Update col nullability for hudi table.
   *
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param nullable .
   */
  public InternalSchema applyColumnNullabilityChange(String colName, boolean nullable) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.updateColumnNullability(colName, nullable);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * Update col type for hudi table.
   *
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param newType .
   */
  public InternalSchema applyColumnTypeChange(String colName, Type newType) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.updateColumnType(colName, newType);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * Update col comment for hudi table.
   *
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param doc .
   */
  public InternalSchema applyColumnCommentChange(String colName, String doc) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.updateColumnComment(colName, doc);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * Reorder the position of col.
   *
   * @param colName column which need to be reordered. if we want to change col from a nested filed, the fullName should be specify.
   * @param referColName reference position.
   * @param positionType col position change type. now support three change types: first/after/before
   */
  public InternalSchema applyReOrderColPositionChange(
      String colName,
      String referColName,
      TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    String parentName = TableChangesHelper.getParentName(colName);
    String referParentName = TableChangesHelper.getParentName(referColName);
    if (positionType.equals(TableChange.ColumnPositionChange.ColumnPositionType.FIRST)) {
      updateChange.addPositionChange(colName, "", positionType);
    } else if (parentName.equals(referParentName)) {
      updateChange.addPositionChange(colName, referColName, positionType);
    } else {
      throw new IllegalArgumentException("cannot reorder two columns which has different parent");
    }
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }
}
