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

import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.TableChange;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.action.TableChangesHelper;

public class SchemaChangePersistHelper {
  private SchemaChangePersistHelper() {}

  /**
   * add columns to table.
   *
   * @param latestSchema latest internal schema.
   * @param colName col name to be added. if we want to add col to a nested filed, the fullName should be specify
   * @param colType col type to be added.
   * @param doc col doc to be added.
   * @param position col position to be added
   * @param positionType col position change type. now support three change types: first/after/before
   */
  public static InternalSchema applyAddChange(
      InternalSchema latestSchema,
      String colName,
      Type colType,
      String doc,
      String position,
      TableChange.ColumnPositionChange.ColumnPositionType positionType) {
    TableChanges.ColumnAddChange add = TableChanges.ColumnAddChange.get(latestSchema);
    String parentName = TableChangesHelper.getParentName(colName);
    add.addColumns(parentName, colName, colType, doc);
    if (position != null && positionType != null) {
      String referParentName = TableChangesHelper.getParentName(position);
      if (!parentName.equals(referParentName)) {
        throw new IllegalArgumentException("cannot reorder two columns which has different parent");
      }
      add.addPositionChange(colName, position, positionType);
    } else if (positionType != null && positionType.equals(TableChange.ColumnPositionChange.ColumnPositionType.FIRST)) {
      add.addPositionChange(colName, "", positionType);
    }
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, add);
  }

  /**
   * delete columns to table.
   * @param latestSchema latest internal schema.
   * @param colName col name to be deleted. if we want to delete col from a nested filed, the fullName should be specify
   */
  public static InternalSchema applyDeleteChange(InternalSchema latestSchema, String colName) {
    TableChanges.ColumnDeleteChange delete = TableChanges.ColumnDeleteChange.get(latestSchema);
    delete.deleteColumn(colName);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, delete);
  }

  /**
   *rename col name for hudi table.
   *
   * @param latestSchema latest internal schema.
   * @param colName col name to be renamed. if we want to rename col from a nested filed, the fullName should be specify
   * @param newName new name for current col. no need to specify fullName.
   */
  public static InternalSchema applyRenameChange(InternalSchema latestSchema, String colName, String newName) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.renameColumn(colName, newName);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * update col type for hudi table.
   *
   * @param latestSchema latest internal schema.
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param nullable .
   */
  public static InternalSchema applyColumnNullabilityChange(InternalSchema latestSchema, String colName, boolean nullable) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.updateColumnNullability(colName, nullable);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * update col comment for hudi table.
   *
   * @param latestSchema latest internal schema.
   * @param colName col name to be changed. if we want to change col from a nested filed, the fullName should be specify
   * @param doc .
   */
  public static InternalSchema applyColumnCommentChange(InternalSchema latestSchema, String colName, String doc) {
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(latestSchema);
    updateChange.updateColumnComment(colName, doc);
    return SchemaChangeUtils.applyTableChanges2Schema(latestSchema, updateChange);
  }

  /**
   * reorder the position of col.
   *
   * @param latestSchema latest internal schema.
   * @param colName column which need to be reordered. if we want to change col from a nested filed, the fullName should be specify.
   * @param referColName reference position.
   * @param positionType col position change type. now support three change types: first/after/before
   */
  public static InternalSchema applyReOrderColPositionChange(
      InternalSchema latestSchema,
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
