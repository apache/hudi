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

package org.apache.hudi.adapter;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.action.InternalSchemaChangeApplier;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.FIRST;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.NO_OPERATION;

/**
 * Adapter clazz for {@code HoodieHiveCatalog}.
 */
public interface HoodieHiveCatalogAdapter extends Catalog {

  default void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, List<TableChange> tableChanges,
      boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    checkNotNull(tablePath, "Table path cannot be null");
    checkNotNull(newCatalogTable, "New catalog table cannot be null");

    if (!isUpdateAllow(tablePath, newCatalogTable, ignoreIfNotExists)) {
      return;
    }
    InternalSchema oldSchema = getInternalSchema(tablePath);
    InternalSchema newSchema = oldSchema;
    for (TableChange tableChange : tableChanges) {
      newSchema = applyTableChange(newSchema, tableChange);
    }
    if (!oldSchema.equals(newSchema)) {
      alterHoodieTableSchema(tablePath, newSchema);
    }
    refreshHMSTable(tablePath, newCatalogTable);
  }

  boolean isUpdateAllow(ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists) throws TableNotExistException;

  InternalSchema getInternalSchema(ObjectPath tablePath) throws TableNotExistException, CatalogException;

  void refreshHMSTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable);

  void alterHoodieTableSchema(ObjectPath tablePath, InternalSchema newSchema) throws TableNotExistException, CatalogException;

  Type convertToInternalType(LogicalType logicalType);

  default InternalSchema applyTableChange(InternalSchema oldSchema, TableChange change) {
    InternalSchemaChangeApplier changeApplier = new InternalSchemaChangeApplier(oldSchema);
    if (change instanceof TableChange.AddColumn) {
      if (((TableChange.AddColumn) change).getColumn().isPhysical()) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        Column column = add.getColumn();
        String colName = column.getName();
        Type colType = convertToInternalType(column.getDataType().getLogicalType());
        String comment = column.getComment().orElse(null);
        Pair<org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType, String> colPositionPair =
            parseColumnPosition(add.getPosition());
        return changeApplier.applyAddChange(
            colName, colType, comment, colPositionPair.getRight(), colPositionPair.getLeft());
      } else {
        throw new HoodieNotSupportedException("Add non-physical column is not supported yet.");
      }
    } else if (change instanceof TableChange.DropColumn) {
      TableChange.DropColumn drop = (TableChange.DropColumn) change;
      return changeApplier.applyDeleteChange(drop.getColumnName());
    } else if (change instanceof TableChange.ModifyColumnName) {
      TableChange.ModifyColumnName modify = (TableChange.ModifyColumnName) change;
      String oldColName = modify.getOldColumnName();
      String newColName = modify.getNewColumnName();
      return changeApplier.applyRenameChange(oldColName, newColName);
    } else if (change instanceof TableChange.ModifyPhysicalColumnType) {
      TableChange.ModifyPhysicalColumnType modify = (TableChange.ModifyPhysicalColumnType) change;
      String colName = modify.getOldColumn().getName();
      Type newColType = convertToInternalType(modify.getNewType().getLogicalType());
      return changeApplier.applyColumnTypeChange(colName, newColType);
    } else if (change instanceof TableChange.ModifyColumnPosition) {
      TableChange.ModifyColumnPosition modify = (TableChange.ModifyColumnPosition) change;
      String colName = modify.getOldColumn().getName();
      Pair<org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType, String> colPositionPair =
          parseColumnPosition(modify.getNewPosition());
      return changeApplier.applyReOrderColPositionChange(
          colName, colPositionPair.getRight(), colPositionPair.getLeft());
    } else if (change instanceof TableChange.ModifyColumnComment) {
      TableChange.ModifyColumnComment modify  = (TableChange.ModifyColumnComment) change;
      String colName = modify.getOldColumn().getName();
      String comment = modify.getNewComment();
      return changeApplier.applyColumnCommentChange(colName, comment);
    } else if (change instanceof TableChange.ResetOption || change instanceof TableChange.SetOption) {
      return oldSchema;
    } else {
      throw new HoodieNotSupportedException(change.getClass().getSimpleName() + " is not supported.");
    }
  }

  default Pair<org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType, String> parseColumnPosition(TableChange.ColumnPosition colPosition) {
    org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType positionType;
    String position = "";
    if (colPosition instanceof TableChange.First) {
      positionType = FIRST;
    } else if (colPosition instanceof TableChange.After) {
      positionType = AFTER;
      position = ((TableChange.After) colPosition).column();
    } else {
      positionType = NO_OPERATION;
    }
    return Pair.of(positionType, position);
  }
}
