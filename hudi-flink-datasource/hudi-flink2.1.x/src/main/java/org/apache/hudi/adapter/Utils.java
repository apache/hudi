/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.FIRST;
import static org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.NO_OPERATION;

/**
 * Adapter utils.
 */
public class Utils {
  public static FactoryUtil.DefaultDynamicTableContext getTableContext(
      ObjectIdentifier tablePath,
      ResolvedCatalogTable catalogTable,
      ReadableConfig conf) {
    return new FactoryUtil.DefaultDynamicTableContext(tablePath, catalogTable,
        Collections.emptyMap(), conf, Thread.currentThread().getContextClassLoader(), false);
  }

  public static BinaryExternalSorter getBinaryExternalSorter(
      final Object owner,
      MemoryManager memoryManager,
      long reservedMemorySize,
      IOManager ioManager,
      AbstractRowDataSerializer<RowData> inputSerializer,
      BinaryRowDataSerializer serializer,
      NormalizedKeyComputer normalizedKeyComputer,
      RecordComparator comparator,
      Configuration conf) {
    return new BinaryExternalSorter(owner, memoryManager, reservedMemorySize,
        ioManager, inputSerializer, serializer, normalizedKeyComputer, comparator,
        conf.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
        conf.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
        (int) conf.get(
             ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE).getBytes(),
        conf.get(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
  }

  public static InternalSchema applyTableChange(InternalSchema oldSchema, List changes, Function<LogicalType, Type> convertFunc) {
    InternalSchema newSchema = oldSchema;
    for (Object change : changes) {
      TableChange tableChange = (TableChange) change;
      newSchema = applyTableChange(newSchema, tableChange, convertFunc);
    }
    return newSchema;
  }

  private static InternalSchema applyTableChange(InternalSchema oldSchema, TableChange change, Function<LogicalType, Type> convertFunc) {
    InternalSchemaChangeApplier changeApplier = new InternalSchemaChangeApplier(oldSchema);
    if (change instanceof TableChange.AddColumn) {
      if (((TableChange.AddColumn) change).getColumn().isPhysical()) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        Column column = add.getColumn();
        String colName = column.getName();
        Type colType = convertFunc.apply(column.getDataType().getLogicalType());
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
      Type newColType = convertFunc.apply(modify.getNewType().getLogicalType());
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

  private static Pair<org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType, String> parseColumnPosition(TableChange.ColumnPosition colPosition) {
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
