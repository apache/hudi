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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utilities for {@link org.apache.flink.table.types.DataType}.
 */
public class DataTypeUtils {
  /**
   * Returns whether the given type is TIMESTAMP type.
   */
  public static boolean isTimestampType(DataType type) {
    return type.getLogicalType().getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
  }

  /**
   * Returns the precision of the given TIMESTAMP type.
   */
  public static int precision(LogicalType logicalType) {
    if (logicalType instanceof TimestampType) {
      return ((TimestampType) logicalType).getPrecision();
    } else if (logicalType instanceof LocalZonedTimestampType) {
      return ((LocalZonedTimestampType) logicalType).getPrecision();
    } else {
      throw new AssertionError("Unexpected type: " + logicalType);
    }
  }

  /**
   * Convert the given {@link Schema} into {@link RowType}.
   */
  public static RowType toRowType(Schema schema) {
    List<RowType.RowField> rowFields =
        schema.getColumns().stream()
            .filter(col -> col instanceof UnresolvedPhysicalColumn)
            .map(col -> new RowType.RowField(col.getName(), ((DataType) ((UnresolvedPhysicalColumn) col).getDataType()).getLogicalType()))
            .collect(Collectors.toList());
    return new RowType(false, rowFields);
  }

  /**
   * Returns whether the given type is DATE type.
   */
  public static boolean isDateType(DataType type) {
    return type.getLogicalType().getTypeRoot() == LogicalTypeRoot.DATE;
  }

  /**
   * Returns whether the given type is DATETIME type.
   */
  public static boolean isDatetimeType(DataType type) {
    return isTimestampType(type) || isDateType(type);
  }

  /**
   * Projects the row fields with given names.
   */
  public static RowType.RowField[] projectRowFields(RowType rowType, String[] names) {
    int[] fieldIndices = Arrays.stream(names).mapToInt(rowType::getFieldIndex).toArray();
    return Arrays.stream(fieldIndices).mapToObj(i -> rowType.getFields().get(i)).toArray(RowType.RowField[]::new);
  }

  /**
   * Returns whether the given logical type belongs to the family.
   */
  public static boolean isFamily(LogicalType logicalType, LogicalTypeFamily family) {
    return logicalType.getTypeRoot().getFamilies().contains(family);
  }

  /**
   * Resolves the partition path string into value obj with given data type.
   */
  public static Object resolvePartition(String partition, DataType type) {
    if (partition == null) {
      return null;
    }

    LogicalTypeRoot typeRoot = type.getLogicalType().getTypeRoot();
    switch (typeRoot) {
      case CHAR:
      case VARCHAR:
        return partition;
      case BOOLEAN:
        return Boolean.parseBoolean(partition);
      case TINYINT:
        return Integer.valueOf(partition).byteValue();
      case SMALLINT:
        return Short.valueOf(partition);
      case INTEGER:
        return Integer.valueOf(partition);
      case BIGINT:
        return Long.valueOf(partition);
      case FLOAT:
        return Float.valueOf(partition);
      case DOUBLE:
        return Double.valueOf(partition);
      case DATE:
        return LocalDate.parse(partition);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return LocalDateTime.parse(partition);
      case DECIMAL:
        return new BigDecimal(partition);
      default:
        throw new RuntimeException(
            String.format(
                "Can not convert %s to type %s for partition value", partition, type));
    }
  }

  /**
   * Ensures the give columns of the row data type are not nullable(for example, the primary keys).
   *
   * @param dataType  The row data type, datatype logicaltype must be rowtype
   * @param pkColumns The primary keys
   * @return a new row data type if any column nullability is tweaked or the original data type
   */
  public static DataType ensureColumnsAsNonNullable(DataType dataType, @Nullable List<String> pkColumns) {
    if (pkColumns == null || pkColumns.isEmpty()) {
      return dataType;
    }
    LogicalType dataTypeLogicalType = dataType.getLogicalType();
    if (!(dataTypeLogicalType instanceof RowType)) {
      throw new RuntimeException("The datatype to be converted must be row type, but this type is :" + dataTypeLogicalType.getClass());
    }
    RowType rowType = (RowType) dataTypeLogicalType;
    List<DataType> originalFieldTypes = dataType.getChildren();
    List<String> fieldNames = rowType.getFieldNames();
    List<DataType> fieldTypes = new ArrayList<>();
    boolean tweaked = false;
    for (int i = 0; i < fieldNames.size(); i++) {
      if (pkColumns.contains(fieldNames.get(i)) && rowType.getTypeAt(i).isNullable()) {
        fieldTypes.add(originalFieldTypes.get(i).notNull());
        tweaked = true;
      } else {
        fieldTypes.add(originalFieldTypes.get(i));
      }
    }
    if (!tweaked) {
      return dataType;
    }
    List<DataTypes.Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      fields.add(DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)));
    }
    return DataTypes.ROW(fields.stream().toArray(DataTypes.Field[]::new)).notNull();
  }

  /**
   * Adds the Hoodie metadata fields to the given row type.
   */
  public static RowType addMetadataFields(
      RowType rowType,
      boolean withOperationField) {
    List<RowType.RowField> mergedFields = new ArrayList<>();
    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();

    RowType.RowField commitTimeField =
        new RowType.RowField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, metadataFieldType, "commit time");
    RowType.RowField commitSeqnoField =
        new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType, "commit seqno");
    RowType.RowField recordKeyField =
        new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType, "record key");
    RowType.RowField partitionPathField =
        new RowType.RowField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, metadataFieldType, "partition path");
    RowType.RowField fileNameField =
        new RowType.RowField(HoodieRecord.FILENAME_METADATA_FIELD, metadataFieldType, "field name");

    mergedFields.add(commitTimeField);
    mergedFields.add(commitSeqnoField);
    mergedFields.add(recordKeyField);
    mergedFields.add(partitionPathField);
    mergedFields.add(fileNameField);

    if (withOperationField) {
      RowType.RowField operationField =
          new RowType.RowField(HoodieRecord.OPERATION_METADATA_FIELD, metadataFieldType, "operation");
      mergedFields.add(operationField);
    }

    mergedFields.addAll(rowType.getFields());

    return new RowType(false, mergedFields);
  }
}
