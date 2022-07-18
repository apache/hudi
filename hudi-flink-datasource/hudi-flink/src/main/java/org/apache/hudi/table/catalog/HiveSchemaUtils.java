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

package org.apache.hudi.table.catalog;

import org.apache.hudi.common.util.StringUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for Hive field schema.
 */
public class HiveSchemaUtils {
  /** Get field names from field schemas. */
  public static List<String> getFieldNames(List<FieldSchema> fieldSchemas) {
    List<String> names = new ArrayList<>(fieldSchemas.size());
    for (FieldSchema fs : fieldSchemas) {
      names.add(fs.getName());
    }
    return names;
  }

  public static org.apache.flink.table.api.Schema convertTableSchema(Table hiveTable) {
    List<FieldSchema> allCols = new ArrayList<>(hiveTable.getSd().getCols());
    allCols.addAll(hiveTable.getPartitionKeys());

    String pkConstraintName = hiveTable.getParameters().get(TableOptionProperties.PK_CONSTRAINT_NAME);
    List<String> primaryColNames = StringUtils.isNullOrEmpty(pkConstraintName)
        ? Collections.EMPTY_LIST
        : StringUtils.split(hiveTable.getParameters().get(TableOptionProperties.PK_COLUMNS),",");

    String[] colNames = new String[allCols.size()];
    DataType[] colTypes = new DataType[allCols.size()];

    for (int i = 0; i < allCols.size(); i++) {
      FieldSchema fs = allCols.get(i);

      colNames[i] = fs.getName();
      colTypes[i] =
          toFlinkType(TypeInfoUtils.getTypeInfoFromTypeString(fs.getType()));
      if (primaryColNames.contains(colNames[i])) {
        colTypes[i] = colTypes[i].notNull();
      }
    }

    org.apache.flink.table.api.Schema.Builder builder = org.apache.flink.table.api.Schema.newBuilder().fromFields(colNames, colTypes);
    if (!StringUtils.isNullOrEmpty(pkConstraintName)) {
      builder.primaryKeyNamed(pkConstraintName, primaryColNames);
    }

    return builder.build();
  }

  /**
   * Convert Hive data type to a Flink data type.
   *
   * @param hiveType a Hive data type
   * @return the corresponding Flink data type
   */
  public static DataType toFlinkType(TypeInfo hiveType) {
    checkNotNull(hiveType, "hiveType cannot be null");

    switch (hiveType.getCategory()) {
      case PRIMITIVE:
        return toFlinkPrimitiveType((PrimitiveTypeInfo) hiveType);
      case LIST:
        ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
        return DataTypes.ARRAY(toFlinkType(listTypeInfo.getListElementTypeInfo()));
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveType;
        return DataTypes.MAP(
            toFlinkType(mapTypeInfo.getMapKeyTypeInfo()),
            toFlinkType(mapTypeInfo.getMapValueTypeInfo()));
      case STRUCT:
        StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType;

        List<String> names = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> typeInfos = structTypeInfo.getAllStructFieldTypeInfos();

        DataTypes.Field[] fields = new DataTypes.Field[names.size()];

        for (int i = 0; i < fields.length; i++) {
          fields[i] = DataTypes.FIELD(names.get(i), toFlinkType(typeInfos.get(i)));
        }

        return DataTypes.ROW(fields);
      default:
        throw new UnsupportedOperationException(
            String.format("Flink doesn't support Hive data type %s yet.", hiveType));
    }
  }

  private static DataType toFlinkPrimitiveType(PrimitiveTypeInfo hiveType) {
    checkNotNull(hiveType, "hiveType cannot be null");

    switch (hiveType.getPrimitiveCategory()) {
      case CHAR:
        return DataTypes.CHAR(((CharTypeInfo) hiveType).getLength());
      case VARCHAR:
        return DataTypes.VARCHAR(((VarcharTypeInfo) hiveType).getLength());
      case STRING:
        return DataTypes.STRING();
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case BYTE:
        return DataTypes.TINYINT();
      case SHORT:
        return DataTypes.SMALLINT();
      case INT:
        return DataTypes.INT();
      case LONG:
        return DataTypes.BIGINT();
      case FLOAT:
        return DataTypes.FLOAT();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case DATE:
        return DataTypes.DATE();
      case TIMESTAMP:
        return DataTypes.TIMESTAMP(9);
      case BINARY:
        return DataTypes.BYTES();
      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) hiveType;
        return DataTypes.DECIMAL(
            decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Flink doesn't support Hive primitive type %s yet", hiveType));
    }
  }

  /** Create Hive columns from Flink TableSchema. */
  public static List<FieldSchema> createHiveColumns(TableSchema schema) {
    String[] fieldNames = schema.getFieldNames();
    DataType[] fieldTypes = schema.getFieldDataTypes();

    List<FieldSchema> columns = new ArrayList<>(fieldNames.length);

    for (int i = 0; i < fieldNames.length; i++) {
      columns.add(
          new FieldSchema(
              fieldNames[i],
              toHiveTypeInfo(fieldTypes[i], true).getTypeName(),
              null));
    }

    return columns;
  }

  /**
   * Convert Flink DataType to Hive TypeInfo. For types with a precision parameter, e.g.
   * timestamp, the supported precisions in Hive and Flink can be different. Therefore the
   * conversion will fail for those types if the precision is not supported by Hive and
   * checkPrecision is true.
   *
   * @param dataType a Flink DataType
   * @param checkPrecision whether to fail the conversion if the precision of the DataType is not
   *     supported by Hive
   * @return the corresponding Hive data type
   */
  public static TypeInfo toHiveTypeInfo(DataType dataType, boolean checkPrecision) {
    checkNotNull(dataType, "type cannot be null");
    LogicalType logicalType = dataType.getLogicalType();
    return logicalType.accept(new TypeInfoLogicalTypeVisitor(dataType, checkPrecision));
  }
}
