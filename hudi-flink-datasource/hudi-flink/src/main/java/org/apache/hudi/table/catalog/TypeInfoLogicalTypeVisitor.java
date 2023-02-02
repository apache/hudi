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

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Create a TypeInfoLogicalTypeVisitor for hoodie table.
 */
public class TypeInfoLogicalTypeVisitor extends LogicalTypeDefaultVisitor<TypeInfo> {
  private final LogicalType type;

  TypeInfoLogicalTypeVisitor(DataType dataType) {
    this(dataType.getLogicalType());
  }

  TypeInfoLogicalTypeVisitor(LogicalType type) {
    this.type = type;
  }

  @Override
  public TypeInfo visit(CharType charType) {
    // hoodie only supports avro compatible data type
    return TypeInfoFactory.stringTypeInfo;
  }

  @Override
  public TypeInfo visit(VarCharType varCharType) {
    // hoodie only supports avro compatible data type
    return TypeInfoFactory.stringTypeInfo;
  }

  @Override
  public TypeInfo visit(BooleanType booleanType) {
    return TypeInfoFactory.booleanTypeInfo;
  }

  @Override
  public TypeInfo visit(VarBinaryType varBinaryType) {
    // Flink's BytesType is defined as VARBINARY(Integer.MAX_VALUE)
    // We don't have more information in LogicalTypeRoot to distinguish BytesType and a
    // VARBINARY(Integer.MAX_VALUE) instance
    // Thus always treat VARBINARY(Integer.MAX_VALUE) as BytesType
    if (varBinaryType.getLength() == VarBinaryType.MAX_LENGTH) {
      return TypeInfoFactory.binaryTypeInfo;
    }
    return defaultMethod(varBinaryType);
  }

  @Override
  public TypeInfo visit(DecimalType decimalType) {
    // Flink and Hive share the same precision and scale range
    // Flink already validates the type so we don't need to validate again here
    return TypeInfoFactory.getDecimalTypeInfo(
        decimalType.getPrecision(), decimalType.getScale());
  }

  @Override
  public TypeInfo visit(TinyIntType tinyIntType) {
    // hoodie only supports avro compatible data type
    return TypeInfoFactory.intTypeInfo;
  }

  @Override
  public TypeInfo visit(SmallIntType smallIntType) {
    // hoodie only supports avro compatible data type
    return TypeInfoFactory.intTypeInfo;
  }

  @Override
  public TypeInfo visit(IntType intType) {
    return TypeInfoFactory.intTypeInfo;
  }

  @Override
  public TypeInfo visit(BigIntType bigIntType) {
    return TypeInfoFactory.longTypeInfo;
  }

  @Override
  public TypeInfo visit(FloatType floatType) {
    return TypeInfoFactory.floatTypeInfo;
  }

  @Override
  public TypeInfo visit(DoubleType doubleType) {
    return TypeInfoFactory.doubleTypeInfo;
  }

  @Override
  public TypeInfo visit(DateType dateType) {
    return TypeInfoFactory.dateTypeInfo;
  }

  @Override
  public TypeInfo visit(TimestampType timestampType) {
    int precision = timestampType.getPrecision();
    // see org.apache.hudi.hive.util.HiveSchemaUtil#convertField for details.
    // default supports timestamp
    if (precision == 6) {
      return TypeInfoFactory.timestampTypeInfo;
    } else {
      return TypeInfoFactory.longTypeInfo;
    }
  }

  @Override
  public TypeInfo visit(ArrayType arrayType) {
    LogicalType elementType = arrayType.getElementType();
    TypeInfo elementTypeInfo = elementType.accept(this);
    if (null != elementTypeInfo) {
      return TypeInfoFactory.getListTypeInfo(elementTypeInfo);
    } else {
      return defaultMethod(arrayType);
    }
  }

  @Override
  public TypeInfo visit(MapType mapType) {
    LogicalType keyType = mapType.getKeyType();
    LogicalType valueType = mapType.getValueType();
    TypeInfo keyTypeInfo = keyType.accept(this);
    TypeInfo valueTypeInfo = valueType.accept(this);
    if (null == keyTypeInfo || null == valueTypeInfo) {
      return defaultMethod(mapType);
    } else {
      return TypeInfoFactory.getMapTypeInfo(keyTypeInfo, valueTypeInfo);
    }
  }

  @Override
  public TypeInfo visit(RowType rowType) {
    List<String> names = rowType.getFieldNames();
    List<TypeInfo> typeInfos = new ArrayList<>(names.size());
    for (String name : names) {
      TypeInfo typeInfo = rowType.getTypeAt(rowType.getFieldIndex(name)).accept(this);
      if (null != typeInfo) {
        typeInfos.add(typeInfo);
      } else {
        return defaultMethod(rowType);
      }
    }
    return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
  }

  @Override
  public TypeInfo visit(NullType nullType) {
    return TypeInfoFactory.voidTypeInfo;
  }

  @Override
  protected TypeInfo defaultMethod(LogicalType logicalType) {
    throw new UnsupportedOperationException(
        String.format(
            "Flink doesn't support converting type %s to Hive type yet.",
            type.toString()));
  }
}
