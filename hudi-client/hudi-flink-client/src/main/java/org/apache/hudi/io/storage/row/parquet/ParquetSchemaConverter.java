/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage.row.parquet;

import org.apache.hudi.common.util.collection.Pair;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;

/**
 * Schema converter converts Parquet schema to and from Flink internal types.
 *
 * <p>Reference org.apache.flink.formats.parquet.utils.ParquetSchemaConverter to support timestamp of INT64 8 bytes.
 */
@Slf4j
public class ParquetSchemaConverter {

  static final String MAP_REPEATED_NAME = "key_value";
  static final String MAP_KEY_NAME = "key";
  static final String MAP_VALUE_NAME = "value";
  static final String LIST_REPEATED_NAME = "list";
  static final String LIST_ELEMENT_NAME = "element";

  public static RowType convertToRowType(MessageType messageType) {
    List<RowType.RowField> dataFields =
        messageType.asGroupType().getFields().stream()
            .map(ParquetSchemaConverter::convertToRowField)
            .collect(Collectors.toList());
    return new RowType(dataFields);
  }

  public static RowType.RowField convertToRowField(Type parquetType) {
    LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();
    DataType dataType;

    if (parquetType.isPrimitive()) {
      switch (parquetType.asPrimitiveType().getPrimitiveTypeName()) {
        case BINARY:
          if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            dataType = DataTypes.STRING();
          } else {
            dataType = DataTypes.BYTES();
          }
          break;
        case BOOLEAN:
          dataType = DataTypes.BOOLEAN();
          break;
        case FLOAT:
          dataType = DataTypes.FLOAT();
          break;
        case DOUBLE:
          dataType = DataTypes.DOUBLE();
          break;
        case INT32:
          if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
            dataType =
                DataTypes.of(new DecimalType(decimalType.getPrecision(), decimalType.getScale()));
          } else if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
                (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
            int bitWidth = intType.getBitWidth();
            if (bitWidth == 8) {
              dataType = DataTypes.TINYINT();
            } else if (bitWidth == 16) {
              dataType = DataTypes.SMALLINT();
            } else {
              dataType = DataTypes.INT();
            }
          } else if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            dataType = DataTypes.DATE();
          } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
            dataType = DataTypes.TIME();
          } else {
            dataType = DataTypes.INT();
          }
          break;
        case INT64:
          if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
                (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
            dataType =
                DataTypes.of(new DecimalType(decimalType.getPrecision(), decimalType.getScale()));
          } else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
                (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
            int precision = timestampType.getUnit().equals(LogicalTypeAnnotation.TimeUnit.MILLIS) ? 3 : 6;
            dataType = DataTypes.of(new TimestampType(precision));
          } else {
            dataType = DataTypes.BIGINT();
          }
          break;
        case INT96:
          dataType = DataTypes.of(new TimestampType(9));
          break;
        case FIXED_LEN_BYTE_ARRAY:
          LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
              (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;
          dataType = DataTypes.of(new DecimalType(decimalType.getPrecision(), decimalType.getScale()));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type: " + parquetType);
      }
      if (parquetType.getRepetition().equals(Type.Repetition.REQUIRED)) {
        dataType = dataType.notNull();
      }
      return new RowType.RowField(parquetType.getName(), dataType.getLogicalType());
    } else {
      GroupType groupType = parquetType.asGroupType();
      if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
        dataType = DataTypes.of(new ArrayType(convertToRowField(parquetListElementType(groupType)).getType()));
      } else if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
        Pair<Type, Type> keyValueType = parquetMapKeyValueType(groupType);
        // Since parquet does not support nullable key, when converting
        // back to DataType, set as nullable by default.
        dataType = DataTypes.of(
            new MapType(
                convertToRowField(keyValueType.getLeft()).getType().copy(true),
                convertToRowField(keyValueType.getRight()).getType()));
      } else {
        dataType =
            DataTypes.of(new RowType(
                groupType.getFields().stream()
                    .map(ParquetSchemaConverter::convertToRowField)
                    .collect(Collectors.toList())));
      }
    }

    if (parquetType.getRepetition().equals(Type.Repetition.REQUIRED)) {
      dataType = dataType.notNull();
    }

    return new RowType.RowField(parquetType.getName(), dataType.getLogicalType());
  }

  public static Type parquetListElementType(GroupType listType) {
    return listType.getType(LIST_REPEATED_NAME).asGroupType().getType(LIST_ELEMENT_NAME);
  }

  public static Pair<Type, Type> parquetMapKeyValueType(GroupType mapType) {
    GroupType keyValue = mapType.getType(MAP_REPEATED_NAME).asGroupType();
    return Pair.of(keyValue.getType(MAP_KEY_NAME), keyValue.getType(MAP_VALUE_NAME));
  }

  public static MessageType convertToParquetMessageType(String name, RowType rowType) {
    Type[] types = new Type[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      String fieldName = rowType.getFieldNames().get(i);
      LogicalType fieldType = rowType.getTypeAt(i);
      types[i] = convertToParquetType(fieldName, fieldType, fieldType.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED);
    }
    return new MessageType(name, types);
  }

  private static Type convertToParquetType(
      String name, LogicalType type, Type.Repetition repetition) {
    switch (type.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
            .as(OriginalType.UTF8)
            .named(name);
      case BOOLEAN:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
            .named(name);
      case BINARY:
      case VARBINARY:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
            .named(name);
      case DECIMAL:
        int precision = ((DecimalType) type).getPrecision();
        int scale = ((DecimalType) type).getScale();
        int numBytes = computeMinBytesForDecimalPrecision(precision);
        return Types.primitive(
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
            .as(LogicalTypeAnnotation.decimalType(scale, precision))
            .length(numBytes)
            .named(name);
      case TINYINT:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(LogicalTypeAnnotation.intType(8, true))
            .named(name);
      case SMALLINT:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(LogicalTypeAnnotation.intType(16, true))
            .named(name);
      case INTEGER:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .named(name);
      case BIGINT:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .named(name);
      case FLOAT:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
            .named(name);
      case DOUBLE:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
            .named(name);
      case DATE:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(LogicalTypeAnnotation.dateType())
            .named(name);
      case TIME_WITHOUT_TIME_ZONE:
        return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .as(LogicalTypeAnnotation.timeType(true, TimeUnit.MILLIS))
            .named(name);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        TimestampType timestampType = (TimestampType) type;
        if (timestampType.getPrecision() == 3 || timestampType.getPrecision() == 6) {
          TimeUnit timeunit = timestampType.getPrecision() == 3 ? TimeUnit.MILLIS : TimeUnit.MICROS;
          return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(true, timeunit))
              .named(name);
        } else {
          return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition)
              .named(name);
        }
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) type;
        if (localZonedTimestampType.getPrecision() == 3 || localZonedTimestampType.getPrecision() == 6) {
          TimeUnit timeunit = localZonedTimestampType.getPrecision() == 3 ? TimeUnit.MILLIS : TimeUnit.MICROS;
          return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
              .as(LogicalTypeAnnotation.timestampType(false, timeunit))
              .named(name);
        } else {
          return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition)
              .named(name);
        }
      case ARRAY:
        // align with Spark And Avro regarding the standard mode array type, see:
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
        //
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        ArrayType arrayType = (ArrayType) type;
        Type.Repetition eleRepetition =
            arrayType.getElementType().isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        return ConversionPatterns.listOfElements(
            repetition, name, convertToParquetType("element", arrayType.getElementType(), eleRepetition));
      case MAP:
        // <map-repetition> group <name> (MAP) {
        //   repeated group key_value {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        MapType mapType = (MapType) type;
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        return Types
            .buildGroup(repetition).as(OriginalType.MAP)
            .addField(
                Types
                    .repeatedGroup()
                    .addField(convertToParquetType("key", keyType, Type.Repetition.REQUIRED))
                    .addField(convertToParquetType("value", valueType, repetition))
                    .named("key_value"))
            .named(name);
      case ROW:
        RowType rowType = (RowType) type;
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);
        rowType.getFields().forEach(field -> builder.addField(convertToParquetType(field.getName(), field.getType(), repetition)));
        return builder.named(name);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  public static int computeMinBytesForDecimalPrecision(int precision) {
    int numBytes = 1;
    while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
      numBytes += 1;
    }
    return numBytes;
  }
}
