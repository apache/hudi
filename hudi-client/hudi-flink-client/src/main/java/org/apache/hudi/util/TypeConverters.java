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

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MAP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/**
 * Tool class used to perform supported casts from a {@link LogicalType} to another
 * as defined in Hudi Comprehensive Schema Evolution's scope.
 */
@Internal
public class TypeConverters {

  @FunctionalInterface
  public interface TypeConverter extends Serializable {
    Object convert(Object val);
  }

  public static final TypeConverter NOOP_CONVERTER = new TypeConverter() {
    private static final long serialVersionUID = 1L;

    @Override
    public Object convert(Object val) {
      return val;
    }
  };

  public static TypeConverter getInstance(LogicalType fromType, LogicalType toType) {
    LogicalTypeRoot from = fromType.getTypeRoot();
    LogicalTypeRoot to = toType.getTypeRoot();

    switch (to) {
      case BIGINT: {
        if (from == INTEGER) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return ((Number) val).longValue();
            }
          };
        }
        break;
      }

      case FLOAT: {
        if (from == INTEGER || from == BIGINT) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return ((Number) val).floatValue();
            }
          };
        }
        break;
      }

      case DOUBLE: {
        if (from == INTEGER || from == BIGINT) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return ((Number) val).doubleValue();
            }
          };
        }
        if (from == FLOAT) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return Double.parseDouble(val.toString());
            }
          };
        }
        break;
      }

      case DECIMAL: {
        if (from == INTEGER || from == BIGINT || from == DOUBLE) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return toDecimalData((Number) val, toType);
            }
          };
        }
        if (from == FLOAT) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return toDecimalData(Double.parseDouble(val.toString()), toType);
            }
          };
        }
        if (from == VARCHAR) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return toDecimalData(Double.parseDouble(val.toString()), toType);
            }
          };
        }
        if (from == DECIMAL) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return toDecimalData(((DecimalData) val).toBigDecimal(), toType);
            }
          };
        }
        break;
      }

      case VARCHAR: {
        if (from == INTEGER
            || from == BIGINT
            || from == FLOAT
            || from == DOUBLE
            || from == DECIMAL) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return new BinaryStringData(String.valueOf(val));
            }
          };
        }
        if (from == DATE) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return new BinaryStringData(LocalDate.ofEpochDay(((Integer) val).longValue()).toString());
            }
          };
        }
        break;
      }

      case DATE: {
        if (from == VARCHAR) {
          return new TypeConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object val) {
              return (int) LocalDate.parse(val.toString()).toEpochDay();
            }
          };
        }
        break;
      }

      case ARRAY: {
        if (from == ARRAY) {
          try {
            LogicalType fromElementType =  fromType.getChildren().get(0);
            LogicalType toElementType = toType.getChildren().get(0);
            return createArrayConverter(fromElementType, toElementType);
          } catch (IllegalStateException ise) {
            return null;
          }
        }
        break;
      }

      case MAP: {
        if (from == MAP) {
          try {
            return createMapConverter(fromType, toType);
          } catch (IllegalStateException ise) {
            return null;
          }
        }
        break;
      }

      case ROW: {
        if (from == ROW) {
          // Assumption: InternalSchemaManager should produce a cast that is of the same size
          try {
            // note: InternalSchema.merge guarantees that the schema to be read fromType is orientated in the same order as toType
            // hence, we can match types by position as it is guaranteed that it is referencing the same field
            // ignoring error messages here as the calling function's checked exception will ignore it
            ValidationUtils.checkArgument(fromType.getChildren().size() == toType.getChildren().size());
            return createRowConverter(fromType, toType);
          } catch (IllegalStateException ise) {
            return null;
          }
        }
        break;
      }

      default:
    }
    return null;
  }

  private static TypeConverter createArrayConverter(LogicalType fromType, LogicalType toType) {
    final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(fromType);
    final TypeConverter converter = getInstance(fromType, toType);

    // need to handle nulls to prevent NullPointerException in #getConversion()
    // ignoring error messages here as the calling function's checked exception will ignore it
    ValidationUtils.checkState(converter != null);

    return new TypeConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(Object val) {
        ArrayData array = (ArrayData) val;
        Object[] objects = new Object[array.size()];
        for (int i = 0; i < array.size(); i++) {
          Object fromObj = elementGetter.getElementOrNull(array, i);
          // need to handle nulls to prevent NullPointerException in #getConversion()
          Object toObj = fromObj != null ? converter.convert(fromObj) : null;
          objects[i] = toObj;
        }
        return new GenericArrayData(objects);
      }
    };
  }

  private static TypeConverter createMapConverter(LogicalType fromType, LogicalType toType) {
    // no schema evolution is allowed on the keyType, hence, we only need to care about the valueType
    final LogicalType keyType = fromType.getChildren().get(0);
    final LogicalType fromValueType = fromType.getChildren().get(1);
    final LogicalType toValueType = toType.getChildren().get(1);
    final ArrayData.ElementGetter keyElementGetter = ArrayData.createElementGetter(keyType);
    final ArrayData.ElementGetter valueElementGetter = ArrayData.createElementGetter(fromValueType);
    final TypeConverter converter = getInstance(fromValueType, toValueType);

    // need to handle nulls to prevent NullPointerException in #getConversion()
    // ignoring error messages here as the calling function's checked exception will ignore it
    ValidationUtils.checkState(converter != null);

    return new TypeConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(Object val) {
        final Map<Object, Object> result = new HashMap<>();
        MapData map = (MapData) val;
        for (int i = 0; i < map.size(); i++) {
          Object keyObj = keyElementGetter.getElementOrNull(map.keyArray(), i);
          Object fromObj = valueElementGetter.getElementOrNull(map.valueArray(), i);
          // need to handle nulls to prevent NullPointerException in #getConversion()
          Object toObj = fromObj != null ? converter.convert(fromObj) : null;
          result.put(keyObj, toObj);
        }
        return new GenericMapData(result);
      }
    };
  }

  private static TypeConverter createRowConverter(LogicalType fromType, LogicalType toType) {
    final List<LogicalType> fromChildren = fromType.getChildren();
    final List<LogicalType> toChildren = toType.getChildren();
    final FieldGetter[] fieldGetters = IntStream
        .range(0, fromChildren.size())
        .mapToObj(i -> RowData.createFieldGetter(fromChildren.get(i), i))
        .toArray(FieldGetter[]::new);
    final TypeConverter[] converters = IntStream
        .range(0, fromChildren.size())
        .mapToObj(i -> {
          LogicalType fromChild = fromChildren.get(i);
          LogicalType toChild = toChildren.get(i);
          if (isPrimitiveTypeRootEqual(fromChild.getTypeRoot(), toChild.getTypeRoot())) {
            return NOOP_CONVERTER;
          } else {
            return getInstance(fromChild, toChild);
          }
        })
        .toArray(TypeConverter[]::new);

    // need to handle nulls to prevent NullPointerException in #getConversion()
    // ignoring error messages here as the calling function's checked exception will ignore it
    ValidationUtils.checkState(Arrays.stream(converters).noneMatch(Objects::isNull));

    return new TypeConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(Object val) {
        RowData row = (RowData) val;
        GenericRowData rowData = new GenericRowData(toType.getChildren().size());
        for (int i = 0; i < toChildren.size(); i++) {
          Object fromObj = fieldGetters[i].getFieldOrNull(row);
          // need to handle nulls to prevent NullPointerException in #getConversion()
          Object toObj = fromObj != null ? converters[i].convert(fromObj) : null;
          rowData.setField(i, toObj);
        }
        return rowData;
      }
    };
  }

  private static boolean isPrimitiveTypeRootEqual(LogicalTypeRoot fromType, LogicalTypeRoot toType) {
    // not-null constraints will be ignored when TypeRoot is used
    boolean isComplex = fromType.equals(ARRAY) || fromType.equals(MAP) || fromType.equals(ROW);
    return !isComplex && fromType.equals(toType);
  }

  /**
   * Helper function to convert a Number object to Flink's DecimalData format.
   *
   * @param val         Number object to be converted
   * @param decimalType DecimalType containing the output decimal's precision and scale specifications
   * @return Converted decimal that is wrapped in Flink's DecimalData object.
   */
  private static DecimalData toDecimalData(Number val, LogicalType decimalType) {
    BigDecimal valAsDecimal = BigDecimal.valueOf(val.doubleValue());
    return toDecimalData(valAsDecimal, decimalType);
  }

  /**
   * Helper function to convert a BigDecimal object to Flink's DecimalData format.
   *
   * @param valAsDecimal BigDecimal object to be converted
   * @param decimalType  DecimalType containing the output decimal's precision and scale specifications
   * @return Converted decimal that is wrapped in Flink's DecimalData object.
   */
  private static DecimalData toDecimalData(BigDecimal valAsDecimal, LogicalType decimalType) {
    return DecimalData.fromBigDecimal(
        valAsDecimal,
        ((DecimalType) decimalType).getPrecision(),
        ((DecimalType) decimalType).getScale());
  }
}
