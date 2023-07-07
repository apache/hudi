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

package org.apache.hudi.table.format;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.util.RowDataCastProjection;
import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
 * CastMap is responsible for conversion of flink types when full schema evolution enabled.
 *
 * <p>Supported cast conversions:
 * <ul>
 *   <li>Integer => Long, Float, Double, Decimal, String</li>
 *   <li>Long => Float, Double, Decimal, String</li>
 *   <li>Float => Double, Decimal, String</li>
 *   <li>Double => Decimal, String</li>
 *   <li>Decimal => Decimal, String</li>
 *   <li>String => Decimal, Date</li>
 *   <li>Date => String</li>
 * </ul>
 */
public final class CastMap implements Serializable {

  private static final long serialVersionUID = 1L;

  // Maps position to corresponding cast
  private final Map<Integer, Cast> castMap = new HashMap<>();

  private DataType[] fileFieldTypes;

  public Option<RowDataProjection> toRowDataProjection(int[] selectedFields) {
    if (castMap.isEmpty()) {
      return Option.empty();
    }
    LogicalType[] requiredType = new LogicalType[selectedFields.length];
    for (int i = 0; i < selectedFields.length; i++) {
      requiredType[i] = fileFieldTypes[selectedFields[i]].getLogicalType();
    }
    return Option.of(new RowDataCastProjection(requiredType, this));
  }

  public Object castIfNeeded(int pos, Object val) {
    Cast cast = castMap.get(pos);
    if (cast == null) {
      return val;
    }
    return cast.convert(val);
  }

  public DataType[] getFileFieldTypes() {
    return fileFieldTypes;
  }

  public void setFileFieldTypes(DataType[] fileFieldTypes) {
    this.fileFieldTypes = fileFieldTypes;
  }

  @VisibleForTesting
  void add(int pos, LogicalType fromType, LogicalType toType) {
    Function<Object, Object> conversion = getConversion(fromType, toType);
    if (conversion == null) {
      throw new IllegalArgumentException(String.format("Cannot create cast %s => %s at pos %s", fromType, toType, pos));
    }
    add(pos, new Cast(fromType, toType, conversion));
  }

  private void add(int pos, Cast cast) {
    castMap.put(pos, cast);
  }

  /**
   * Helper function to create a callable conversion function for supported type castings.
   * <p>
   * NOTE: The argument must be non-null when applying it to the returned function.
   *
   * @param fromType The input LogicalType of the value to be converted from
   * @param toType   The output LogicalType of the value to be converted to
   * @return Function to perform the required conversion for the provided from and to types
   */
  private static Function<Object, Object> getConversion(LogicalType fromType, LogicalType toType) {
    LogicalTypeRoot from = fromType.getTypeRoot();
    LogicalTypeRoot to = toType.getTypeRoot();
    switch (to) {
      case BIGINT: {
        if (from == INTEGER) {
          return val -> ((Number) val).longValue();
        }
        break;
      }
      case FLOAT: {
        if (from == INTEGER || from == BIGINT) {
          return val -> ((Number) val).floatValue();
        }
        break;
      }
      case DOUBLE: {
        if (from == INTEGER || from == BIGINT) {
          return val -> ((Number) val).doubleValue();
        }
        if (from == FLOAT) {
          return val -> Double.parseDouble(val.toString());
        }
        break;
      }
      case DECIMAL: {
        if (from == INTEGER || from == BIGINT || from == DOUBLE) {
          return val -> toDecimalData((Number) val, toType);
        }
        if (from == FLOAT) {
          return val -> toDecimalData(Double.parseDouble(val.toString()), toType);
        }
        if (from == VARCHAR) {
          return val -> toDecimalData(Double.parseDouble(val.toString()), toType);
        }
        if (from == DECIMAL) {
          return val -> toDecimalData(((DecimalData) val).toBigDecimal(), toType);
        }
        break;
      }
      case VARCHAR: {
        if (from == INTEGER
            || from == BIGINT
            || from == FLOAT
            || from == DOUBLE
            || from == DECIMAL) {
          return val -> new BinaryStringData(String.valueOf(val));
        }
        if (from == DATE) {
          return val -> new BinaryStringData(LocalDate.ofEpochDay(((Integer) val).longValue()).toString());
        }
        break;
      }
      case DATE: {
        if (from == VARCHAR) {
          return val -> (int) LocalDate.parse(val.toString()).toEpochDay();
        }
        break;
      }
      case ARRAY: {
        if (from == ARRAY) {
          LogicalType fromElementType =  fromType.getChildren().get(0);
          LogicalType toElementType = toType.getChildren().get(0);
          try {
            return array -> doArrayConversion((ArrayData) array, fromElementType, toElementType);
          } catch (IllegalStateException ise) {
            return null;
          }
        }
        break;
      }
      case MAP: {
        if (from == MAP) {
          try {
            return map -> doMapConversion((MapData) map, fromType, toType);
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
            return row -> doRowConversion((RowData) row, fromType, toType);
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

  /**
   * Helper function to perform convert an arrayData from one LogicalType to another.
   *
   * @param array    Non-null array data to be converted; however array-elements are allowed to be null
   * @param fromType The input LogicalType of the row data to be converted from
   * @param toType   The output LogicalType of the row data to be converted to
   * @return Converted array that has the structure/specifications of that defined by the output LogicalType
   */
  private static ArrayData doArrayConversion(@Nonnull ArrayData array, LogicalType fromType, LogicalType toType) {
    // using Object type here as primitives are not allowed to be null
    Object[] objects = new Object[array.size()];
    ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(fromType);
    for (int i = 0; i < array.size(); i++) {
      Object fromObject = elementGetter.getElementOrNull(array, i);
      Function<Object, Object> conversion = getConversion(fromType, toType);
      // need to handle nulls to prevent NullPointerException in #getConversion()
      ValidationUtils.checkState(conversion != null, String.format("Failed to perform ARRAY conversion when casting %s => %s", fromType, toType));
      Object toObject = fromObject != null ? conversion .apply(fromObject) : null;
      objects[i] = toObject;
    }
    return new GenericArrayData(objects);
  }

  /**
   * Helper function to perform convert a MapData from one LogicalType to another.
   *
   * @param map      Non-null map data to be converted; however, values are allowed to be null
   * @param fromType The input LogicalType of the row data to be converted from
   * @param toType   The output LogicalType of the row data to be converted to
   * @return Converted map that has the structure/specifications of that defined by the output LogicalType
   */
  private static MapData doMapConversion(@Nonnull MapData map, LogicalType fromType, LogicalType toType) {
    // no schema evolution is allowed on the keyType, hence, we only need to care about the valueType
    LogicalType fromValueType = fromType.getChildren().get(1);
    LogicalType toValueType = toType.getChildren().get(1);
    LogicalType keyType = fromType.getChildren().get(0);

    final Map<Object, Object> result = new HashMap<>();
    ArrayData.ElementGetter keyElementGetter = ArrayData.createElementGetter(keyType);
    ArrayData.ElementGetter valueElementGetter = ArrayData.createElementGetter(fromValueType);
    for (int i = 0; i < map.size(); i++) {
      Object keyObject = keyElementGetter.getElementOrNull(map.keyArray(), i);
      Object fromObject = valueElementGetter.getElementOrNull(map.valueArray(), i);
      Function<Object, Object> conversion = getConversion(fromValueType, toValueType);

      // need to handle nulls to prevent NullPointerException in #getConversion()
      ValidationUtils.checkState(conversion != null, String.format("Failed to perform MAP conversion when cast %s => %s", fromType, toType));
      Object toObject = fromObject != null ? conversion.apply(fromObject) : null;
      result.put(keyObject, toObject);
    }
    return new GenericMapData(result);
  }

  /**
   * Helper function to perform convert a RowData from one LogicalType to another.
   *
   * @param row      Non-null row data to be converted; however, fields might contain nulls
   * @param fromType The input LogicalType of the row data to be converted from
   * @param toType   The output LogicalType of the row data to be converted to
   * @return Converted row that has the structure/specifications of that defined by the output LogicalType
   */
  private static RowData doRowConversion(@Nonnull RowData row, LogicalType fromType, LogicalType toType) {
    // note: InternalSchema.merge guarantees that the schema to be read fromType is orientated in the same order as toType
    // hence, we can match types by position as it is guaranteed that it is referencing the same field
    List<LogicalType> fromChildren = fromType.getChildren();
    List<LogicalType> toChildren = toType.getChildren();
    ValidationUtils.checkArgument(fromChildren.size() == toChildren.size(),
        "fromType [" + fromType + "] size: != toType [" + toType + "] size");

    GenericRowData rowData = new GenericRowData(toType.getChildren().size());
    for (int i = 0; i < toChildren.size(); i++) {
      Object fromVal = RowData.createFieldGetter(fromChildren.get(i), i).getFieldOrNull(row);
      Object toVal;

      // need to handle nulls to prevent NullPointerException in #getConversion()
      if (fromChildren.get(i).equals(toChildren.get(i)) || fromVal == null) {
        // no conversion required if the types are the same / fromVal is null
        toVal = fromVal;
      } else {
        // conversion required
        Function<Object, Object> conversion = getConversion(fromChildren.get(i), toChildren.get(i));
        ValidationUtils.checkState(conversion != null, String.format("Failed to perform ROW conversion when casting %s => %s", fromType, toType));
        toVal = conversion.apply(fromVal);
      }

      rowData.setField(i, toVal);
    }
    return rowData;
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

  /**
   * Fields {@link Cast#from} and {@link Cast#to} are redundant due to {@link Cast#convert(Object)} determines conversion.
   * However, it is convenient to debug {@link CastMap} when {@link Cast#toString()} prints types.
   */
  private static final class Cast implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LogicalType from;
    private final LogicalType to;
    private final Function<Object, Object> conversion;

    Cast(LogicalType from, LogicalType to, Function<Object, Object> conversion) {
      this.from = from;
      this.to = to;
      this.conversion = conversion;
    }

    Object convert(@Nonnull Object val) {
      return conversion.apply(val);
    }

    @Override
    public String toString() {
      return from + " => " + to;
    }
  }

  @Override
  public String toString() {
    return castMap.entrySet().stream()
        .map(e -> e.getKey() + ": " + e.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
  }
}
