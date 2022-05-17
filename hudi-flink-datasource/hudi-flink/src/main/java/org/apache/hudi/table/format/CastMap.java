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

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.util.AvroSchemaConverter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/**
 * CastMap is responsible for type conversion when full schema evolution enabled.
 */
public final class CastMap implements Serializable {
  // Maps position (column number) to corresponding cast
  private final Map<Integer, Cast> castMap = new HashMap<>();

  /**
   * Creates CastMap by comparing two schemes. Cast of a specific column is created if its type has changed.
   */
  public static CastMap of(String tableName, InternalSchema querySchema, InternalSchema actualSchema) {
    DataType queryType = internalSchemaToDataType(tableName, querySchema);
    DataType actualType = internalSchemaToDataType(tableName, actualSchema);
    CastMap castMap = new CastMap();
    InternalSchemaUtils.collectTypeChangedCols(querySchema, actualSchema).entrySet()
            .stream()
            .filter(e -> !isSameType(e.getValue().getLeft(), e.getValue().getRight()))
            .forEach(e -> {
              int pos = e.getKey();
              LogicalType target = queryType.getChildren().get(pos).getLogicalType();
              LogicalType actual = actualType.getChildren().get(pos).getLogicalType();
              castMap.add(pos, actual, target);
            });
    return castMap;
  }

  public Object castIfNeed(int pos, Object val) {
    Cast cast = castMap.get(pos);
    if (cast == null) {
      return val;
    }
    return cast(val, cast.from(), cast.to());
  }

  private Object cast(Object val, LogicalType fromType, LogicalType toType) {
    LogicalTypeRoot from = fromType.getTypeRoot();
    LogicalTypeRoot to = toType.getTypeRoot();
    switch (to) {
      case BIGINT: {
        // Integer => Long
        if (from == INTEGER) {
          return ((Number) val).longValue();
        }
        break;
      }
      case FLOAT: {
        // Integer => Float
        // Long => Float
        if (from == INTEGER || from == BIGINT) {
          return ((Number) val).floatValue();
        }
        break;
      }
      case DOUBLE: {
        // Integer => Double
        // Long => Double
        if (from == INTEGER || from == BIGINT) {
          return ((Number) val).doubleValue();
        }
        // Float => Double
        if (from == FLOAT) {
          return Double.parseDouble(val.toString());
        }
        break;
      }
      case DECIMAL: {
        // Integer => Decimal
        // Long => Decimal
        // Double => Decimal
        if (from == INTEGER || from == BIGINT || from == DOUBLE) {
          return toDecimalData((Number) val, toType);
        }
        // Float => Decimal
        if (from == FLOAT) {
          return toDecimalData(Double.parseDouble(val.toString()), toType);
        }
        // String => Decimal
        if (from == VARCHAR) {
          return toDecimalData(Double.parseDouble(val.toString()), toType);
        }
        // Decimal => Decimal
        if (from == DECIMAL) {
          return toDecimalData(((DecimalData) val).toBigDecimal(), toType);
        }
        break;
      }
      case VARCHAR: {
        // Integer => String
        // Long => String
        // Float => String
        // Double => String
        // Decimal => String
        if (from == INTEGER
            || from == BIGINT
            || from == FLOAT
            || from == DOUBLE
            || from == DECIMAL) {
          return new BinaryStringData(String.valueOf(val));
        }
        // Date => String
        if (from == DATE) {
          return new BinaryStringData(LocalDate.ofEpochDay(((Integer) val).longValue()).toString());
        }
        break;
      }
      case DATE: {
        // String => Date
        if (from == VARCHAR) {
          return (int) LocalDate.parse(val.toString()).toEpochDay();
        }
        break;
      }
      default:
    }
    return val;
  }

  public boolean containsAnyPos(Collection<Integer> positions) {
    return positions.stream().anyMatch(castMap.keySet()::contains);
  }

  public CastMap rearrange(List<Integer> oldIndexes, List<Integer> newIndexes) {
    Preconditions.checkArgument(oldIndexes.size() == newIndexes.size());
    CastMap newCastMap = new CastMap();
    for (int i = 0; i < oldIndexes.size(); i++) {
      Cast cast = castMap.get(oldIndexes.get(i));
      if (cast != null) {
        newCastMap.add(newIndexes.get(i), cast.from(), cast.to());
      }
    }
    return newCastMap;
  }

  @VisibleForTesting
  void add(int pos, LogicalType from, LogicalType to) {
    castMap.put(pos, new Cast(from, to));
  }

  private DecimalData toDecimalData(Number val, LogicalType decimalType) {
    BigDecimal valAsDecimal = BigDecimal.valueOf(val.doubleValue());
    return toDecimalData(valAsDecimal, decimalType);
  }

  private DecimalData toDecimalData(BigDecimal valAsDecimal, LogicalType decimalType) {
    return DecimalData.fromBigDecimal(
            valAsDecimal,
            ((DecimalType) decimalType).getPrecision(),
            ((DecimalType) decimalType).getScale());
  }

  private static boolean isSameType(Type left, Type right) {
    if (left instanceof Types.DecimalType && right instanceof Types.DecimalType) {
      return left.equals(right);
    }
    return left.typeId().equals(right.typeId());
  }

  private static DataType internalSchemaToDataType(String tableName, InternalSchema internalSchema) {
    Schema schema = AvroInternalSchemaConverter.convert(internalSchema, tableName);
    return AvroSchemaConverter.convertToDataType(schema);
  }

  private static final class Cast implements Serializable {
    private final LogicalType from;
    private final LogicalType to;

    Cast(LogicalType from, LogicalType to) {
      this.from = from;
      this.to = to;
    }

    LogicalType from() {
      return from;
    }

    LogicalType to() {
      return to;
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
