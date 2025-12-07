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
import org.apache.hudi.util.RowDataCastProjection;
import org.apache.hudi.util.RowDataProjection;
import org.apache.hudi.util.TypeConverters;
import org.apache.hudi.util.TypeConverters.TypeConverter;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

  @Getter
  @Setter
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

  @VisibleForTesting
  void add(int pos, LogicalType fromType, LogicalType toType) {
    TypeConverter converter = TypeConverters.getInstance(fromType, toType);
    if (converter == null) {
      throw new IllegalArgumentException(String.format("Cannot create cast %s => %s at pos %s", fromType, toType, pos));
    }
    add(pos, new Cast(fromType, toType, converter));
  }

  private void add(int pos, Cast cast) {
    castMap.put(pos, cast);
  }

  /**
   * Fields {@link Cast#from} and {@link Cast#to} are redundant due to {@link Cast#convert(Object)} determines conversion.
   * However, it is convenient to debug {@link CastMap} when {@link Cast#toString()} prints types.
   */
  private static final class Cast implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LogicalType from;
    private final LogicalType to;
    private final TypeConverter castMapConverter;

    Cast(LogicalType from, LogicalType to, TypeConverter conversion) {
      this.from = from;
      this.to = to;
      this.castMapConverter = conversion;
    }

    Object convert(Object val) {
      return castMapConverter.convert(val);
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
