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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;

/**
 * Implicit type converter for predicates push down.
 */
@Slf4j
public class ImplicitTypeConverter {

  /**
   * Convert the literal to the corresponding type.
   * @param literalType The type of the literal.
   * @param literal The literal value.
   * @return The converted literal.
   */
  public static Serializable convertImplicitly(LogicalType literalType, Serializable literal) {
    try {
      switch (literalType.getTypeRoot()) {
        case BOOLEAN:
          if (literal instanceof Boolean) {
            return literal;
          } else {
            return Boolean.valueOf(String.valueOf(literal));
          }
        case TINYINT:
        case SMALLINT:
        case INTEGER:
          if (literal instanceof Integer) {
            return literal;
          } else {
            return Integer.valueOf(String.valueOf(literal));
          }
        case BIGINT:
          if (literal instanceof Long) {
            return literal;
          } else if (literal instanceof Integer) {
            return new Long((Integer) literal);
          } else {
            return Long.valueOf(String.valueOf(literal));
          }
        case FLOAT:
          if (literal instanceof Float) {
            return literal;
          } else {
            return Float.valueOf(String.valueOf(literal));
          }
        case DOUBLE:
          if (literal instanceof Double) {
            return literal;
          } else {
            return Double.valueOf(String.valueOf(literal));
          }
        case BINARY:
        case VARBINARY:
          if (literal instanceof byte[]) {
            return literal;
          } else {
            return String.valueOf(literal).getBytes();
          }
        case DATE:
          if (literal instanceof LocalDate) {
            return (int) ((LocalDate) literal).toEpochDay();
          } else if (literal instanceof Integer) {
            return literal;
          } else if (literal instanceof Long) {
            return ((Long) literal).intValue();
          } else {
            return (int) LocalDate.parse(String.valueOf(literal)).toEpochDay();
          }
        case CHAR:
        case VARCHAR:
          if (literal instanceof String) {
            return literal;
          } else {
            return String.valueOf(literal);
          }
        case TIME_WITHOUT_TIME_ZONE:
          if (literal instanceof LocalTime) {
            return ((LocalTime) literal).get(ChronoField.MILLI_OF_DAY);
          } else if (literal instanceof Integer) {
            return literal;
          } else if (literal instanceof Long) {
            return ((Long) literal).intValue();
          } else {
            return LocalTime.parse(String.valueOf(literal)).get(ChronoField.MILLI_OF_DAY);
          }
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          if (literal instanceof LocalDateTime) {
            return ((LocalDateTime) literal).toInstant(ZoneOffset.UTC).toEpochMilli();
          } else if (literal instanceof Long) {
            return literal;
          } else if (literal instanceof Integer) {
            return new Long((Integer) literal);
          } else {
            return LocalDateTime.parse(String.valueOf(literal)).toInstant(ZoneOffset.UTC).toEpochMilli();
          }
        default:
          return literal;
      }
    } catch (RuntimeException e) {
      log.warn("Failed to convert literal [{}] to type [{}]. Will use its original type", literal, literalType);
      return literal;
    }
  }
}
