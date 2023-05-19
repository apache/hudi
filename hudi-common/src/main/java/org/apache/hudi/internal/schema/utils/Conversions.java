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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

public class Conversions {

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static final HashSet<Type.TypeID> SUPPORTED_PARTITION_TYPES = new HashSet<>(
      Arrays.asList(Type.TypeID.INT,
          Type.TypeID.LONG,
          Type.TypeID.BOOLEAN,
          Type.TypeID.FLOAT,
          Type.TypeID.DECIMAL,
          Type.TypeID.DOUBLE,
          Type.TypeID.UUID,
          Type.TypeID.DATE,
          Type.TypeID.STRING));

  public static boolean isSchemaSupportedConversion(Types.RecordType schema) {
    for (Types.Field field: schema.fields()) {
      if (!SUPPORTED_PARTITION_TYPES.contains(field.type().typeId())) {
        return false;
      }
    }
    return true;
  }

  public static Object fromPartitionString(String partitionValue, Type type) {
    switch (type.typeId()) {
      case INT:
        return Integer.parseInt(partitionValue);
      case LONG:
        return Long.parseLong(partitionValue);
      case BOOLEAN:
        return Boolean.parseBoolean(partitionValue);
      case FLOAT:
        return Float.parseFloat(partitionValue);
      case DECIMAL:
        return new BigDecimal(partitionValue);
      case DOUBLE:
        return Double.parseDouble(partitionValue);
      case UUID:
        return UUID.fromString(partitionValue);
      case DATE:
        // TODO Support zoneId and different date format
        return Math.toIntExact(ChronoUnit.DAYS.between(
            EPOCH_DAY, LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE)));
      case STRING:
        return partitionValue;
      default:
        throw new UnsupportedOperationException("Cast value " + partitionValue
            + " to type " + type + " is not supported yet");
    }
  }
}
