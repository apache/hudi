/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.hudi.avro.AvroSchemaUtils.getNonNullTypeFromUnion;

public class PartitionPathParser {
  public static final String DEPRECATED_DEFAULT_PARTITION_PATH = "default";
  public static final String DEFAULT_PARTITION_PATH = "__HIVE_DEFAULT_PARTITION__";
  private static final String EQUALS_SIGN = "=";
  private static final String DASH = "-";
  private static final String SLASH = "/";

  public Object[] getPartitionFieldVals(Option<String[]> partitionFields,
                                        String partitionPath,
                                        Schema writerSchema) {
    if (!partitionFields.isPresent()) {
      return new Object[0];
    }
    return getPartitionValues(partitionFields.get(), partitionPath, writerSchema);
  }

  private static Object[] getPartitionValues(String[] partitionFields,
                                             String partitionPath,
                                             Schema schema) {
    String[] parts = partitionPath.split("/");
    int pathSegment = 0;
    boolean hasDateField = false;
    Object[] partitionValues = new Object[partitionFields.length];
    for (int i = 0; i < partitionFields.length; i++) {
      String partitionField = partitionFields[i];
      Schema.Field field = schema.getField(partitionField);
      // if the field is not present in the schema, we assume it is a string
      Schema fieldSchema = field == null ? Schema.create(Schema.Type.STRING) : getNonNullTypeFromUnion(field.schema());
      LogicalType logicalType = fieldSchema.getLogicalType();
      if (isTimeBasedLogicalType(logicalType)) {
        if (hasDateField) {
          throw new IllegalArgumentException("Only one date field based partition is supported");
        }
        hasDateField = true;
        int numDateDirs = parts.length - partitionFields.length + 1;
        partitionValues[i] = inferDateValue(partitionPath, parts, pathSegment, numDateDirs, fieldSchema);
        pathSegment += numDateDirs;
      } else {
        String segment = parts[pathSegment];
        String[] segmentParts = segment.split(EQUALS_SIGN);
        partitionValues[i] = parseValue(segmentParts[segmentParts.length - 1], fieldSchema);
        pathSegment++;
      }
    }
    return partitionValues;
  }

  @VisibleForTesting
  static Object parseValue(String partitionValue, Schema fieldSchema) {
    if (partitionValue.equals(DEFAULT_PARTITION_PATH) || partitionValue.equals(DEPRECATED_DEFAULT_PARTITION_PATH)) {
      return null;
    }

    switch (fieldSchema.getType()) {
      case STRING:
        return PartitionPathEncodeUtils.unescapePathName(partitionValue);
      case INT:
        return Integer.parseInt(partitionValue);
      case LONG:
        return Long.parseLong(partitionValue);
      case FLOAT:
        return Float.parseFloat(partitionValue);
      case DOUBLE:
        return Double.parseDouble(partitionValue);
      case BOOLEAN:
        return Boolean.parseBoolean(partitionValue);
      case BYTES:
      case FIXED:
        if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
          return new java.math.BigDecimal(partitionValue);
        } else {
          return partitionValue.getBytes(StandardCharsets.UTF_8);
        }
      default:
        throw new IllegalArgumentException("Unexpected type " + fieldSchema.getType());
    }
  }

  private static Object inferDateValue(
      String partitionPath,
      String[] parts,
      int pathSegment,
      int numDateDirs,
      Schema fieldSchema) {
    StringBuilder condensedPartitionValue = new StringBuilder();
    for (int i = 0; i < numDateDirs; i++) {
      String partitionValue = parts[pathSegment + i];
      // remove the field name if it is present due to hive-style partitioning
      if (partitionValue.contains(EQUALS_SIGN)) {
        partitionValue = partitionValue.split(EQUALS_SIGN)[1];
      }
      if (partitionValue.contains(DASH)) {
        partitionValue = partitionValue.replace(DASH, "");
      }
      condensedPartitionValue.append(partitionValue.replace(SLASH, ""));
    }
    LocalDateTime time;
    switch (condensedPartitionValue.length()) {
      case 4: // Year
        time = LocalDateTime.of(Integer.parseInt(condensedPartitionValue.substring(0, 4)), 1, 1, 0, 0);
        break;
      case 6: // Month
        time = LocalDateTime.of(
            Integer.parseInt(condensedPartitionValue.substring(0, 4)),
            Integer.parseInt(condensedPartitionValue.substring(4, 6)), 1, 0, 0);
        break;
      case 8: // Day
        time = LocalDateTime.of(
            Integer.parseInt(condensedPartitionValue.substring(0, 4)),
            Integer.parseInt(condensedPartitionValue.substring(4, 6)),
            Integer.parseInt(condensedPartitionValue.substring(6, 8)), 0, 0);
        break;
      case 10: // Hour
        time = LocalDateTime.of(
            Integer.parseInt(condensedPartitionValue.substring(0, 4)),
            Integer.parseInt(condensedPartitionValue.substring(4, 6)),
            Integer.parseInt(condensedPartitionValue.substring(6, 8)),
            Integer.parseInt(condensedPartitionValue.substring(8, 10)), 0);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown date format for partition path: " + partitionPath);
    }
    if (fieldSchema.getLogicalType() instanceof LogicalTypes.Date) {
      return Date.valueOf(time.toLocalDate());
    }
    return Timestamp.from(time.toInstant(ZoneOffset.UTC));
  }

  private static boolean isTimeBasedLogicalType(LogicalType logicalType) {
    return logicalType instanceof LogicalTypes.Date
        || logicalType instanceof LogicalTypes.TimestampMillis
        || logicalType instanceof LogicalTypes.TimestampMicros
        || logicalType instanceof LogicalTypes.TimeMillis
        || logicalType instanceof LogicalTypes.TimeMicros
        || logicalType instanceof LogicalTypes.LocalTimestampMicros
        || logicalType instanceof LogicalTypes.LocalTimestampMillis;
  }
}
