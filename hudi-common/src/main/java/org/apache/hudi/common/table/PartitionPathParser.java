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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.VisibleForTesting;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PartitionPathParser {
  public static final String DEPRECATED_DEFAULT_PARTITION_PATH = "default";
  public static final String DEFAULT_PARTITION_PATH = "__HIVE_DEFAULT_PARTITION__";
  private static final String EQUALS_SIGN = "=";
  private static final String DASH = "-";
  private static final String SLASH = "/";

  public Object[] getPartitionFieldVals(Option<String[]> partitionFields,
                                        String partitionPath,
                                        HoodieSchema writerSchema) {
    return getPartitionFieldVals(partitionFields, partitionPath, writerSchema, false);
  }

  /**
   * @param slashSeparatedDatePartitioning whether the table was written with
   *        {@code hoodie.datasource.write.slash.separated.date.partitioning}, in which case a
   *        partition value spans several path segments rather than one.
   */
  public Object[] getPartitionFieldVals(Option<String[]> partitionFields,
                                        String partitionPath,
                                        HoodieSchema writerSchema,
                                        boolean slashSeparatedDatePartitioning) {
    if (!partitionFields.isPresent()) {
      return new Object[0];
    }
    return getPartitionValues(partitionFields.get(), partitionPath, writerSchema, slashSeparatedDatePartitioning);
  }

  private static Object[] getPartitionValues(String[] partitionFields,
                                             String partitionPath,
                                             HoodieSchema schema,
                                             boolean slashSeparatedDatePartitioning) {
    String[] parts = partitionPath.split("/");
    int pathSegment = 0;
    boolean hasDateField = false;
    // NOTE: The writer only slash-separates a table partitioned by a single column -- see the guard
    //       in [[KeyGenUtils#getRecordPartitionPath]] -- so that is the only shape whose value is
    //       known to span several segments here. Multi-field slash partitioning produces a layout
    //       that cannot be lined up with the partition columns at all; that is tracked in HUDI
    //       issue #19666 and deliberately left alone
    boolean valueSpansSegments = slashSeparatedDatePartitioning && partitionFields.length == 1;
    Object[] partitionValues = new Object[partitionFields.length];
    for (int i = 0; i < partitionFields.length; i++) {
      String partitionField = partitionFields[i];
      Option<HoodieSchemaField> field = schema.getField(partitionField);
      // if the field is not present in the schema, we assume it is a string
      HoodieSchema fieldSchema = field.map(f -> f.schema().getNonNullType()).orElseGet(() -> HoodieSchema.create(HoodieSchemaType.STRING));
      if (isTimeBasedType(fieldSchema.getType())) {
        if (hasDateField) {
          throw new IllegalArgumentException("Only one date field based partition is supported");
        }
        hasDateField = true;
        int numDateDirs = parts.length - partitionFields.length + 1;
        partitionValues[i] = inferDateValue(partitionPath, parts, pathSegment, numDateDirs, fieldSchema);
        pathSegment += numDateDirs;
      } else {
        // A slash-separated value occupies every segment this field is entitled to, mirroring the
        // way [[#inferDateValue]] consumes them for a time-based column
        int numDirs = valueSpansSegments ? parts.length - partitionFields.length + 1 : 1;
        String segment = joinSegmentsWithDash(parts, pathSegment, numDirs);
        String[] segmentParts = segment.split(EQUALS_SIGN);
        partitionValues[i] = parseValue(segmentParts[segmentParts.length - 1], fieldSchema);
        pathSegment += numDirs;
      }
    }
    return partitionValues;
  }

  /**
   * Undoes the {@code -} -> {@code /} substitution the writer performs for
   * {@code hoodie.datasource.write.slash.separated.date.partitioning}, rejoining the {@code numDirs}
   * path segments starting at {@code pathSegment} back into the single value they were written from.
   * For a value that was not slash-separated {@code numDirs} is 1 and the segment is returned as-is.
   */
  private static String joinSegmentsWithDash(String[] parts, int pathSegment, int numDirs) {
    if (numDirs == 1) {
      return parts[pathSegment];
    }
    StringBuilder value = new StringBuilder(parts[pathSegment]);
    for (int i = 1; i < numDirs; i++) {
      value.append(DASH).append(parts[pathSegment + i]);
    }
    return value.toString();
  }

  @VisibleForTesting
  static Object parseValue(String partitionValue, HoodieSchema fieldSchema) {
    if (partitionValue.equals(DEFAULT_PARTITION_PATH) || partitionValue.equals(DEPRECATED_DEFAULT_PARTITION_PATH)) {
      return null;
    }

    switch (fieldSchema.getType()) {
      case STRING:
      case ENUM:
      case UUID:
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
        return partitionValue.getBytes(StandardCharsets.UTF_8);
      case DECIMAL:
        return new BigDecimal(partitionValue);
      default:
        throw new IllegalArgumentException("Unexpected type " + fieldSchema.getType());
    }
  }

  private static Object inferDateValue(
      String partitionPath,
      String[] parts,
      int pathSegment,
      int numDateDirs,
      HoodieSchema fieldSchema) {
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
    if (fieldSchema.getType() == HoodieSchemaType.DATE) {
      return Date.valueOf(time.toLocalDate());
    }
    return Timestamp.from(time.toInstant(ZoneOffset.UTC));
  }

  private static boolean isTimeBasedType(HoodieSchemaType type) {
    return type == HoodieSchemaType.DATE || type == HoodieSchemaType.TIMESTAMP || type == HoodieSchemaType.TIME;
  }
}
