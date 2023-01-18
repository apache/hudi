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

package org.apache.hudi.keygen;

import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.apache.hudi.common.util.CollectionUtils.tail;

public class SparkRowAccessor {

  private static final Logger LOG = LogManager.getLogger(SparkRowAccessor.class);

  private final HoodieUnsafeRowUtils.NestedFieldPath[] recordKeyFieldsPaths;
  private final HoodieUnsafeRowUtils.NestedFieldPath[] partitionPathFieldsPaths;

  SparkRowAccessor(StructType schema, List<String> recordKeyFieldNames, List<String> partitionPathFieldNames) {
    this.recordKeyFieldsPaths = resolveNestedFieldPaths(recordKeyFieldNames, schema, false);
    // Sometimes, we need to extract the recordKey from the partition-dropped data
    // To be consistent with avro key generator
    // ParquetBootstrapMetadataHandler
    this.partitionPathFieldsPaths = resolveNestedFieldPaths(partitionPathFieldNames, schema, true);
  }

  public Object[] getRecordKeyParts(Row row) {
    return getNestedFieldValues(row, recordKeyFieldsPaths);
  }

  public Object[] getRecordPartitionPathValues(Row row) {
    if (partitionPathFieldsPaths == null) {
      throw new HoodieException("Failed to resolve nested partition field");
    }
    return getNestedFieldValues(row, partitionPathFieldsPaths);
  }

  public Object[] getRecordKeyParts(InternalRow row) {
    return getNestedFieldValues(row, recordKeyFieldsPaths);
  }

  public Object[] getRecordPartitionPathValues(InternalRow row) {
    if (partitionPathFieldsPaths == null) {
      throw new HoodieException("Failed to resolve nested partition field");
    }
    return getNestedFieldValues(row, partitionPathFieldsPaths);
  }

  private Object[] getNestedFieldValues(Row row, HoodieUnsafeRowUtils.NestedFieldPath[] nestedFieldsPaths) {
    Object[] nestedFieldValues = new Object[nestedFieldsPaths.length];
    for (int i = 0; i < nestedFieldsPaths.length; ++i) {
      nestedFieldValues[i] = HoodieUnsafeRowUtils$.MODULE$.getNestedRowValue(row, nestedFieldsPaths[i]);
    }
    return nestedFieldValues;
  }

  private Object[] getNestedFieldValues(InternalRow row, HoodieUnsafeRowUtils.NestedFieldPath[] nestedFieldsPaths) {
    Object[] nestedFieldValues = new Object[nestedFieldsPaths.length];
    for (int i = 0; i < nestedFieldsPaths.length; ++i) {
      Object rawValue = HoodieUnsafeRowUtils$.MODULE$.getNestedInternalRowValue(row, nestedFieldsPaths[i]);
      DataType dataType = tail(nestedFieldsPaths[i].parts())._2.dataType();

      nestedFieldValues[i] = convertToLogicalDataType(dataType, rawValue);
    }

    return nestedFieldValues;
  }

  private HoodieUnsafeRowUtils.NestedFieldPath[] resolveNestedFieldPaths(List<String> fieldPaths, StructType schema, boolean returnNull) {
    try {
      return fieldPaths.stream()
          .map(fieldPath -> HoodieUnsafeRowUtils$.MODULE$.composeNestedFieldPath(schema, fieldPath))
          .toArray(HoodieUnsafeRowUtils.NestedFieldPath[]::new);
    } catch (Exception e) {
      if (returnNull) {
        return null;
      }
      LOG.error(String.format("Failed to resolve nested field-paths (%s) in schema (%s)", fieldPaths, schema), e);
      throw new HoodieException("Failed to resolve nested field-paths", e);
    }
  }

  /**
   * Converts provided (raw) value extracted from the {@link InternalRow} object into a deserialized,
   * JVM native format (for ex, converting {@code Long} into {@link Instant},
   * {@code Integer} to {@link LocalDate}, etc)
   *
   * This method allows to avoid costly full-row deserialization sequence. Note, that this method
   * should be maintained in sync w/
   *
   * <ol>
   *   <li>{@code RowEncoder#deserializerFor}, as well as</li>
   *   <li>{@code HoodieAvroUtils#convertValueForAvroLogicalTypes}</li>
   * </ol>
   *
   * @param dataType target data-type of the given value
   * @param value target value to be converted
   */
  private static Object convertToLogicalDataType(DataType dataType, Object value) {
    if (value == null) {
      return null;
    } else if (dataType instanceof TimestampType) {
      // Provided value have to be [[Long]] in this case, representing micros since epoch
      return new Timestamp((Long) value / 1000);
    } else if (dataType instanceof DateType) {
      // Provided value have to be [[Int]] in this case
      return LocalDate.ofEpochDay((Integer) value);
    }

    return value;
  }
}