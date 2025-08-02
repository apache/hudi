/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * This class is used for test purpose, and should never be used
 * in other ways.
 */
public class MockUserProvidedKeyGenerator extends BuiltinKeyGenerator {

  private final SimpleAvroKeyGenerator simpleAvroKeyGenerator;

  public MockUserProvidedKeyGenerator(TypedProperties props) {
    this(props, Option.ofNullable(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), null)),
        props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
  }

  public MockUserProvidedKeyGenerator(TypedProperties props, String partitionPathField) {
    this(props, Option.empty(), partitionPathField);
  }

  public MockUserProvidedKeyGenerator(TypedProperties props, Option<String> recordKeyField, String partitionPathField) {
    super(props);
    // Make sure key-generator is configured properly
    validateRecordKey(recordKeyField);
    validatePartitionPath(partitionPathField);

    this.recordKeyFields = !recordKeyField.isPresent() ? Collections.emptyList() : Collections.singletonList(recordKeyField.get());
    this.partitionPathFields = partitionPathField == null ? Collections.emptyList() : Collections.singletonList(partitionPathField);
    this.simpleAvroKeyGenerator = new SimpleAvroKeyGenerator(props, recordKeyField, partitionPathField);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return simpleAvroKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return simpleAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    tryInitRowAccessor(row.schema());

    Object[] recordKeys = rowAccessor.getRecordKeyParts(row);
    // NOTE: [[SimpleKeyGenerator]] is restricted to allow only primitive (non-composite)
    //       record-key field
    if (recordKeys[0] == null) {
      return handleNullRecordKey(null);
    } else {
      return requireNonNullNonEmptyKey(recordKeys[0].toString());
    }
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    tryInitRowAccessor(schema);

    Object[] recordKeyValues = rowAccessor.getRecordKeyParts(internalRow);
    // NOTE: [[SimpleKeyGenerator]] is restricted to allow only primitive (non-composite)
    //       record-key field
    if (recordKeyValues[0] == null) {
      return handleNullRecordKey(null);
    } else if (recordKeyValues[0] instanceof UTF8String) {
      return requireNonNullNonEmptyKey((UTF8String) recordKeyValues[0]);
    } else {
      return requireNonNullNonEmptyKey(UTF8String.fromString(recordKeyValues[0].toString()));
    }
  }

  @Override
  public String getPartitionPath(Row row) {
    tryInitRowAccessor(row.schema());
    return combinePartitionPath(rowAccessor.getRecordPartitionPathValues(row));
  }

  @Override
  public UTF8String getPartitionPath(InternalRow row, StructType schema) {
    tryInitRowAccessor(schema);
    return combinePartitionPathUnsafe(rowAccessor.getRecordPartitionPathValues(row));
  }

  private static void validatePartitionPath(String partitionPathField) {
    checkArgument(partitionPathField == null || !partitionPathField.isEmpty(),
        "Partition-path field has to be non-empty!");
    checkArgument(partitionPathField == null || !partitionPathField.contains(FIELDS_SEP),
        String.format("Single partition-path field is expected; provided (%s)", partitionPathField));
  }

  private void validateRecordKey(Option<String> recordKeyField) {
    checkArgument(!recordKeyField.isPresent() || !recordKeyField.get().isEmpty(),
        "Record key field has to be non-empty!");
    checkArgument(!recordKeyField.isPresent() || !recordKeyField.get().contains(FIELDS_SEP),
        String.format("Single record-key field is expected; provided (%s)", recordKeyField));
  }
}
