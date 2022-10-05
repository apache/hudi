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

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collections;

/**
 * Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class SimpleKeyGenerator extends BuiltinKeyGenerator {

  private final SimpleAvroKeyGenerator simpleAvroKeyGenerator;

  public SimpleKeyGenerator(TypedProperties props) {
    this(props, props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()),
        props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
  }

  SimpleKeyGenerator(TypedProperties props, String partitionPathField) {
    this(props, null, partitionPathField);
  }

  SimpleKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField) {
    super(props);
    // Make sure key-generator is configured properly
    ValidationUtils.checkArgument(recordKeyField == null || !recordKeyField.isEmpty(),
        "Record key field has to be non-empty!");
    ValidationUtils.checkArgument(partitionPathField == null || !partitionPathField.isEmpty(),
        "Partition path field has to be non-empty!");

    this.recordKeyFields = recordKeyField == null ? Collections.emptyList() : Collections.singletonList(recordKeyField);
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
}
