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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.hudi.keygen.KeyGenUtils.HUDI_DEFAULT_PARTITION_PATH;

/**
 * Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedKeyGenerator extends SimpleKeyGenerator {

  private final TimestampBasedAvroKeyGenerator timestampBasedAvroKeyGenerator;

  public TimestampBasedKeyGenerator(TypedProperties config) throws IOException {
    this(config, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), null),
        config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()));
  }

  TimestampBasedKeyGenerator(TypedProperties config, String partitionPathField) throws IOException {
    this(config, null, partitionPathField);
  }

  TimestampBasedKeyGenerator(TypedProperties config, String recordKeyField, String partitionPathField) throws IOException {
    super(config, Option.ofNullable(recordKeyField), partitionPathField);
    timestampBasedAvroKeyGenerator = new TimestampBasedAvroKeyGenerator(config, recordKeyField, partitionPathField);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return timestampBasedAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    tryInitRowAccessor(row.schema());
    return combineRecordKey(getRecordKeyFieldNames(), Arrays.asList(rowAccessor.getRecordKeyParts(row)));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    tryInitRowAccessor(schema);
    return combineRecordKeyUnsafe(getRecordKeyFieldNames(), Arrays.asList(rowAccessor.getRecordKeyParts(internalRow)));
  }

  @Override
  public String getPartitionPath(Row row) {
    tryInitRowAccessor(row.schema());
    Object[] partitionPathValues = rowAccessor.getRecordPartitionPathValues(row);
    return getFormattedPartitionPath(partitionPathValues[0]);
  }

  @Override
  public UTF8String getPartitionPath(InternalRow row, StructType schema) {
    tryInitRowAccessor(schema);
    Object[] partitionPathValues = rowAccessor.getRecordPartitionPathValues(row);
    return UTF8String.fromString(getFormattedPartitionPath(partitionPathValues[0]));
  }

  private String getFormattedPartitionPath(Object partitionPathPart) {
    Object fieldVal;
    if (partitionPathPart == null || Objects.equals(partitionPathPart, HUDI_DEFAULT_PARTITION_PATH)) {
      fieldVal = timestampBasedAvroKeyGenerator.getDefaultPartitionVal();
    } else if (partitionPathPart instanceof UTF8String) {
      fieldVal = partitionPathPart.toString();
    } else {
      fieldVal = partitionPathPart;
    }

    try {
      return timestampBasedAvroKeyGenerator.getPartitionPath(fieldVal);
    } catch (Exception e) {
      throw new HoodieKeyGeneratorException(String.format("Failed to properly format partition-path (%s)", fieldVal), e);
    }
  }
}