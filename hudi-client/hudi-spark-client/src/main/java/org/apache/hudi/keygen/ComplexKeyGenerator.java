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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Key generator prefixing field names before corresponding record-key parts.
 *
 * <p/>
 * For example, for the schema of {@code { "key": string, "value": bytes }}, and corresponding record
 * {@code { "key": "foo" }}, record-key "key:foo" will be produced.
 */
public class ComplexKeyGenerator extends BuiltinKeyGenerator {

  private final ComplexAvroKeyGenerator complexAvroKeyGenerator;

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = KeyGenUtils.getRecordKeyFields(props);
    this.partitionPathFields = Arrays.stream(props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()).split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    this.complexAvroKeyGenerator = new ComplexAvroKeyGenerator(props);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return complexAvroKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return complexAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    if (recordKeyFields.isEmpty()) {
      return emptyKey();
    }
    tryInitRowAccessor(row.schema());
    return combineCompositeRecordKey(rowAccessor.getRecordKeyParts(row));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    if (recordKeyFields.isEmpty()) {
      return emptyKeyForInternalRow();
    }
    tryInitRowAccessor(schema);
    return combineCompositeRecordKeyUnsafe(rowAccessor.getRecordKeyParts(internalRow));
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
