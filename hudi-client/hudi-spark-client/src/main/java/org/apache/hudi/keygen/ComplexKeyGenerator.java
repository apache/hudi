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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.UTF8StringBuilder;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_RECORD_KEY_PARTS_SEPARATOR;

/**
 * Key generator prefixing field names before corresponding record-key parts.
 *
 * <p/>
 * For example, for the schema of {@code { "key": string, "value": bytes }}, and corresponding record
 * {@code { "key": "foo" }}, record-key "key:foo" will be produced.
 */
public class ComplexKeyGenerator extends BuiltinKeyGenerator {

  private static final String COMPOSITE_KEY_FIELD_VALUE_INFIX = ":";

  private final ComplexAvroKeyGenerator complexAvroKeyGenerator;

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.stream(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()).split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    this.partitionPathFields = Arrays.stream(props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()).split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    complexAvroKeyGenerator = new ComplexAvroKeyGenerator(props);
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
    tryInitRowAccessor(row.schema());
    return combineCompositeRecordKey(rowAccessor.getRecordKeyParts(row));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    tryInitRowAccessor(schema);
    return combineCompositeRecordKeyUnsafe(rowAccessor.getRecordKeyParts(internalRow));
  }

  @Override
  public String getRecordKey(InternalRow internalRow, StructType schema) {
    buildFieldSchemaInfoIfNeeded(schema);
    return RowKeyGeneratorHelper.getRecordKeyFromInternalRow(internalRow, getRecordKeyFields(), recordKeySchemaInfo, true);
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

  private String combineCompositeRecordKey(Object... recordKeyParts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      sb.append(recordKeyFields.get(i));
      sb.append(COMPOSITE_KEY_FIELD_VALUE_INFIX);
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      sb.append(requireNonNullNonEmptyKey(recordKeyParts[i].toString()));

      if (i < recordKeyParts.length - 1) {
        sb.append(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.toString();
  }

  private UTF8String combineCompositeRecordKeyUnsafe(Object... recordKeyParts) {
    UTF8StringBuilder sb = new UTF8StringBuilder();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      sb.append(recordKeyFields.get(i));
      sb.append(COMPOSITE_KEY_FIELD_VALUE_INFIX);
      sb.append(requireNonNullNonEmptyKey(toUTF8String(recordKeyParts[i])));

      if (i < recordKeyParts.length - 1) {
        sb.append(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.build();
  }
}
