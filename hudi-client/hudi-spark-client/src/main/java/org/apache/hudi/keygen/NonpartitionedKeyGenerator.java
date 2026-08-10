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

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Simple Key generator for non-partitioned Hive Tables.
 */
public class NonpartitionedKeyGenerator extends BuiltinKeyGenerator {

  private final NonpartitionedAvroKeyGenerator nonpartitionedAvroKeyGenerator;

  public NonpartitionedKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = KeyGenUtils.getRecordKeyFields(props);
    this.partitionPathFields = Collections.emptyList();
    this.nonpartitionedAvroKeyGenerator = new NonpartitionedAvroKeyGenerator(props);
  }

  @Override
  public List<String> getPartitionPathFields() {
    return nonpartitionedAvroKeyGenerator.getPartitionPathFields();
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return nonpartitionedAvroKeyGenerator.getRecordKey(record);
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
  public String getPartitionPath(GenericRecord record) {
    return nonpartitionedAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getPartitionPath(Row row) {
    return nonpartitionedAvroKeyGenerator.getEmptyPartition();
  }

  @Override
  public UTF8String getPartitionPath(InternalRow row, StructType schema) {
    return UTF8String.EMPTY_UTF8;
  }
}

