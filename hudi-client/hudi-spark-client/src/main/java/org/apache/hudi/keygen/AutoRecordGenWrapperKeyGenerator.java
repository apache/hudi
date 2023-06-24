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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

/**
 * A wrapper key generator to intercept getRecordKey calls for auto record key generator.
 * <ol>
 *   <li>Generated keys will be unique not only w/in provided [[org.apache.spark.sql.DataFrame]], but
 *   globally unique w/in the target table</li>
 *   <li>Generated keys have minimal overhead (to compute, persist and read)</li>
 * </ol>
 *
 * Keys adhere to the following format:
 *
 * [instantTime]_[PartitionId]_[RowId]
 *
 * where
 * instantTime refers to the commit time of the batch being ingested.
 * PartitionId refers to spark's partition Id.
 * RowId refers to the row index within the spark partition.
 */
public class AutoRecordGenWrapperKeyGenerator extends BuiltinKeyGenerator {

  private final BuiltinKeyGenerator builtinKeyGenerator;
  private final int partitionId;
  private final String instantTime;
  private int rowId;

  public AutoRecordGenWrapperKeyGenerator(TypedProperties config, BuiltinKeyGenerator builtinKeyGenerator) {
    super(config);
    this.builtinKeyGenerator = builtinKeyGenerator;
    this.rowId = 0;
    this.partitionId = config.getInteger(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG);
    this.instantTime = config.getString(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return HoodieRecord.generateSequenceId(instantTime, partitionId, rowId++);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return builtinKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    return HoodieRecord.generateSequenceId(instantTime, partitionId, rowId++);
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    return UTF8String.fromString(HoodieRecord.generateSequenceId(instantTime, partitionId, rowId++));
  }

  @Override
  public String getPartitionPath(Row row) {
    return builtinKeyGenerator.getPartitionPath(row);
  }

  @Override
  public UTF8String getPartitionPath(InternalRow internalRow, StructType schema) {
    return builtinKeyGenerator.getPartitionPath(internalRow, schema);
  }

  @Override
  public List<String> getRecordKeyFieldNames() {
    return builtinKeyGenerator.getRecordKeyFieldNames();
  }

  public List<String> getPartitionPathFields() {
    return builtinKeyGenerator.getPartitionPathFields();
  }

  public boolean isConsistentLogicalTimestampEnabled() {
    return builtinKeyGenerator.isConsistentLogicalTimestampEnabled();
  }

}
