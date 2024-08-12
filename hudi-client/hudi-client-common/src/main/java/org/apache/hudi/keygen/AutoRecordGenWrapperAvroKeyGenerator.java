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
public class AutoRecordGenWrapperAvroKeyGenerator extends BaseKeyGenerator implements AutoRecordKeyGeneratorWrapper {

  private final BaseKeyGenerator keyGenerator;
  private Integer partitionId;
  private String instantTime;
  private int rowId;

  public AutoRecordGenWrapperAvroKeyGenerator(TypedProperties config, BaseKeyGenerator keyGenerator) {
    super(config);
    this.keyGenerator = keyGenerator;
    this.rowId = 0;
    partitionId = null;
    instantTime = null;
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return generateSequenceId(rowId++);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return keyGenerator.getPartitionPath(record);
  }

  @Override
  public List<String> getRecordKeyFieldNames() {
    return keyGenerator.getRecordKeyFieldNames();
  }

  public List<String> getPartitionPathFields() {
    return keyGenerator.getPartitionPathFields();
  }

  public boolean isConsistentLogicalTimestampEnabled() {
    return keyGenerator.isConsistentLogicalTimestampEnabled();
  }

  @Override
  public BaseKeyGenerator getPartitionKeyGenerator() {
    return keyGenerator;
  }

  private String generateSequenceId(long recordIndex) {
    if (partitionId == null) {
      this.partitionId = config.getInteger(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG);
    }
    if (instantTime == null) {
      this.instantTime = config.getString(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG);
    }
    return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
  }
}
