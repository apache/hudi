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

import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * Avro simple Key generator for unpartitioned Hive Tables.
 */
public class NonpartitionedAvroKeyGenerator extends BaseKeyGenerator {

  private static final String EMPTY_PARTITION = "";
  private static final List<String> EMPTY_PARTITION_FIELD_LIST = new ArrayList<>();

  public NonpartitionedAvroKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = KeyGenUtils.getRecordKeyFields(props);
    this.partitionPathFields = EMPTY_PARTITION_FIELD_LIST;
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return EMPTY_PARTITION;
  }

  @Override
  public List<String> getPartitionPathFields() {
    return EMPTY_PARTITION_FIELD_LIST;
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    if (getRecordKeyFieldNames().isEmpty()) {
      return EMPTY_STRING;
    }
    // for backward compatibility, we need to use the right format according to the number of record key fields
    // 1. if there is only one record key field, the format of record key is just "<value>"
    // 2. if there are multiple record key fields, the format is "<field1>:<value1>,<field2>:<value2>,..."
    if (getRecordKeyFieldNames().size() == 1) {
      return KeyGenUtils.getRecordKey(record, getRecordKeyFields().get(0), isConsistentLogicalTimestampEnabled());
    }
    return KeyGenUtils.getRecordKey(record, getRecordKeyFields(), isConsistentLogicalTimestampEnabled());
  }

  public String getEmptyPartition() {
    return EMPTY_PARTITION;
  }
}
