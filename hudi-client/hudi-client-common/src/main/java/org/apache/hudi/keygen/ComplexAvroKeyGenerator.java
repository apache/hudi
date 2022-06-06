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

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * Avro complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class ComplexAvroKeyGenerator extends BaseKeyGenerator {
  public static final String DEFAULT_RECORD_KEY_SEPARATOR = ":";

  public ComplexAvroKeyGenerator(TypedProperties props) {
    super(props);
    if (props.containsKey(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key())) {
      this.recordKeyFields = Arrays.stream(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()).split(","))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList());
    } else {
      this.recordKeyFields = Collections.emptyList();
    }
    this.partitionPathFields = Arrays.stream(props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()).split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    if (getRecordKeyFields().isEmpty()) {
      return EMPTY_STRING;
    }
    return KeyGenUtils.getRecordKey(record, getRecordKeyFields(), isConsistentLogicalTimestampEnabled());
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return KeyGenUtils.getRecordPartitionPath(record, getPartitionPathFields(), hiveStylePartitioning, encodePartitionPath, isConsistentLogicalTimestampEnabled());
  }
}
