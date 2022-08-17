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

import org.apache.hudi.common.model.HoodieKey;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Avro key generator for empty record key Hudi tables.
 */
public class EmptyAvroKeyGenerator extends BaseKeyGenerator {

  private static final Logger LOG = LogManager.getLogger(EmptyAvroKeyGenerator.class);
  public static final String EMPTY_RECORD_KEY = HoodieKey.EMPTY_RECORD_KEY;
  private static final List<String> EMPTY_RECORD_KEY_FIELD_LIST = Collections.emptyList();

  public EmptyAvroKeyGenerator(TypedProperties props) {
    super(props);
    if (config.containsKey(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key())) {
      LOG.warn(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key() + " will be ignored while using "
          + this.getClass().getSimpleName());
    }
    this.recordKeyFields = EMPTY_RECORD_KEY_FIELD_LIST;
    this.partitionPathFields = Arrays.stream(props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
        .split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return EMPTY_RECORD_KEY;
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return KeyGenUtils.getRecordPartitionPath(record, getPartitionPathFields(), hiveStylePartitioning, encodePartitionPath, isConsistentLogicalTimestampEnabled());
  }
}
