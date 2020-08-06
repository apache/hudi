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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Key generator for deletes using global indices. Global index deletes do not require partition value
 * so this key generator avoids using partition value for generating HoodieKey.
 */
public class GlobalDeleteKeyGenerator extends BuiltinKeyGenerator {

  private static final String EMPTY_PARTITION = "";

  protected final List<String> recordKeyFields;

  public GlobalDeleteKeyGenerator(TypedProperties config) {
    super(config);
    this.recordKeyFields = Arrays.stream(config.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()).split(",")).map(String::trim).collect(Collectors.toList());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return KeyGenUtils.getRecordKey(record, recordKeyFields);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return EMPTY_PARTITION;
  }

  @Override
  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  @Override
  public List<String> getPartitionPathFields() {
    return new ArrayList<>();
  }
}