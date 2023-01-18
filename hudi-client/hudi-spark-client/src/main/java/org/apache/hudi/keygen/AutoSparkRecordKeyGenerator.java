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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.stream.Collectors;

public class AutoSparkRecordKeyGenerator extends BuiltinKeyGenerator {

  private final AutoRecordKeyGenerator avroRecordKeyGenerator;

  public AutoSparkRecordKeyGenerator(TypedProperties config) {
    super(config);
    this.partitionPathFields = Arrays.stream(config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()).split(FIELDS_SEP))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
    this.avroRecordKeyGenerator = new AutoRecordKeyGenerator(config, partitionPathFields);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    throw new UnsupportedOperationException("Get partition path operation is not supported by " + AutoRecordKeyGenerator.class.getSimpleName());
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return avroRecordKeyGenerator.getRecordKey(record);
  }
}
