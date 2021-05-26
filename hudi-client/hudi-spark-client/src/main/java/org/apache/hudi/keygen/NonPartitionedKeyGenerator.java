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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple Key generator for unPartitioned Hive Tables.
 */
public class NonPartitionedKeyGenerator extends BuiltinKeyGenerator {

  private final NonPartitionedAvroKeyGenerator nonPartitionedAvroKeyGenerator;

  public NonPartitionedKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.stream(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY)
        .split(",")).map(String::trim).collect(Collectors.toList());
    this.partitionPathFields = Collections.emptyList();
    nonPartitionedAvroKeyGenerator = new NonPartitionedAvroKeyGenerator(props);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return nonPartitionedAvroKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return nonPartitionedAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public List<String> getPartitionPathFields() {
    return nonPartitionedAvroKeyGenerator.getPartitionPathFields();
  }

  @Override
  public String getPartitionPath(Row row) {
    return nonPartitionedAvroKeyGenerator.getEmptyPartition();
  }

}

