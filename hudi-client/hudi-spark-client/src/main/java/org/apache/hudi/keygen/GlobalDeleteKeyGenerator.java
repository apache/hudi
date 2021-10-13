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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Key generator for deletes using global indices. Global index deletes do not require partition value so this key generator
 * avoids using partition value for generating HoodieKey.
 */
public class GlobalDeleteKeyGenerator extends BuiltinKeyGenerator {

  private final GlobalAvroDeleteKeyGenerator globalAvroDeleteKeyGenerator;
  public GlobalDeleteKeyGenerator(TypedProperties config) {
    super(config);
    this.recordKeyFields = Arrays.asList(config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()).split(","));
    globalAvroDeleteKeyGenerator = new GlobalAvroDeleteKeyGenerator(config);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    return globalAvroDeleteKeyGenerator.getRecordKey(record);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return globalAvroDeleteKeyGenerator.getPartitionPath(record);
  }

  @Override
  public List<String> getPartitionPathFields() {
    return new ArrayList<>();
  }

  @Override
  public String getRecordKey(Row row) {
    buildFieldPositionMapIfNeeded(row.schema());
    return RowKeyGeneratorHelper.getRecordKeyFromRow(row, getRecordKeyFields(), recordKeyPositions, true);
  }

  @Override
  public String getPartitionPath(Row row) {
    return globalAvroDeleteKeyGenerator.getEmptyPartition();
  }

  @Override
  public String getPartitionPath(InternalRow row, StructType structType) {
    return globalAvroDeleteKeyGenerator.getEmptyPartition();
  }
}

