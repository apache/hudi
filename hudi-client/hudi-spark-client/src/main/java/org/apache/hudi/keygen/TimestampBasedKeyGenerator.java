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
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Row;

import java.io.IOException;

import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.keygen.KeyGenUtils.EMPTY_RECORDKEY_PLACEHOLDER;
import static org.apache.hudi.keygen.KeyGenUtils.NULL_RECORDKEY_PLACEHOLDER;

/**
 * Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedKeyGenerator extends SimpleKeyGenerator {

  private final TimestampBasedAvroKeyGenerator timestampBasedAvroKeyGenerator;

  public TimestampBasedKeyGenerator(TypedProperties config) throws IOException {
    this(config, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY),
        config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY));
  }

  TimestampBasedKeyGenerator(TypedProperties config, String partitionPathField) throws IOException {
    this(config, null, partitionPathField);
  }

  TimestampBasedKeyGenerator(TypedProperties config, String recordKeyField, String partitionPathField) throws IOException {
    super(config, recordKeyField, partitionPathField);
    timestampBasedAvroKeyGenerator = new TimestampBasedAvroKeyGenerator(config, recordKeyField, partitionPathField);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return timestampBasedAvroKeyGenerator.getPartitionPath(record);
  }

  @Override
  public String getRecordKey(Row row) {
    buildFieldPositionMapIfNeeded(row.schema());
    return RowKeyGeneratorHelper.getRecordKeyFromRow(row, getRecordKeyFields(), recordKeyPositions, false);
  }

  @Override
  public String getPartitionPath(Row row) {
    Object fieldVal = null;
    buildFieldPositionMapIfNeeded(row.schema());
    Object partitionPathFieldVal = RowKeyGeneratorHelper.getNestedFieldVal(row, partitionPathPositions.get(getPartitionPathFields().get(0)));
    try {
      if (partitionPathFieldVal == null || partitionPathFieldVal.toString().contains(DEFAULT_PARTITION_PATH) || partitionPathFieldVal.toString().contains(NULL_RECORDKEY_PLACEHOLDER)
          || partitionPathFieldVal.toString().contains(EMPTY_RECORDKEY_PLACEHOLDER)) {
        fieldVal = timestampBasedAvroKeyGenerator.getDefaultPartitionVal();
      } else {
        fieldVal = partitionPathFieldVal;
      }
      return timestampBasedAvroKeyGenerator.getPartitionPath(fieldVal);
    } catch (Exception e) {
      throw new HoodieKeyGeneratorException("Unable to parse input partition field :" + fieldVal, e);
    }
  }
}