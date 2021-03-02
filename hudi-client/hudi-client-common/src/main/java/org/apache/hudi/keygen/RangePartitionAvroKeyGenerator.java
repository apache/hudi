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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;

public class RangePartitionAvroKeyGenerator extends SimpleAvroKeyGenerator {

  private final Long rangePerBucket;
  private final String partitionName;

  public static class Config {
    public static final String RANGE_PER_PARTITION_PROP = "hoodie.keygen.range.partition.num";
    public static final Long DEFAULT_RANGE_PER_PARTITION = 100000L;
    public static final String RANGE_PARTITION_NAME_PROP = "hoodie.keygen.range.partition.name";
    public static final String DEFAULT_RANGE_PARTITION_NAME = "rangePartition";
  }

  public RangePartitionAvroKeyGenerator(TypedProperties config) {
    this(config, config.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY),
        config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY));
  }

  RangePartitionAvroKeyGenerator(TypedProperties config, String partitionPathField) {
    this(config, null, partitionPathField);
  }

  RangePartitionAvroKeyGenerator(TypedProperties config, String recordKeyField, String partitionPathField) {
    super(config, recordKeyField, partitionPathField);
    this.rangePerBucket = config.getLong(Config.RANGE_PER_PARTITION_PROP, Config.DEFAULT_RANGE_PER_PARTITION);
    this.partitionName = config.getString(Config.RANGE_PARTITION_NAME_PROP, Config.DEFAULT_RANGE_PARTITION_NAME);
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    Object partitionVal = HoodieAvroUtils.getNestedFieldVal(record, getPartitionPathFields().get(0), true);
    String partitionPath;
    if (partitionVal == null) {
      partitionPath = KeyGenUtils.DEFAULT_PARTITION_PATH;
    } else {
      partitionPath = getPartitionPath(partitionVal);
    }
    return partitionPath;
  }

  public Object getDefaultPartitionVal() {
    return 1L;
  }

  public String getPartitionPath(Object partitionVal) {
    String bucketVal;
    if (partitionVal instanceof Long) {
      bucketVal = String.valueOf((Long) partitionVal / rangePerBucket);
    } else if (partitionVal instanceof Integer) {
      bucketVal = String.valueOf((Integer) partitionVal / rangePerBucket);
    } else if (partitionVal instanceof Double) {
      bucketVal = String.valueOf(Math.round((Double) partitionVal / rangePerBucket));
    } else if (partitionVal instanceof Float) {
      bucketVal = String.valueOf(Math.round((Float) partitionVal / rangePerBucket));
    } else if (partitionVal instanceof String) {
      Long partitionValInLong;
      try {
        partitionValInLong = Long.parseLong((String) partitionVal);
      } catch (Exception e) {
        throw new HoodieKeyGeneratorException(
            "Failed to parse partition field: " + partitionVal + " from String to Long");
      }
      bucketVal = String.valueOf(partitionValInLong / rangePerBucket);
    } else {
      throw new HoodieNotSupportedException(
          "Unexpected type for partition field: " + partitionVal.getClass().getName());
    }
    return hiveStylePartitioning ? partitionName + "=" + bucketVal : bucketVal;
  }
}
