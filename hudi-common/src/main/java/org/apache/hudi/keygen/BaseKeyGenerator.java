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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;

import java.util.List;

/**
 * Base abstract class to extend for {@link KeyGenerator} with default logic of taking
 * partitioning and timestamp configs.
 */
public abstract class BaseKeyGenerator extends KeyGenerator {

  public static final String EMPTY_PARTITION = "";
  public static final String CUSTOM_KEY_GENERATOR_SPLIT_REGEX = ":";
  public static final String FIELD_SEPARATOR = ",";
  protected List<String> recordKeyFields;
  protected List<String> partitionPathFields;
  protected final boolean encodePartitionPath;
  protected final boolean hiveStylePartitioning;
  protected final boolean consistentLogicalTimestampEnabled;

  protected BaseKeyGenerator(TypedProperties config) {
    super(config);
    this.encodePartitionPath = config.getBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.key(),
        Boolean.parseBoolean(KeyGeneratorOptions.URL_ENCODE_PARTITIONING.defaultValue()));
    this.hiveStylePartitioning = config.getBoolean(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(),
        Boolean.parseBoolean(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.defaultValue()));
    this.consistentLogicalTimestampEnabled = config.getBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
  }

  /**
   * Generate a record Key out of provided generic record.
   */
  public abstract String getRecordKey(GenericRecord record);

  /**
   * Generate a partition path out of provided generic record.
   */
  public abstract String getPartitionPath(GenericRecord record);

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  @Override
  public final HoodieKey getKey(GenericRecord record) {
    if (getRecordKeyFieldNames() == null || getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }
    return new HoodieKey(getRecordKey(record), getPartitionPath(record));
  }

  @Override
  public List<String> getRecordKeyFieldNames() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }

  public boolean isConsistentLogicalTimestampEnabled() {
    return consistentLogicalTimestampEnabled;
  }
}
