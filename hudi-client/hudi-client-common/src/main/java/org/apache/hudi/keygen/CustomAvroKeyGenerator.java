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
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.IOException;
import java.util.Collections;

/**
 * This is a generic implementation of KeyGenerator where users can configure record key as a single field or a combination of fields. Similarly partition path can be configured to have multiple
 * fields or only one field. This class expects value for prop "hoodie.datasource.write.partitionpath.field" in a specific format. For example:
 * <p>
 * properties.put("hoodie.datasource.write.partitionpath.field", "field1:PartitionKeyType1,field2:PartitionKeyType2").
 * <p>
 * The complete partition path is created as <value for field1 basis PartitionKeyType1>/<value for field2 basis PartitionKeyType2> and so on.
 * <p>
 * Few points to consider: 1. If you want to customize some partition path field on a timestamp basis, you can use field1:timestampBased 2. If you simply want to have the value of your configured
 * field in the partition path, use field1:simple 3. If you want your table to be non partitioned, simply leave it as blank.
 * <p>
 * RecordKey is internally generated using either SimpleKeyGenerator or ComplexKeyGenerator.
 */
public class CustomAvroKeyGenerator extends BaseKeyGenerator {

  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
  public static final String SPLIT_REGEX = ":";

  /**
   * Used as a part of config in CustomKeyGenerator.java.
   */
  public enum PartitionKeyType {
    SIMPLE, TIMESTAMP
  }

  public CustomAvroKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = props.getStringList(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), ",",
        Collections.singletonList(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.defaultValue()));
    this.partitionPathFields = props.getStringList(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), ",",
        Collections.emptyList());
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    if (getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for partition path in cfg");
    }

    String partitionPathField;
    StringBuilder partitionPath = new StringBuilder();

    //Corresponds to no partition case
    if (getPartitionPathFields().isEmpty()) {
      return "";
    }
    for (String field : getPartitionPathFields()) {
      String[] fieldWithType = field.split(SPLIT_REGEX);
      if (fieldWithType.length != 2) {
        throw new HoodieKeyException("Unable to find field names for partition path in proper format");
      }

      partitionPathField = fieldWithType[0];
      PartitionKeyType keyType = PartitionKeyType.valueOf(fieldWithType[1].toUpperCase());
      switch (keyType) {
        case SIMPLE:
          partitionPath.append(new SimpleAvroKeyGenerator(config, partitionPathField).getPartitionPath(record));
          break;
        case TIMESTAMP:
          try {
            partitionPath.append(new TimestampBasedAvroKeyGenerator(config, partitionPathField).getPartitionPath(record));
          } catch (IOException e) {
            throw new HoodieKeyGeneratorException("Unable to initialise TimestampBasedKeyGenerator class", e);
          }
          break;
        default:
          throw new HoodieKeyGeneratorException("Please provide valid PartitionKeyType with fields! You provided: " + keyType);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    validateRecordKeyFields();
    return getRecordKeyFields().size() == 1
        ? new SimpleAvroKeyGenerator(config).getRecordKey(record)
        : new ComplexAvroKeyGenerator(config).getRecordKey(record);
  }

  private void validateRecordKeyFields() {
    if (getRecordKeyFields() == null || getRecordKeyFields().isEmpty()) {
      throw new HoodieKeyException("Unable to find field names for record key in cfg");
    }
  }

  public String getDefaultPartitionPathSeparator() {
    return DEFAULT_PARTITION_PATH_SEPARATOR;
  }
}
