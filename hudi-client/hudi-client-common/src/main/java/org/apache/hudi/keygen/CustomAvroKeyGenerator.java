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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

  public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";
  private final List<BaseKeyGenerator> partitionKeyGenerators;
  private final BaseKeyGenerator recordKeyGenerator;

  /**
   * Used as a part of config in CustomKeyGenerator.java.
   */
  public enum PartitionKeyType {
    SIMPLE, TIMESTAMP
  }

  public CustomAvroKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Option.ofNullable(props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), null))
        .map(recordKeyConfigValue ->
            Arrays.stream(recordKeyConfigValue.split(FIELD_SEPARATOR))
                .map(String::trim).collect(Collectors.toList())
        ).orElse(Collections.emptyList());
    this.partitionPathFields = Arrays.stream(props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()).split(FIELD_SEPARATOR)).map(String::trim).collect(Collectors.toList());
    this.recordKeyGenerator = getRecordKeyFieldNames().size() == 1 ? new SimpleAvroKeyGenerator(config) : new ComplexAvroKeyGenerator(config);
    this.partitionKeyGenerators = getPartitionKeyGenerators(this.partitionPathFields, config);
  }

  private static List<BaseKeyGenerator> getPartitionKeyGenerators(List<String> partitionPathFields, TypedProperties config) {
    if (partitionPathFields.size() == 1 && partitionPathFields.get(0).isEmpty()) {
      return Collections.emptyList(); // Corresponds to no partition case
    } else {
      return partitionPathFields.stream().map(field -> {
        Pair<String, Option<CustomAvroKeyGenerator.PartitionKeyType>> partitionAndType = getPartitionFieldAndKeyType(field);
        if (partitionAndType.getRight().isEmpty()) {
          throw getPartitionPathFormatException();
        }
        CustomAvroKeyGenerator.PartitionKeyType keyType = partitionAndType.getRight().get();
        String partitionPathField = partitionAndType.getLeft();
        switch (keyType) {
          case SIMPLE:
            return new SimpleAvroKeyGenerator(config, partitionPathField);
          case TIMESTAMP:
            try {
              return new TimestampBasedAvroKeyGenerator(config, partitionPathField);
            } catch (IOException e) {
              throw new HoodieKeyGeneratorException("Unable to initialise TimestampBasedKeyGenerator class", e);
            }
          default:
            throw new HoodieKeyGeneratorException("Please provide valid PartitionKeyType with fields! You provided: " + keyType);
        }
      }).collect(Collectors.toList());
    }
  }

  /**
   * Returns list of partition types configured in the partition fields for custom key generator.
   *
   * @param tableConfig Table config where partition fields are configured
   */
  public static List<PartitionKeyType> getPartitionTypes(HoodieTableConfig tableConfig) {
    List<String> partitionPathFields = HoodieTableConfig.getPartitionFieldsForKeyGenerator(tableConfig).orElse(Collections.emptyList());
    if (partitionPathFields.size() == 1 && partitionPathFields.get(0).isEmpty()) {
      return Collections.emptyList(); // Corresponds to no partition case
    } else {
      return partitionPathFields.stream().map(field -> getPartitionFieldAndKeyType(field).getRight())
          .filter(Option::isPresent)
          .map(Option::get)
          .collect(Collectors.toList());
    }
  }

  /**
   * Returns the partition fields with timestamp partition type.
   *
   * @param tableConfig Table config where partition fields are configured
   * @return Optional list of partition fields with timestamp partition type
   */
  public static Option<List<String>> getTimestampFields(HoodieTableConfig tableConfig) {
    List<String> partitionPathFields = HoodieTableConfig.getPartitionFieldsForKeyGenerator(tableConfig).orElse(Collections.emptyList());
    if (partitionPathFields.isEmpty() || (partitionPathFields.size() == 1 && partitionPathFields.get(0).isEmpty())) {
      return Option.of(Collections.emptyList()); // Corresponds to no partition case
    } else if (getPartitionFieldAndKeyType(partitionPathFields.get(0)).getRight().isEmpty()) {
      // Partition type is not configured for the partition fields therefore timestamp partition fields
      // can not be determined
      return Option.empty();
    } else {
      return Option.of(partitionPathFields.stream()
          .map(CustomAvroKeyGenerator::getPartitionFieldAndKeyType)
          .filter(fieldAndKeyType -> fieldAndKeyType.getRight().isPresent() && fieldAndKeyType.getRight().get().equals(PartitionKeyType.TIMESTAMP))
          .map(Pair::getLeft)
          .collect(Collectors.toList()));
    }
  }

  public static Pair<String, Option<PartitionKeyType>> getPartitionFieldAndKeyType(String field) {
    String[] fieldWithType = field.split(BaseKeyGenerator.CUSTOM_KEY_GENERATOR_SPLIT_REGEX);
    if (fieldWithType.length == 2) {
      return Pair.of(fieldWithType[0], Option.of(PartitionKeyType.valueOf(fieldWithType[1].toUpperCase())));
    } else {
      return Pair.of(fieldWithType[0], Option.empty());
    }
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    if (getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for partition path in cfg");
    }
    // Corresponds to no partition case
    if (partitionKeyGenerators.isEmpty()) {
      return "";
    }
    StringBuilder partitionPath = new StringBuilder();
    for (int i = 0; i < partitionKeyGenerators.size(); i++) {
      BaseKeyGenerator partitionKeyGenerator = partitionKeyGenerators.get(i);
      partitionPath.append(partitionKeyGenerator.getPartitionPath(record));
      if (i != partitionKeyGenerators.size() - 1) {
        partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }
    return partitionPath.toString();
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    validateRecordKeyFields();
    return recordKeyGenerator.getRecordKey(record);
  }

  private void validateRecordKeyFields() {
    if (getRecordKeyFieldNames() == null || getRecordKeyFieldNames().isEmpty()) {
      throw new HoodieKeyException("Unable to find field names for record key in cfg");
    }
  }

  static HoodieKeyGeneratorException getPartitionPathFormatException() {
    return new HoodieKeyGeneratorException("Unable to find field names for partition path in proper format. "
        + "Please specify the partition field names in format `field1:type1,field2:type2`. Example: `city:simple,ts:timestamp`");
  }

  public String getDefaultPartitionPathSeparator() {
    return DEFAULT_PARTITION_PATH_SEPARATOR;
  }
}
