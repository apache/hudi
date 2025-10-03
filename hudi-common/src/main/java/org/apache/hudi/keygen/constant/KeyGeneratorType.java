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

package org.apache.hudi.keygen.constant;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;

/**
 * Types of {@link org.apache.hudi.keygen.KeyGenerator}.
 */
@EnumDescription("Key generator type, indicating the key generator class to use, that implements "
    + "`org.apache.hudi.keygen.KeyGenerator`.")
public enum KeyGeneratorType {

  @EnumFieldDescription("Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.")
  SIMPLE("org.apache.hudi.keygen.SimpleKeyGenerator"),
  @EnumFieldDescription("Simple key generator, which takes names of fields to be used for recordKey and partitionPath as configs.")
  SIMPLE_AVRO("org.apache.hudi.keygen.SimpleAvroKeyGenerator"),

  @EnumFieldDescription("Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.")
  COMPLEX("org.apache.hudi.keygen.ComplexKeyGenerator"),
  @EnumFieldDescription("Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.")
  COMPLEX_AVRO("org.apache.hudi.keygen.ComplexAvroKeyGenerator"),

  @EnumFieldDescription("Timestamp-based key generator, that relies on timestamps for partitioning field. Still picks record key by name.")
  TIMESTAMP("org.apache.hudi.keygen.TimestampBasedKeyGenerator"),
  @EnumFieldDescription("Timestamp-based key generator, that relies on timestamps for partitioning field. Still picks record key by name.")
  TIMESTAMP_AVRO("org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator"),

  @EnumFieldDescription("This is a generic implementation type of KeyGenerator where users can configure record key as a single field or "
      + " a combination of fields. Similarly partition path can be configured to have multiple fields or only one field. "
      + " This KeyGenerator expects value for prop \"hoodie.datasource.write.partitionpath.field\" in a specific format. "
      + " For example: "
      + " properties.put(\"hoodie.datasource.write.partitionpath.field\", \"field1:PartitionKeyType1,field2:PartitionKeyType2\").")
  CUSTOM("org.apache.hudi.keygen.CustomKeyGenerator"),
  @EnumFieldDescription("This is a generic implementation type of KeyGenerator where users can configure record key as a single field or "
      + " a combination of fields. Similarly partition path can be configured to have multiple fields or only one field. "
      + " This KeyGenerator expects value for prop \"hoodie.datasource.write.partitionpath.field\" in a specific format. "
      + " For example: "
      + " properties.put(\"hoodie.datasource.write.partitionpath.field\", \"field1:PartitionKeyType1,field2:PartitionKeyType2\").")
  CUSTOM_AVRO("org.apache.hudi.keygen.CustomAvroKeyGenerator"),

  @EnumFieldDescription("Simple Key generator for non-partitioned tables.")
  NON_PARTITION("org.apache.hudi.keygen.NonpartitionedKeyGenerator"),
  @EnumFieldDescription("Simple Key generator for non-partitioned tables.")
  NON_PARTITION_AVRO("org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator"),

  @EnumFieldDescription("Key generator for deletes using global indices.")
  GLOBAL_DELETE("org.apache.hudi.keygen.GlobalDeleteKeyGenerator"),
  @EnumFieldDescription("Key generator for deletes using global indices.")
  GLOBAL_DELETE_AVRO("org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator"),

  @EnumFieldDescription("Automatic record key generation.")
  AUTO_RECORD("org.apache.hudi.keygen.AutoRecordGenWrapperKeyGenerator"),
  @EnumFieldDescription("Automatic record key generation.")
  AUTO_RECORD_AVRO("org.apache.hudi.keygen.AutoRecordGenWrapperAvroKeyGenerator"),

  @EnumFieldDescription("Custom key generator for the Hudi table metadata.")
  HOODIE_TABLE_METADATA("org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator"),

  @EnumFieldDescription("Custom spark-sql specific KeyGenerator overriding behavior handling TimestampType partition values.")
  SPARK_SQL("org.apache.spark.sql.hudi.command.SqlKeyGenerator"),

  @EnumFieldDescription("A KeyGenerator which use the uuid as the record key.")
  SPARK_SQL_UUID("org.apache.spark.sql.hudi.command.UuidKeyGenerator"),

  @EnumFieldDescription("Meant to be used internally for the spark sql MERGE INTO command.")
  SPARK_SQL_MERGE_INTO("org.apache.spark.sql.hudi.command.MergeIntoKeyGenerator"),

  @EnumFieldDescription("A KeyGenerator specified from the configuration.")
  USER_PROVIDED(StringUtils.EMPTY_STRING);

  private String className;
  private static final Logger LOG = LoggerFactory.getLogger(KeyGeneratorType.class);

  KeyGeneratorType(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public static KeyGeneratorType fromClassName(String className) {
    if (StringUtils.isNullOrEmpty(className)) {
      throw new IllegalArgumentException("Invalid keyGenerator class: " + className);
    }
    for (KeyGeneratorType type : KeyGeneratorType.values()) {
      if (type.getClassName().equals(className)) {
        return type;
      }
    }
    return USER_PROVIDED;
  }

  public static List<String> getNames() {
    List<String> names = new ArrayList<>(KeyGeneratorType.values().length);
    Arrays.stream(KeyGeneratorType.values())
        .forEach(x -> names.add(x.name()));
    return names;
  }

  @Nullable
  public static String getKeyGeneratorClassName(TypedProperties props) {
    // For USER_PROVIDED type, since we set key generator class only for this type.
    if (ConfigUtils.containsConfigProperty(props, KEY_GENERATOR_CLASS_NAME)) {
      return ConfigUtils.getStringWithAltKeys(props, KEY_GENERATOR_CLASS_NAME);
    }
    // For other types.
    KeyGeneratorType keyGeneratorType;
    if (ConfigUtils.containsConfigProperty(props, KEY_GENERATOR_TYPE)) {
      keyGeneratorType = KeyGeneratorType.valueOf(ConfigUtils.getStringWithAltKeys(props, KEY_GENERATOR_TYPE));
      // For USER_PROVIDED type, the key generator class has to be provided.
      if (USER_PROVIDED == keyGeneratorType) {
        throw new IllegalArgumentException("No key generator class is provided properly for type: " + USER_PROVIDED.name());
      }
      return keyGeneratorType.getClassName();
    }
    // No key generator information is provided.
    LOG.info("No key generator type is set properly");
    return null;
  }

  @Nullable
  public static String getKeyGeneratorClassName(HoodieConfig config) {
    return getKeyGeneratorClassName(config.getProps());
  }

  @Nullable
  public static String getKeyGeneratorClassName(Map<String, String> config) {
    TypedProperties props = new TypedProperties();
    config.forEach(props::setProperty);
    return getKeyGeneratorClassName(props);
  }

  public static boolean isComplexKeyGenerator(HoodieConfig config) {
    KeyGeneratorType keyGeneratorType;
    if (config.contains(KEY_GENERATOR_TYPE)) {
      keyGeneratorType = KeyGeneratorType.valueOf(config.getString(KEY_GENERATOR_TYPE));
    } else if (config.contains(KEY_GENERATOR_CLASS_NAME)) {
      try {
        keyGeneratorType = fromClassName(config.getString(KEY_GENERATOR_CLASS_NAME));
      } catch (IllegalArgumentException e) {
        return false;
      }
    } else {
      return false;
    }
    return keyGeneratorType == COMPLEX || keyGeneratorType == COMPLEX_AVRO;
  }
}
