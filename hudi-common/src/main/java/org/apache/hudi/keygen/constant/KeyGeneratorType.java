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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

  @EnumFieldDescription("A test KeyGenerator for deltastreamer tests.")
  STREAMER_TEST("org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer$TestGenerator");

  private final String className;

  KeyGeneratorType(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public static KeyGeneratorType fromClassName(String className) {
    for (KeyGeneratorType type : KeyGeneratorType.values()) {
      if (type.getClassName().equals(className)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No KeyGeneratorType found for class name: " + className);
  }

  public static List<String> getNames() {
    List<String> names = new ArrayList<>(KeyGeneratorType.values().length);
    Arrays.stream(KeyGeneratorType.values())
        .forEach(x -> names.add(x.name()));
    return names;
  }

  @Nullable
  public static String getKeyGeneratorClassName(HoodieConfig config) {
    if (config.contains(KEY_GENERATOR_CLASS_NAME)) {
      return config.getString(KEY_GENERATOR_CLASS_NAME);
    } else if (config.contains(KEY_GENERATOR_TYPE)) {
      return KeyGeneratorType.valueOf(config.getString(KEY_GENERATOR_TYPE)).getClassName();
    }
    return null;
  }
}
