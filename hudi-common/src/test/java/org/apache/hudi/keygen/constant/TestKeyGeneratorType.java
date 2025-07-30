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

package org.apache.hudi.keygen.constant;

import org.apache.hudi.common.config.HoodieConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestKeyGeneratorType {
  @Test
  void testIsComplexKeyGeneratorWithKeyGeneratorType() {
    HoodieConfig config = new HoodieConfig();

    config.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    assertTrue(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX_AVRO.name());
    assertTrue(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE.name());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE_AVRO.name());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.TIMESTAMP.name());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.CUSTOM.name());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));
  }

  @Test
  void testIsComplexKeyGeneratorWithKeyGeneratorClassName() {
    HoodieConfig config = new HoodieConfig();

    config.setValue(KEY_GENERATOR_CLASS_NAME, KeyGeneratorType.COMPLEX.getClassName());
    assertTrue(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_CLASS_NAME, KeyGeneratorType.COMPLEX_AVRO.getClassName());
    assertTrue(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_CLASS_NAME, KeyGeneratorType.SIMPLE.getClassName());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_CLASS_NAME, KeyGeneratorType.SIMPLE_AVRO.getClassName());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_CLASS_NAME, KeyGeneratorType.TIMESTAMP.getClassName());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));

    config.setValue(KEY_GENERATOR_CLASS_NAME, KeyGeneratorType.CUSTOM.getClassName());
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));
  }

  @Test
  void testIsComplexKeyGeneratorWithUserDefinedClassName() {
    HoodieConfig config = new HoodieConfig();

    config.setValue(KEY_GENERATOR_CLASS_NAME, "com.example.UserDefinedKeyGenerator");
    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));
  }

  @Test
  void testIsComplexKeyGeneratorWithNoConfig() {
    HoodieConfig config = new HoodieConfig();

    assertFalse(KeyGeneratorType.isComplexKeyGenerator(config));
  }

  private static Stream<Arguments> testFromClassNameParams() {
    return Stream.of(
        Arguments.of("org.apache.hudi.keygen.SimpleKeyGenerator", KeyGeneratorType.SIMPLE),
        Arguments.of("org.apache.hudi.keygen.SimpleAvroKeyGenerator", KeyGeneratorType.SIMPLE_AVRO),
        Arguments.of("org.apache.hudi.keygen.ComplexKeyGenerator", KeyGeneratorType.COMPLEX),
        Arguments.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator", KeyGeneratorType.COMPLEX_AVRO),
        Arguments.of("org.apache.hudi.keygen.TimestampBasedKeyGenerator", KeyGeneratorType.TIMESTAMP),
        Arguments.of("org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator", KeyGeneratorType.TIMESTAMP_AVRO),
        Arguments.of("org.apache.hudi.keygen.CustomKeyGenerator", KeyGeneratorType.CUSTOM),
        Arguments.of("org.apache.hudi.keygen.CustomAvroKeyGenerator", KeyGeneratorType.CUSTOM_AVRO),
        Arguments.of("org.apache.hudi.keygen.NonpartitionedKeyGenerator", KeyGeneratorType.NON_PARTITION),
        Arguments.of("org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator", KeyGeneratorType.NON_PARTITION_AVRO),
        Arguments.of("org.apache.hudi.keygen.GlobalDeleteKeyGenerator", KeyGeneratorType.GLOBAL_DELETE),
        Arguments.of("org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator", KeyGeneratorType.GLOBAL_DELETE_AVRO),
        Arguments.of("org.apache.hudi.keygen.AutoRecordGenWrapperKeyGenerator", KeyGeneratorType.AUTO_RECORD),
        Arguments.of("org.apache.hudi.keygen.AutoRecordGenWrapperAvroKeyGenerator", KeyGeneratorType.AUTO_RECORD_AVRO),
        Arguments.of("org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator", KeyGeneratorType.HOODIE_TABLE_METADATA),
        Arguments.of("org.apache.spark.sql.hudi.command.SqlKeyGenerator", KeyGeneratorType.SPARK_SQL),
        Arguments.of("org.apache.spark.sql.hudi.command.UuidKeyGenerator", KeyGeneratorType.SPARK_SQL_UUID),
        Arguments.of("org.apache.spark.sql.hudi.command.MergeIntoKeyGenerator", KeyGeneratorType.SPARK_SQL_MERGE_INTO),
        Arguments.of("org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer$TestGenerator", KeyGeneratorType.STREAMER_TEST),
        Arguments.of("org.apache.hudi.keygen.CustomUserProvidedKeyGenerator", KeyGeneratorType.USER_PROVIDED),
        Arguments.of("com.example.CustomKeyGenerator", KeyGeneratorType.USER_PROVIDED)
    );
  }

  @ParameterizedTest
  @MethodSource("testFromClassNameParams")
  void testFromClassName(String className, KeyGeneratorType expectedType) {
    KeyGeneratorType result = KeyGeneratorType.fromClassName(className);
    assertEquals(expectedType, result);
  }

  @Test
  void testFromClassNameWithNull() {
    assertThrows(IllegalArgumentException.class, () -> KeyGeneratorType.fromClassName(null));
  }

  @Test
  void testFromClassNameWithEmpty() {
    assertThrows(IllegalArgumentException.class, () -> KeyGeneratorType.fromClassName(""));
  }
}
