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

package org.apache.hudi.keygen.factory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.AutoRecordGenWrapperAvroKeyGenerator;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

public class TestCreateAvroKeyGeneratorByTypeWithFactory {

  private TypedProperties props;

  private static Stream<Arguments> configParams() {
    String[] types = {KeyGeneratorType.SIMPLE.name(), KeyGeneratorType.TIMESTAMP.name(), KeyGeneratorType.COMPLEX.name(),
        KeyGeneratorType.CUSTOM.name(), KeyGeneratorType.NON_PARTITION.name(), KeyGeneratorType.GLOBAL_DELETE.name()};
    return Stream.of(types).map(Arguments::of);
  }

  @BeforeEach
  public void init() {
    props = new TypedProperties();
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    props.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");

    // for timestamp based key generator
    props.put("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING");
    props.put("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd");
    props.put("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
  }

  @AfterEach
  public void teardown() {
    props = null;
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testKeyGeneratorTypes(String keyGenType) throws IOException {
    props.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), keyGenType);
    KeyGeneratorType keyType = KeyGeneratorType.valueOf(keyGenType);
    if (keyType == KeyGeneratorType.CUSTOM) {
      // input needs to be properly formatted
      props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp:timestamp");
    }
    KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(props);
    switch (keyType) {
      case SIMPLE:
        Assertions.assertEquals(SimpleAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());
        return;
      case COMPLEX:
        Assertions.assertEquals(ComplexAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());
        return;
      case TIMESTAMP:
        Assertions.assertEquals(TimestampBasedAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());
        return;
      case CUSTOM:
        Assertions.assertEquals(CustomAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());
        return;
      case NON_PARTITION:
        Assertions.assertEquals(NonpartitionedAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());
        return;
      case GLOBAL_DELETE:
        Assertions.assertEquals(GlobalAvroDeleteKeyGenerator.class.getName(), keyGenerator.getClass().getName());
        return;
      default:
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGenType);
    }
  }

  @Test
  public void testAutoRecordKeyGenerator() throws IOException {
    props = new TypedProperties();
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");
    props.put(KeyGenUtils.RECORD_KEY_GEN_INSTANT_TIME_CONFIG, "100");
    props.put(KeyGenUtils.RECORD_KEY_GEN_PARTITION_ID_CONFIG, 1);
    KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(props);
    Assertions.assertEquals(AutoRecordGenWrapperAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());
  }
}
