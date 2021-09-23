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

package org.apache.hudi.keygen.factory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TestComplexKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class assist test KeyGenerator configuration(class name and type) priority.
 * <p>
 * The functional test of KeyGenerator is left to other unit tests. {@link TestComplexKeyGenerator etc.}.
 */
public class TestHoodieSparkKeyGeneratorFactory {
  @Test
  public void testKeyGeneratorFactory() throws IOException {
    TypedProperties props = getCommonProps();

    // set KeyGenerator type only
    props.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name());
    KeyGenerator keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    Assertions.assertEquals(SimpleKeyGenerator.class.getName(), keyGenerator.getClass().getName());

    // set KeyGenerator class only
    props = getCommonProps();
    props.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName());
    KeyGenerator keyGenerator2 = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    Assertions.assertEquals(SimpleKeyGenerator.class.getName(), keyGenerator2.getClass().getName());

    // set both class name and keyGenerator type
    props.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.CUSTOM.name());
    KeyGenerator keyGenerator3 = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    // KEYGENERATOR_TYPE_PROP was overitten by KEYGENERATOR_CLASS_PROP
    Assertions.assertEquals(SimpleKeyGenerator.class.getName(), keyGenerator3.getClass().getName());

    // set wrong class name
    final TypedProperties props2 = getCommonProps();
    props2.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), TestHoodieSparkKeyGeneratorFactory.class.getName());
    assertThrows(IOException.class, () -> HoodieSparkKeyGeneratorFactory.createKeyGenerator(props2));

    // set wrong keyGenerator type
    final TypedProperties props3 = getCommonProps();
    props3.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), "wrong_type");
    assertThrows(HoodieKeyGeneratorException.class, () -> HoodieSparkKeyGeneratorFactory.createKeyGenerator(props3));
  }

  private TypedProperties getCommonProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    return properties;
  }
}
