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

package org.apache.hudi.keygen.factory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.CheckedBiFunction;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Base class for creating {@link org.apache.hudi.keygen.KeyGenerator}.
 * <p>
 * This factory will try {@link HoodieWriteConfig#KEYGENERATOR_CLASS_NAME} firstly, this ensures the class prop
 * will not be overwritten by {@link KeyGeneratorType}
 */
public class KeyGeneratorFactory {

  static KeyGenerator createKeyGenerator(TypedProperties props, Logger log,
      CheckedBiFunction<KeyGeneratorType, TypedProperties, IOException, KeyGenerator> typeToKeyGeneratorFunction)
      throws IOException {
    // keyGenerator class name has higher priority
    KeyGenerator keyGenerator = createKeyGeneratorByClassName(props);
    return Objects.isNull(keyGenerator)
        ? createKeyGeneratorByType(props, log, typeToKeyGeneratorFunction)
        : keyGenerator;
  }

  private static KeyGenerator createKeyGeneratorByClassName(TypedProperties props) throws IOException {
    KeyGenerator keyGenerator = null;
    String keyGeneratorClass = props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), null);
    if (!StringUtils.isNullOrEmpty(keyGeneratorClass)) {
      try {
        keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
      } catch (Throwable e) {
        throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
      }
    }
    return keyGenerator;
  }

  private static KeyGenerator createKeyGeneratorByType(TypedProperties props, Logger log,
      CheckedBiFunction<KeyGeneratorType, TypedProperties, IOException, KeyGenerator> typeToKeyGeneratorFunction)
      throws IOException {
    // Use KeyGeneratorType.SIMPLE as default keyGeneratorType
    String keyGeneratorType =
        props.getString(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), null);

    if (StringUtils.isNullOrEmpty(keyGeneratorType)) {
      log.info("The value of {} is empty, using SIMPLE", HoodieWriteConfig.KEYGENERATOR_TYPE.key());
      keyGeneratorType = KeyGeneratorType.SIMPLE.name();
    }

    KeyGeneratorType keyGeneratorTypeEnum;
    try {
      keyGeneratorTypeEnum = KeyGeneratorType.valueOf(keyGeneratorType.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
    }

    return typeToKeyGeneratorFunction.apply(keyGeneratorTypeEnum, props);
  }
}
