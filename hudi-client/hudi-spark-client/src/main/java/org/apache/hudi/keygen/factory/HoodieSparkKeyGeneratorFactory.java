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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.GlobalDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Factory help to create {@link org.apache.hudi.keygen.KeyGenerator}.
 * <p>
 * This factory will try {@link HoodieWriteConfig#KEYGENERATOR_CLASS_PROP} firstly, this ensures the class prop
 * will not be overwritten by {@link KeyGeneratorType}
 */
public class HoodieSparkKeyGeneratorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkKeyGeneratorFactory.class);

  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    // keyGenerator class name has higher priority
    KeyGenerator keyGenerator = KeyGenUtils.createKeyGeneratorByClassName(props);

    return Objects.isNull(keyGenerator) ? createKeyGeneratorByType(props) : keyGenerator;
  }

  private static BuiltinKeyGenerator createKeyGeneratorByType(TypedProperties props) throws IOException {
    // Use KeyGeneratorType.SIMPLE as default keyGeneratorType
    String keyGeneratorType =
        props.getString(HoodieWriteConfig.KEYGENERATOR_TYPE_PROP.key(), null);

    if (StringUtils.isNullOrEmpty(keyGeneratorType)) {
      LOG.info("The value of {} is empty, use SIMPLE", HoodieWriteConfig.KEYGENERATOR_TYPE_PROP.key());
      keyGeneratorType = KeyGeneratorType.SIMPLE.name();
    }

    KeyGeneratorType keyGeneratorTypeEnum;
    try {
      keyGeneratorTypeEnum = KeyGeneratorType.valueOf(keyGeneratorType.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
    }
    switch (keyGeneratorTypeEnum) {
      case SIMPLE:
        return new SimpleKeyGenerator(props);
      case COMPLEX:
        return new ComplexKeyGenerator(props);
      case TIMESTAMP:
        return new TimestampBasedKeyGenerator(props);
      case CUSTOM:
        return new CustomKeyGenerator(props);
      case NON_PARTITION:
        return new NonpartitionedKeyGenerator(props);
      case GLOBAL_DELETE:
        return new GlobalDeleteKeyGenerator(props);
      default:
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
    }
  }

}
