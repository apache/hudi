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
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.GlobalDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory help to create {@link org.apache.hudi.keygen.KeyGenerator}.
 * <p>
 * This factory will try {@link HoodieWriteConfig#KEYGENERATOR_CLASS_NAME} firstly, this ensures the class prop
 * will not be overwritten by {@link KeyGeneratorType}
 */
public class HoodieSparkKeyGeneratorFactory extends KeyGeneratorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkKeyGeneratorFactory.class);

  private static final Map<String, String> COMMON_TO_SPARK_KEYGENERATOR = new HashMap<>();
  static {
    COMMON_TO_SPARK_KEYGENERATOR.put("org.apache.hudi.keygen.ComplexAvroKeyGenerator",
        "org.apache.hudi.keygen.ComplexKeyGenerator");
    COMMON_TO_SPARK_KEYGENERATOR.put("org.apache.hudi.keygen.CustomAvroKeyGenerator",
        "org.apache.hudi.keygen.CustomKeyGenerator");
    COMMON_TO_SPARK_KEYGENERATOR.put("org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator",
        "org.apache.hudi.keygen.GlobalDeleteKeyGenerator");
    COMMON_TO_SPARK_KEYGENERATOR.put("org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator",
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    COMMON_TO_SPARK_KEYGENERATOR.put("org.apache.hudi.keygen.SimpleAvroKeyGenerator",
        "org.apache.hudi.keygen.SimpleKeyGenerator");
    COMMON_TO_SPARK_KEYGENERATOR.put("org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator",
        "org.apache.hudi.keygen.TimestampBasedKeyGenerator");
  }

  /**
   * Convert hoodie-common KeyGenerator to SparkKeyGeneratorInterface implement.
   */
  public static String convertToSparkKeyGenerator(String keyGeneratorClassName) {
    return COMMON_TO_SPARK_KEYGENERATOR.getOrDefault(keyGeneratorClassName, keyGeneratorClassName);
  }

  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    return KeyGeneratorFactory.createKeyGenerator(props, LOG, HoodieSparkKeyGeneratorFactory::getKeyGenerator);
  }

  private static KeyGenerator getKeyGenerator(KeyGeneratorType keyGeneratorTypeEnum, TypedProperties props)
      throws IOException {
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
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorTypeEnum);
    }
  }
}
