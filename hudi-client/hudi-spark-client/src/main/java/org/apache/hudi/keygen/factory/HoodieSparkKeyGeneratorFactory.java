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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.GlobalDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Factory help to create {@link org.apache.hudi.keygen.KeyGenerator}.
 * <p>
 * This factory will try {@link HoodieWriteConfig#KEYGENERATOR_CLASS_NAME} firstly, this ensures the class prop
 * will not be overwritten by {@link KeyGeneratorType}
 */
public class HoodieSparkKeyGeneratorFactory {

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

  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    String keyGeneratorClass = getKeyGeneratorClassName(props);
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  public static String inferKeyGenClazz(TypedProperties props) {
    String partitionFields = props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), null);
    if (partitionFields != null) {
      int numPartFields = partitionFields.split(",").length;
      String recordsKeyFields = props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), KeyGeneratorOptions.RECORDKEY_FIELD_NAME.defaultValue());
      int numRecordKeyFields = recordsKeyFields.split(",").length;
      if (numPartFields == 1 && numRecordKeyFields == 1) {
        return SimpleKeyGenerator.class.getName();
      } else {
        return ComplexKeyGenerator.class.getName();
      }
    } else {
      return NonpartitionedKeyGenerator.class.getName();
    }
  }

  public static String getKeyGeneratorClassName(TypedProperties props) {
    String keyGeneratorClass = props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), null);

    if (StringUtils.isNullOrEmpty(keyGeneratorClass)) {
      String keyGeneratorType = props.getString(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name());
      LOG.info("The value of {} is empty, use SIMPLE", HoodieWriteConfig.KEYGENERATOR_TYPE.key());
      KeyGeneratorType keyGeneratorTypeEnum;
      try {
        keyGeneratorTypeEnum = KeyGeneratorType.valueOf(keyGeneratorType.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
      }
      switch (keyGeneratorTypeEnum) {
        case SIMPLE:
          keyGeneratorClass = SimpleKeyGenerator.class.getName();
          break;
        case COMPLEX:
          keyGeneratorClass = ComplexKeyGenerator.class.getName();
          break;
        case TIMESTAMP:
          keyGeneratorClass = TimestampBasedKeyGenerator.class.getName();
          break;
        case CUSTOM:
          keyGeneratorClass = CustomKeyGenerator.class.getName();
          break;
        case NON_PARTITION:
          keyGeneratorClass = NonpartitionedKeyGenerator.class.getName();
          break;
        case GLOBAL_DELETE:
          keyGeneratorClass = GlobalDeleteKeyGenerator.class.getName();
          break;
        default:
          throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
      }
    }
    return keyGeneratorClass;
  }

  /**
   * Convert hoodie-common KeyGenerator to SparkKeyGeneratorInterface implement.
   */
  public static String convertToSparkKeyGenerator(String keyGeneratorClassName) {
    return COMMON_TO_SPARK_KEYGENERATOR.getOrDefault(keyGeneratorClassName, keyGeneratorClassName);
  }
}
