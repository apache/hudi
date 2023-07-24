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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyGeneratorException;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.keygen.GlobalDeleteKeyGenerator;
import org.apache.hudi.keygen.AutoRecordGenWrapperKeyGenerator;
import org.apache.hudi.keygen.KeyGenUtils;
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

import static org.apache.hudi.config.HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY;
import static org.apache.hudi.config.HoodieWriteConfig.KEYGENERATOR_TYPE;
import static org.apache.hudi.keygen.KeyGenUtils.inferKeyGeneratorType;

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
    boolean autoRecordKeyGen = KeyGenUtils.enableAutoGenerateRecordKeys(props)
        //Need to prevent overwriting the keygen for spark sql merge into because we need to extract
        //the recordkey from the meta cols if it exists. Sql keygen will use pkless keygen if needed.
        && !props.getBoolean(SPARK_SQL_MERGE_INTO_PREPPED_KEY, false);
    try {
      KeyGenerator keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
      if (autoRecordKeyGen) {
        return new AutoRecordGenWrapperKeyGenerator(props, (BuiltinKeyGenerator) keyGenerator);
      } else {
        // if user comes with their own key generator.
        return keyGenerator;
      }
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  /**
   * @param type {@link KeyGeneratorType} enum.
   * @return The key generator class name for Spark based on the {@link KeyGeneratorType}.
   */
  public static String getKeyGeneratorClassNameFromType(KeyGeneratorType type) {
    switch (type) {
      case SIMPLE:
        return SimpleKeyGenerator.class.getName();
      case COMPLEX:
        return ComplexKeyGenerator.class.getName();
      case TIMESTAMP:
        return TimestampBasedKeyGenerator.class.getName();
      case CUSTOM:
        return CustomKeyGenerator.class.getName();
      case NON_PARTITION:
        return NonpartitionedKeyGenerator.class.getName();
      case GLOBAL_DELETE:
        return GlobalDeleteKeyGenerator.class.getName();
      default:
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + type);
    }
  }

  /**
   * Infers the key generator type based on the record key and partition fields.
   * If neither of the record key and partition fields are set, the default type is returned.
   *
   * @param props Properties from the write config.
   * @return Inferred key generator type.
   */
  public static KeyGeneratorType inferKeyGeneratorTypeFromWriteConfig(TypedProperties props) {
    String partitionFields = props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), null);
    String recordsKeyFields = props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), null);
    return inferKeyGeneratorType(Option.ofNullable(recordsKeyFields), partitionFields);
  }

  public static String getKeyGeneratorClassName(TypedProperties props) {
    String keyGeneratorClass = props.getString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), null);

    if (StringUtils.isNullOrEmpty(keyGeneratorClass)) {
      String keyGeneratorType = props.getString(KEYGENERATOR_TYPE.key(), null);
      KeyGeneratorType keyGeneratorTypeEnum;
      if (StringUtils.isNullOrEmpty(keyGeneratorType)) {
        keyGeneratorTypeEnum = inferKeyGeneratorTypeFromWriteConfig(props);
        LOG.info("The value of {} is empty; inferred to be {}",
            KEYGENERATOR_TYPE.key(), keyGeneratorTypeEnum);
      } else {
        try {
          keyGeneratorTypeEnum = KeyGeneratorType.valueOf(keyGeneratorType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
          throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorType);
        }
      }
      keyGeneratorClass = getKeyGeneratorClassNameFromType(keyGeneratorTypeEnum);
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
