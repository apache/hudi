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
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.CustomAvroKeyGenerator;
import org.apache.hudi.keygen.GlobalAvroDeleteKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory help to create {@link org.apache.hudi.keygen.KeyGenerator}.
 * <p>
 * This factory will try {@link HoodieWriteConfig#KEYGENERATOR_CLASS_NAME} firstly, this ensures the class prop
 * will not be overwritten by {@link KeyGeneratorType}
 */
public class HoodieAvroKeyGeneratorFactory extends KeyGeneratorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAvroKeyGeneratorFactory.class);

  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    return KeyGeneratorFactory.createKeyGenerator(props, LOG, HoodieAvroKeyGeneratorFactory::convertTypeToKeyGenerator);
  }

  private static KeyGenerator convertTypeToKeyGenerator(KeyGeneratorType keyGeneratorTypeEnum, TypedProperties props)
      throws IOException {
    switch (keyGeneratorTypeEnum) {
      case SIMPLE:
        return new SimpleAvroKeyGenerator(props);
      case COMPLEX:
        return new ComplexAvroKeyGenerator(props);
      case TIMESTAMP:
        return new TimestampBasedAvroKeyGenerator(props);
      case CUSTOM:
        return new CustomAvroKeyGenerator(props);
      case NON_PARTITION:
        return new NonpartitionedAvroKeyGenerator(props);
      case GLOBAL_DELETE:
        return new GlobalAvroDeleteKeyGenerator(props);
      default:
        throw new HoodieKeyGeneratorException("Unsupported keyGenerator Type " + keyGeneratorTypeEnum);
    }
  }
}
