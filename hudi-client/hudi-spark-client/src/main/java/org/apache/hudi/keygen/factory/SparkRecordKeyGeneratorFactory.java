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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.AutoSparkRecordKeyGenerator;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.ComplexSparkRecordKeyGenerator;
import org.apache.hudi.keygen.CompositeSparkRecordKeyGenerator;
import org.apache.hudi.keygen.GlobalDeleteKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.SimpleSparkRecordKeyGenerator;
import org.apache.hudi.keygen.SparkRecordKeyGeneratorInterface;
import org.apache.hudi.keygen.TimestampBasedKeyGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkRecordKeyGeneratorFactory {

  private enum KeyGeneratorType {
    SIMPLE, COMPLEX, COMPOSITE;
  }

  private static Map<Class<? extends BuiltinKeyGenerator>, KeyGeneratorType> DEFAULT_MAP = new HashMap<>();

  static {
    DEFAULT_MAP.put(ComplexKeyGenerator.class, KeyGeneratorType.COMPOSITE);
    DEFAULT_MAP.put(GlobalDeleteKeyGenerator.class, KeyGeneratorType.COMPOSITE);
    DEFAULT_MAP.put(NonpartitionedKeyGenerator.class, KeyGeneratorType.COMPLEX);
    DEFAULT_MAP.put(TimestampBasedKeyGenerator.class, KeyGeneratorType.COMPLEX);
    DEFAULT_MAP.put(SimpleKeyGenerator.class, KeyGeneratorType.SIMPLE);
  }

  public static SparkRecordKeyGeneratorInterface getSparkRecordKeyGenerator(TypedProperties props, BuiltinKeyGenerator.SparkRowAccessor rowAccessor,
                                                                            List<String> recordKeyFields, Class<? extends BuiltinKeyGenerator> cls) {
    if (props.getBoolean(HoodieWriteConfig.AUTO_GENERATE_RECORD_KEYS.key(), HoodieWriteConfig.AUTO_GENERATE_RECORD_KEYS.defaultValue())) {
      return new AutoSparkRecordKeyGenerator(props);
    }
    KeyGeneratorType keyGeneratorType = DEFAULT_MAP.get(cls);
    switch (keyGeneratorType) {
      case SIMPLE:
        return new SimpleSparkRecordKeyGenerator(rowAccessor);
      case COMPLEX:
        return new ComplexSparkRecordKeyGenerator(recordKeyFields, rowAccessor);
      case COMPOSITE:
        return new CompositeSparkRecordKeyGenerator(recordKeyFields, rowAccessor);
      default:
        throw new UnsupportedOperationException("No RecordKeyGenerator defined for " + cls.getSimpleName());
    }
  }
}
