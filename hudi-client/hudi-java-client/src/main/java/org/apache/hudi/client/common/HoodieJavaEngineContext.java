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

package org.apache.hudi.client.common;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.BaseLocalEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.storage.StorageConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A java engine implementation of HoodieEngineContext.
 */
public class HoodieJavaEngineContext extends BaseLocalEngineContext {

  public HoodieJavaEngineContext(StorageConfiguration<?> conf) {
    this(conf, new JavaTaskContextSupplier());
  }

  public HoodieJavaEngineContext(StorageConfiguration<?> conf, TaskContextSupplier taskContextSupplier) {
    super(conf, taskContextSupplier);
  }

  // Allowlist of safe system properties to include in commit metadata. Avoid wildcarding system
  // properties since callers may pass credentials via -D flags (e.g. -Ddb.password=...).
  private static final String[] SAFE_SYSTEM_PROPERTIES = {
      "java.version",
      "java.vendor",
      "java.vm.name",
      "java.vm.version",
      "os.name",
      "os.version",
      "os.arch"
  };

  @Override
  public Map<String, String> getEngineProperties() {
    Map<String, String> info = new HashMap<>();
    for (String property : SAFE_SYSTEM_PROPERTIES) {
      String value = System.getProperty(property);
      if (value != null) {
        info.put(property, value);
      }
    }
    return info;
  }

  @Override
  public KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    return HoodieAvroKeyGeneratorFactory.createKeyGenerator(props);
  }
}
