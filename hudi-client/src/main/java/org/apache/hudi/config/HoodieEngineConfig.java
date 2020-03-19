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

package org.apache.hudi.config;

import org.apache.hudi.common.model.HoodieEngineType;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Engine related config.
 */
public class HoodieEngineConfig extends DefaultHoodieConfig {
  public static final String HOODIE_ENGINE_TYPE_PROP = "hoodie.engine.type";
  public static final String DEFAULT_HOODIE_ENGINE_TYPE = HoodieEngineType.SPARK.name();

  public HoodieEngineConfig(Properties props) {
    super(props);
  }

  public static HoodieEngineConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withHoodieEngineType(HoodieEngineType engineType) {
      props.setProperty(HOODIE_ENGINE_TYPE_PROP, engineType.name());
      return this;
    }

    public HoodieEngineConfig build() {
      HoodieEngineConfig config = new HoodieEngineConfig(props);
      setDefaultOnCondition(props, !props.containsKey(HOODIE_ENGINE_TYPE_PROP), HOODIE_ENGINE_TYPE_PROP, DEFAULT_HOODIE_ENGINE_TYPE);
      HoodieEngineType.valueOf(props.getProperty(HOODIE_ENGINE_TYPE_PROP));
      return config;
    }
  }
}
