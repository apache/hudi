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

package org.apache.hudi.common.config;

import java.util.Properties;

/**
 * A config class extending {@link HoodieConfig} for testing only.
 */
public class HoodieTestFakeConfig extends HoodieConfig {

  public static ConfigProperty<String> FAKE_STRING_CONFIG = TestConfigProperty.FAKE_STRING_CONFIG;
  public static ConfigProperty<Integer> FAKE_INTEGER_CONFIG = TestConfigProperty.FAKE_INTEGER_CONFIG;
  public static ConfigProperty<String> FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER
      = TestConfigProperty.FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER;
  public static ConfigProperty<String> FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY
      = TestConfigProperty.FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY;

  private HoodieTestFakeConfig() {
    super();
  }

  private HoodieTestFakeConfig(Properties props) {
    super(props);
  }

  public String getFakeString() {
    return getString(FAKE_STRING_CONFIG);
  }

  public int getFakeInteger() {
    return getInt(FAKE_INTEGER_CONFIG);
  }

  public String getFakeStringNoDefaultWithInfer() {
    return getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER);
  }

  public String getFakeStringNoDefaultWithInferEmpty() {
    return getString(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY);
  }

  public static HoodieTestFakeConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private final HoodieTestFakeConfig config = new HoodieTestFakeConfig();

    public Builder withFakeString(String value) {
      config.setValue(FAKE_STRING_CONFIG, value);
      return this;
    }

    public Builder withFakeInteger(int value) {
      config.setValue(FAKE_INTEGER_CONFIG, String.valueOf(value));
      return this;
    }

    public Builder withFakeStringNoDefaultWithInfer(String value) {
      config.setValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER, value);
      return this;
    }

    public Builder withFakeStringNoDefaultWithInferEmpty(String value) {
      config.setValue(FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY, value);
      return this;
    }

    public HoodieTestFakeConfig build() {
      setDefaults();
      return new HoodieTestFakeConfig(config.getProps());
    }

    private void setDefaults() {
      config.setDefaults(HoodieTestFakeConfig.class.getName());
    }
  }
}
