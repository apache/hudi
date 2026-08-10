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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Pre-write validator configurations.
 * These validators are invoked before the write operation begins.
 */
@Immutable
@ConfigClassProperty(name = "PreWrite Validator Configurations",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "The following set of configurations help validate data before writes.")
public class HoodiePreWriteValidatorConfig extends HoodieConfig {

  public static final ConfigProperty<String> VALIDATOR_CLASS_NAMES = ConfigProperty
      .key("hoodie.prewrite.validators")
      .defaultValue("")
      .markAdvanced()
      .withDocumentation("Comma separated list of class names that can be invoked to validate before write operations");

  private HoodiePreWriteValidatorConfig() {
    super();
  }

  public static HoodiePreWriteValidatorConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodiePreWriteValidatorConfig preWriteValidatorConfig = new HoodiePreWriteValidatorConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.preWriteValidatorConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.preWriteValidatorConfig.getProps().putAll(props);
      return this;
    }

    public Builder withPreWriteValidator(String preWriteValidators) {
      preWriteValidatorConfig.setValue(VALIDATOR_CLASS_NAMES, preWriteValidators);
      return this;
    }

    public HoodiePreWriteValidatorConfig build() {
      preWriteValidatorConfig.setDefaults(HoodiePreWriteValidatorConfig.class.getName());
      return preWriteValidatorConfig;
    }
  }
}
