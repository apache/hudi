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

import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.config.DefaultHoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Bootstrap specific configs.
 */
public class HoodieBootstrapConfig extends DefaultHoodieConfig {

  public static final String BOOTSTRAP_BASE_PATH_PROP = "hoodie.bootstrap.base.path";
  public static final String BOOTSTRAP_MODE_SELECTOR = "hoodie.bootstrap.mode.selector";
  public static final String FULL_BOOTSTRAP_INPUT_PROVIDER = "hoodie.bootstrap.full.input.provider";
  public static final String BOOTSTRAP_KEYGEN_CLASS = "hoodie.bootstrap.keygen.class";
  public static final String BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS =
      "hoodie.bootstrap.partitionpath.translator.class";
  public static final String DEFAULT_BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS =
      IdentityBootstrapPartitionPathTranslator.class.getName();

  public static final String BOOTSTRAP_PARALLELISM = "hoodie.bootstrap.parallelism";
  public static final String DEFAULT_BOOTSTRAP_PARALLELISM = "1500";

  // Used By BootstrapRegexModeSelector class. When a partition path matches the regex, the corresponding
  // mode will be used. Otherwise, the alternative mode will be used.
  public static final String BOOTSTRAP_MODE_SELECTOR_REGEX = "hoodie.bootstrap.mode.selector.regex";
  public static final String BOOTSTRAP_MODE_SELECTOR_REGEX_MODE = "hoodie.bootstrap.mode.selector.regex.mode";
  public static final String DEFAULT_BOOTSTRAP_MODE_SELECTOR_REGEX = ".*";
  public static final String DEFAULT_BOOTSTRAP_MODE_SELECTOR_REGEX_MODE = BootstrapMode.METADATA_ONLY.name();

  public static final String BOOTSTRAP_INDEX_CLASS_PROP = "hoodie.bootstrap.index.class";
  public static final String DEFAULT_BOOTSTRAP_INDEX_CLASS = HFileBootstrapIndex.class.getName();

  public HoodieBootstrapConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
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

    public Builder withBootstrapBasePath(String basePath) {
      props.setProperty(BOOTSTRAP_BASE_PATH_PROP, basePath);
      return this;
    }

    public Builder withBootstrapModeSelector(String partitionSelectorClass) {
      props.setProperty(BOOTSTRAP_MODE_SELECTOR, partitionSelectorClass);
      return this;
    }

    public Builder withFullBootstrapInputProvider(String partitionSelectorClass) {
      props.setProperty(FULL_BOOTSTRAP_INPUT_PROVIDER, partitionSelectorClass);
      return this;
    }

    public Builder withBootstrapKeyGenClass(String keyGenClass) {
      props.setProperty(BOOTSTRAP_KEYGEN_CLASS, keyGenClass);
      return this;
    }

    public Builder withBootstrapPartitionPathTranslatorClass(String partitionPathTranslatorClass) {
      props.setProperty(BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS, partitionPathTranslatorClass);
      return this;
    }

    public Builder withBootstrapParallelism(int parallelism) {
      props.setProperty(BOOTSTRAP_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withBootstrapModeSelectorRegex(String regex) {
      props.setProperty(BOOTSTRAP_MODE_SELECTOR_REGEX, regex);
      return this;
    }

    public Builder withBootstrapModeForRegexMatch(BootstrapMode modeForRegexMatch) {
      props.setProperty(BOOTSTRAP_MODE_SELECTOR_REGEX_MODE, modeForRegexMatch.name());
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieBootstrapConfig build() {
      HoodieBootstrapConfig config = new HoodieBootstrapConfig(props);
      setDefaultOnCondition(props, !props.containsKey(BOOTSTRAP_PARALLELISM), BOOTSTRAP_PARALLELISM,
          DEFAULT_BOOTSTRAP_PARALLELISM);
      setDefaultOnCondition(props, !props.containsKey(BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS),
          BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS, DEFAULT_BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS);
      setDefaultOnCondition(props, !props.containsKey(BOOTSTRAP_MODE_SELECTOR), BOOTSTRAP_MODE_SELECTOR,
          MetadataOnlyBootstrapModeSelector.class.getCanonicalName());
      setDefaultOnCondition(props, !props.containsKey(BOOTSTRAP_MODE_SELECTOR_REGEX), BOOTSTRAP_MODE_SELECTOR_REGEX,
          DEFAULT_BOOTSTRAP_MODE_SELECTOR_REGEX);
      setDefaultOnCondition(props, !props.containsKey(BOOTSTRAP_MODE_SELECTOR_REGEX_MODE),
          BOOTSTRAP_MODE_SELECTOR_REGEX_MODE, DEFAULT_BOOTSTRAP_MODE_SELECTOR_REGEX_MODE);
      BootstrapMode.valueOf(props.getProperty(BOOTSTRAP_MODE_SELECTOR_REGEX_MODE));
      setDefaultOnCondition(props, !props.containsKey(BOOTSTRAP_INDEX_CLASS_PROP), BOOTSTRAP_INDEX_CLASS_PROP,
          DEFAULT_BOOTSTRAP_INDEX_CLASS);
      return config;
    }
  }
}
