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
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Bootstrap specific configs.
 */
@ConfigClassProperty(name = "Bootstrap Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control how you want to bootstrap your existing tables for the first time into hudi. "
        + "The bootstrap operation can flexibly avoid copying data over before you can use Hudi and support running the existing "
        + " writers and new hudi writers in parallel, to validate the migration.")
public class HoodieBootstrapConfig extends HoodieConfig {

  public static final ConfigProperty<String> BASE_PATH = ConfigProperty
      .key("hoodie.bootstrap.base.path")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Base path of the dataset that needs to be bootstrapped as a Hudi table");

  public static final ConfigProperty<String> MODE_SELECTOR_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.mode.selector")
      .defaultValue(MetadataOnlyBootstrapModeSelector.class.getCanonicalName())
      .sinceVersion("0.6.0")
      .withDocumentation("Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped");

  public static final ConfigProperty<String> FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.full.input.provider")
      .defaultValue("org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider")
      .sinceVersion("0.6.0")
      .withDocumentation("Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD");

  public static final ConfigProperty<String> KEYGEN_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.keygen.class")
      .noDefaultValue()
      .sinceVersion("0.6.0")
      .withDocumentation("Key generator implementation to be used for generating keys from the bootstrapped dataset");

  public static final ConfigProperty<String> KEYGEN_TYPE = ConfigProperty
      .key("hoodie.bootstrap.keygen.type")
      .defaultValue(KeyGeneratorType.SIMPLE.name())
      .sinceVersion("0.9.0")
      .withDocumentation("Type of build-in key generator, currently support SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE");

  public static final ConfigProperty<String> PARTITION_PATH_TRANSLATOR_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.partitionpath.translator.class")
      .defaultValue(IdentityBootstrapPartitionPathTranslator.class.getName())
      .sinceVersion("0.6.0")
      .withDocumentation("Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.");

  public static final ConfigProperty<String> PARALLELISM_VALUE = ConfigProperty
      .key("hoodie.bootstrap.parallelism")
      .defaultValue("1500")
      .sinceVersion("0.6.0")
      .withDocumentation("Parallelism value to be used to bootstrap data into hudi");

  public static final ConfigProperty<String> PARTITION_SELECTOR_REGEX_PATTERN = ConfigProperty
      .key("hoodie.bootstrap.mode.selector.regex")
      .defaultValue(".*")
      .sinceVersion("0.6.0")
      .withDocumentation("Matches each bootstrap dataset partition against this regex and applies the mode below to it.");

  public static final ConfigProperty<String> PARTITION_SELECTOR_REGEX_MODE = ConfigProperty
      .key("hoodie.bootstrap.mode.selector.regex.mode")
      .defaultValue(BootstrapMode.METADATA_ONLY.name())
      .sinceVersion("0.6.0")
      .withDocumentation("Bootstrap mode to apply for partition paths, that match regex above. "
          + "METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. "
          + "FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.");

  public static final ConfigProperty<String> INDEX_CLASS_NAME = ConfigProperty
      .key("hoodie.bootstrap.index.class")
      .defaultValue(HFileBootstrapIndex.class.getName())
      .sinceVersion("0.6.0")
      .withDocumentation("Implementation to use, for mapping a skeleton base file to a boostrap base file.");

  /**
   * @deprecated Use {@link #BASE_PATH} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_BASE_PATH_PROP = BASE_PATH.key();
  /**
   * @deprecated Use {@link #INDEX_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_INDEX_CLASS_PROP = INDEX_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #INDEX_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_INDEX_CLASS = INDEX_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #MODE_SELECTOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_MODE_SELECTOR = MODE_SELECTOR_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String FULL_BOOTSTRAP_INPUT_PROVIDER = FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_FULL_BOOTSTRAP_INPUT_PROVIDER = FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #KEYGEN_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_KEYGEN_CLASS = KEYGEN_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #PARTITION_PATH_TRANSLATOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS = PARTITION_PATH_TRANSLATOR_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #PARTITION_PATH_TRANSLATOR_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS = PARTITION_PATH_TRANSLATOR_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_PARALLELISM = PARALLELISM_VALUE.key();
  /**
   * @deprecated Use {@link #PARALLELISM_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_PARALLELISM = PARALLELISM_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #PARTITION_SELECTOR_REGEX_PATTERN} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_MODE_SELECTOR_REGEX = PARTITION_SELECTOR_REGEX_PATTERN.key();
  /**
   * @deprecated Use {@link #PARTITION_SELECTOR_REGEX_MODE} and its methods instead
   */
  @Deprecated
  public static final String BOOTSTRAP_MODE_SELECTOR_REGEX_MODE = PARTITION_SELECTOR_REGEX_MODE.key();
  /**
   * @deprecated Use {@link #PARTITION_SELECTOR_REGEX_PATTERN} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_MODE_SELECTOR_REGEX = PARTITION_SELECTOR_REGEX_PATTERN.defaultValue();
  /**
   * @deprecated Use {@link #PARTITION_SELECTOR_REGEX_MODE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_BOOTSTRAP_MODE_SELECTOR_REGEX_MODE = PARTITION_SELECTOR_REGEX_MODE.defaultValue();

  private HoodieBootstrapConfig() {
    super();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HoodieBootstrapConfig bootstrapConfig = new HoodieBootstrapConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.bootstrapConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder withBootstrapBasePath(String basePath) {
      bootstrapConfig.setValue(BASE_PATH, basePath);
      return this;
    }

    public Builder withBootstrapModeSelector(String partitionSelectorClass) {
      bootstrapConfig.setValue(MODE_SELECTOR_CLASS_NAME, partitionSelectorClass);
      return this;
    }

    public Builder withFullBootstrapInputProvider(String partitionSelectorClass) {
      bootstrapConfig.setValue(FULL_BOOTSTRAP_INPUT_PROVIDER_CLASS_NAME, partitionSelectorClass);
      return this;
    }

    public Builder withBootstrapKeyGenClass(String keyGenClass) {
      bootstrapConfig.setValue(KEYGEN_CLASS_NAME, keyGenClass);
      return this;
    }

    public Builder withBootstrapKeyGenType(String keyGenType) {
      bootstrapConfig.setValue(KEYGEN_TYPE, keyGenType);
      return this;
    }

    public Builder withBootstrapPartitionPathTranslatorClass(String partitionPathTranslatorClass) {
      bootstrapConfig
          .setValue(PARTITION_PATH_TRANSLATOR_CLASS_NAME, partitionPathTranslatorClass);
      return this;
    }

    public Builder withBootstrapParallelism(int parallelism) {
      bootstrapConfig.setValue(PARALLELISM_VALUE, String.valueOf(parallelism));
      return this;
    }

    public Builder withBootstrapModeSelectorRegex(String regex) {
      bootstrapConfig.setValue(PARTITION_SELECTOR_REGEX_PATTERN, regex);
      return this;
    }

    public Builder withBootstrapModeForRegexMatch(BootstrapMode modeForRegexMatch) {
      bootstrapConfig.setValue(PARTITION_SELECTOR_REGEX_MODE, modeForRegexMatch.name());
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.bootstrapConfig.getProps().putAll(props);
      return this;
    }

    public HoodieBootstrapConfig build() {
      // TODO: use infer function instead
      bootstrapConfig.setDefaultValue(INDEX_CLASS_NAME, HoodieTableConfig.getDefaultBootstrapIndexClass(
          bootstrapConfig.getProps()));
      bootstrapConfig.setDefaults(HoodieBootstrapConfig.class.getName());
      return bootstrapConfig;
    }
  }
}
