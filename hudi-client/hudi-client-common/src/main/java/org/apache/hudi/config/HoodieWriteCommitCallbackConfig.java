/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hudi.common.util.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Write callback related config.
 */
@ConfigClassProperty(name = "Write commit callback configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.COMMIT_CALLBACK,
    areCommonConfigs = true,
    description = "")
public class HoodieWriteCommitCallbackConfig extends HoodieConfig {

  public static final String CALLBACK_PREFIX = "hoodie.write.commit.callback.";

  public static final ConfigProperty<Boolean> TURN_CALLBACK_ON = ConfigProperty
      .key(CALLBACK_PREFIX + "on")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Turn commit callback on/off. off by default.");

  public static final ConfigProperty<String> CALLBACK_CLASS_NAME = ConfigProperty
      .key(CALLBACK_PREFIX + "class")
      .defaultValue("org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback")
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, "
          + "org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default");

  // ***** HTTP callback configs *****
  public static final ConfigProperty<String> CALLBACK_HTTP_URL = ConfigProperty
      .key(CALLBACK_PREFIX + "http.url")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Callback host to be sent along with callback messages");

  public static final ConfigProperty<String> CALLBACK_HTTP_API_KEY_VALUE = ConfigProperty
      .key(CALLBACK_PREFIX + "http.api.key")
      .defaultValue("hudi_write_commit_http_callback")
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Http callback API key. hudi_write_commit_http_callback by default");

  public static final ConfigProperty<Integer> CALLBACK_HTTP_TIMEOUT_IN_SECONDS = ConfigProperty
      .key(CALLBACK_PREFIX + "http.timeout.seconds")
      .defaultValue(30)
      .markAdvanced()
      .sinceVersion("0.6.0")
      .withDocumentation("Callback timeout in seconds.");

  public static final ConfigProperty<String> CALLBACK_HTTP_CUSTOM_HEADERS = ConfigProperty
      .key(CALLBACK_PREFIX + "http.custom.headers")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Http callback custom headers. Format: HeaderName1:HeaderValue1;HeaderName2:HeaderValue2");

  /**
   * @deprecated Use {@link #TURN_CALLBACK_ON} and its methods instead
   */
  @Deprecated
  public static final String CALLBACK_ON = TURN_CALLBACK_ON.key();
  /**
   * @deprecated Use {@link #TURN_CALLBACK_ON} and its methods instead
   */
  @Deprecated
  public static final boolean DEFAULT_CALLBACK_ON = TURN_CALLBACK_ON.defaultValue();
  /**
   * @deprecated Use {@link #CALLBACK_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String CALLBACK_CLASS_PROP = CALLBACK_CLASS_NAME.key();
  /**
   * @deprecated Use {@link #CALLBACK_CLASS_NAME} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CALLBACK_CLASS_PROP = CALLBACK_CLASS_NAME.defaultValue();
  /**
   * @deprecated Use {@link #CALLBACK_HTTP_URL} and its methods instead
   */
  @Deprecated
  public static final String CALLBACK_HTTP_URL_PROP = CALLBACK_HTTP_URL.key();
  /**
   * @deprecated Use {@link #CALLBACK_HTTP_API_KEY_VALUE} and its methods instead
   */
  @Deprecated
  public static final String CALLBACK_HTTP_API_KEY = CALLBACK_HTTP_API_KEY_VALUE.key();
  /**
   * @deprecated Use {@link #CALLBACK_HTTP_API_KEY_VALUE} and its methods instead
   */
  @Deprecated
  public static final String DEFAULT_CALLBACK_HTTP_API_KEY = CALLBACK_HTTP_API_KEY_VALUE.defaultValue();
  /**
   * @deprecated Use {@link #CALLBACK_HTTP_TIMEOUT_IN_SECONDS} and its methods instead
   */
  @Deprecated
  public static final String CALLBACK_HTTP_TIMEOUT_SECONDS = CALLBACK_HTTP_TIMEOUT_IN_SECONDS.key();
  /**
   * @deprecated Use {@link #CALLBACK_HTTP_TIMEOUT_IN_SECONDS} and its methods instead
   */
  @Deprecated
  public static final int DEFAULT_CALLBACK_HTTP_TIMEOUT_SECONDS = CALLBACK_HTTP_TIMEOUT_IN_SECONDS.defaultValue();

  private HoodieWriteCommitCallbackConfig() {
    super();
  }

  public static HoodieWriteCommitCallbackConfig.Builder newBuilder() {
    return new HoodieWriteCommitCallbackConfig.Builder();
  }

  public static class Builder {

    private final HoodieWriteCommitCallbackConfig writeCommitCallbackConfig = new HoodieWriteCommitCallbackConfig();

    public HoodieWriteCommitCallbackConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.writeCommitCallbackConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieWriteCommitCallbackConfig.Builder fromProperties(Properties props) {
      this.writeCommitCallbackConfig.getProps().putAll(props);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder writeCommitCallbackOn(String callbackOn) {
      writeCommitCallbackConfig.setValue(TURN_CALLBACK_ON, callbackOn);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackClass(String callbackClass) {
      writeCommitCallbackConfig.setValue(CALLBACK_CLASS_NAME, callbackClass);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackHttpUrl(String url) {
      writeCommitCallbackConfig.setValue(CALLBACK_HTTP_URL, url);
      return this;
    }

    public Builder withCallbackHttpTimeoutSeconds(String timeoutSeconds) {
      writeCommitCallbackConfig.setValue(CALLBACK_HTTP_TIMEOUT_IN_SECONDS, timeoutSeconds);
      return this;
    }

    public Builder withCallbackHttpApiKey(String apiKey) {
      writeCommitCallbackConfig.setValue(CALLBACK_HTTP_API_KEY_VALUE, apiKey);
      return this;
    }

    public Builder withCustomHeaders(String customHeaders) {
      if (!StringUtils.isNullOrEmpty(customHeaders)) {
        writeCommitCallbackConfig.setValue(CALLBACK_HTTP_CUSTOM_HEADERS, customHeaders);
      }
      return this;
    }

    public HoodieWriteCommitCallbackConfig build() {
      writeCommitCallbackConfig.setDefaults(HoodieWriteCommitCallbackConfig.class.getName());
      return writeCommitCallbackConfig;
    }
  }

}
