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

import org.apache.hudi.common.config.DefaultHoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Write callback related config.
 */
public class HoodieWriteCommitCallbackConfig extends DefaultHoodieConfig {

  public static final String CALLBACK_PREFIX = "hoodie.write.commit.callback.";
  public static final String CALLBACK_ON = CALLBACK_PREFIX + "on";
  public static final boolean DEFAULT_CALLBACK_ON = false;

  public static final String CALLBACK_CLASS_PROP = CALLBACK_PREFIX + "class";
  public static final String DEFAULT_CALLBACK_CLASS_PROP = "org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback";

  // ***** HTTP callback configs *****
  public static final String CALLBACK_HTTP_URL_PROP = CALLBACK_PREFIX + "http.url";
  public static final String CALLBACK_HTTP_API_KEY = CALLBACK_PREFIX + "http.api.key";
  public static final String DEFAULT_CALLBACK_HTTP_API_KEY = "hudi_write_commit_http_callback";
  public static final String CALLBACK_HTTP_TIMEOUT_SECONDS = CALLBACK_PREFIX + "http.timeout.seconds";
  public static final int DEFAULT_CALLBACK_HTTP_TIMEOUT_SECONDS = 3;

  private HoodieWriteCommitCallbackConfig(Properties props) {
    super(props);
  }

  public static HoodieWriteCommitCallbackConfig.Builder newBuilder() {
    return new HoodieWriteCommitCallbackConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieWriteCommitCallbackConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieWriteCommitCallbackConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder writeCommitCallbackOn(String callbackOn) {
      props.setProperty(CALLBACK_ON, callbackOn);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackClass(String callbackClass) {
      props.setProperty(CALLBACK_CLASS_PROP, callbackClass);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackHttpUrl(String url) {
      props.setProperty(CALLBACK_HTTP_URL_PROP, url);
      return this;
    }

    public Builder withCallbackHttpTimeoutSeconds(String timeoutSeconds) {
      props.setProperty(CALLBACK_HTTP_TIMEOUT_SECONDS, timeoutSeconds);
      return this;
    }

    public Builder withCallbackHttpApiKey(String apiKey) {
      props.setProperty(CALLBACK_HTTP_API_KEY, apiKey);
      return this;
    }

    public HoodieWriteCommitCallbackConfig build() {
      HoodieWriteCommitCallbackConfig config = new HoodieWriteCommitCallbackConfig(props);
      setDefaultOnCondition(props, !props.containsKey(CALLBACK_ON), CALLBACK_ON, String.valueOf(DEFAULT_CALLBACK_ON));
      setDefaultOnCondition(props, !props.containsKey(CALLBACK_CLASS_PROP), CALLBACK_CLASS_PROP, DEFAULT_CALLBACK_CLASS_PROP);
      setDefaultOnCondition(props, !props.containsKey(CALLBACK_HTTP_API_KEY), CALLBACK_HTTP_API_KEY, DEFAULT_CALLBACK_HTTP_API_KEY);
      setDefaultOnCondition(props, !props.containsKey(CALLBACK_HTTP_TIMEOUT_SECONDS), CALLBACK_HTTP_TIMEOUT_SECONDS,
          String.valueOf(DEFAULT_CALLBACK_HTTP_TIMEOUT_SECONDS));

      return config;
    }
  }

}
