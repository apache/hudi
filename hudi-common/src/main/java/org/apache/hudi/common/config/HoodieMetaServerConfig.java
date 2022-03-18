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

package org.apache.hudi.common.config;

import javax.annotation.concurrent.Immutable;
import java.util.Properties;

/**
 * Configurations used by the HUDI Metastore.
 */
@Immutable
@ConfigClassProperty(name = "Metastore Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations used by the Hudi Metastore.")
public class HoodieMetaServerConfig extends HoodieConfig {

  public static final String META_SERVER_PREFIX = "hoodie.metaserver";

  public static final ConfigProperty<Boolean> META_SERVER_ENABLE = ConfigProperty
      .key(META_SERVER_PREFIX + ".enable")
      .defaultValue(false)
      .withDocumentation("Use metastore server to store hoodie table metadata");

  public static final ConfigProperty<String> META_SERVER_URLS = ConfigProperty
      .key(META_SERVER_PREFIX + ".uris")
      .defaultValue("thrift://localhost:9090")
      .withDocumentation("Metastore server uris");

  public static final ConfigProperty<Integer> META_SERVER_CONNECTION_RETRIES = ConfigProperty
      .key(META_SERVER_PREFIX + ".connect.retries")
      .defaultValue(3)
      .withDocumentation("Number of retries while opening a connection to metastore");

  public static final ConfigProperty<Integer> META_SERVER_CONNECTION_RETRY_DELAY = ConfigProperty
      .key(META_SERVER_PREFIX + ".connect.retry.delay")
      .defaultValue(1)
      .withDocumentation("Number of seconds for the client to wait between consecutive connection attempts");

  public static HoodieMetaServerConfig.Builder newBuilder() {
    return new HoodieMetaServerConfig.Builder();
  }

  public boolean enableMetaServer() {
    return getBoolean(META_SERVER_ENABLE);
  }

  public String getMetaServerUris() {
    return getStringOrDefault(META_SERVER_URLS);
  }

  public int getConnectionRetryLimit() {
    return getIntOrDefault(META_SERVER_CONNECTION_RETRIES);
  }

  public int getConnectionRetryDelay() {
    return getIntOrDefault(META_SERVER_CONNECTION_RETRY_DELAY);
  }

  public static class Builder {
    private final HoodieMetaServerConfig config = new HoodieMetaServerConfig();

    public Builder fromProperties(Properties props) {
      this.config.getProps().putAll(props);
      return this;
    }

    public Builder setUris(String uris) {
      config.setValue(META_SERVER_URLS, uris);
      return this;
    }

    public HoodieMetaServerConfig build() {
      config.setDefaults(HoodieMetaServerConfig.class.getName());
      return config;
    }
  }
}
