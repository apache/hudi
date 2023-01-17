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
public class HoodieMetaserverConfig extends HoodieConfig {

  public static final String METASERVER_PREFIX = "hoodie.metaserver";

  public static final ConfigProperty<Boolean> METASERVER_ENABLE = ConfigProperty
      .key(METASERVER_PREFIX + ".enabled")
      .defaultValue(false)
      .withDocumentation("Enable Hudi metaserver for storing Hudi tables' metadata.");

  public static final ConfigProperty<String> METASERVER_URLS = ConfigProperty
      .key(METASERVER_PREFIX + ".uris")
      .defaultValue("thrift://localhost:9090")
      .withDocumentation("Metastore server uris");

  public static final ConfigProperty<Integer> METASERVER_CONNECTION_RETRIES = ConfigProperty
      .key(METASERVER_PREFIX + ".connect.retries")
      .defaultValue(3)
      .withDocumentation("Number of retries while opening a connection to metastore");

  public static final ConfigProperty<Integer> METASERVER_CONNECTION_RETRY_DELAY = ConfigProperty
      .key(METASERVER_PREFIX + ".connect.retry.delay")
      .defaultValue(1)
      .withDocumentation("Number of seconds for the client to wait between consecutive connection attempts");

  public static HoodieMetaserverConfig.Builder newBuilder() {
    return new HoodieMetaserverConfig.Builder();
  }

  public boolean isMetaserverEnabled() {
    return getBoolean(METASERVER_ENABLE);
  }

  public String getMetaserverUris() {
    return getStringOrDefault(METASERVER_URLS);
  }

  public int getConnectionRetryLimit() {
    return getIntOrDefault(METASERVER_CONNECTION_RETRIES);
  }

  public int getConnectionRetryDelay() {
    return getIntOrDefault(METASERVER_CONNECTION_RETRY_DELAY);
  }

  /**
   * Builder for {@link HoodieMetaserverConfig}.
   */
  public static class Builder {
    private final HoodieMetaserverConfig config = new HoodieMetaserverConfig();

    public Builder fromProperties(Properties props) {
      this.config.getProps().putAll(props);
      return this;
    }

    public Builder setUris(String uris) {
      config.setValue(METASERVER_URLS, uris);
      return this;
    }

    public HoodieMetaserverConfig build() {
      config.setDefaults(HoodieMetaserverConfig.class.getName());
      return config;
    }
  }
}
