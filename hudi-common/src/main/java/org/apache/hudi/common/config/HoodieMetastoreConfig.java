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
public class HoodieMetastoreConfig extends HoodieConfig {

  public static final String METASTORE_PREFIX = "hoodie.metastore";

  public static final ConfigProperty<Boolean> METASTORE_ENABLE = ConfigProperty
      .key(METASTORE_PREFIX + ".enable")
      .defaultValue(false)
      .withDocumentation("Use metastore server to store hoodie table metadata");

  public static final ConfigProperty<String> METASTORE_URLS = ConfigProperty
      .key(METASTORE_PREFIX + ".uris")
      .defaultValue("thrift://localhost:9090")
      .withDocumentation("Metastore server uris");

  public static final ConfigProperty<Integer> METASTORE_CONNECTION_RETRIES = ConfigProperty
      .key(METASTORE_PREFIX + ".connect.retries")
      .defaultValue(3)
      .withDocumentation("Number of retries while opening a connection to metastore");

  public static final ConfigProperty<Integer> METASTORE_CONNECTION_RETRY_DELAY = ConfigProperty
      .key(METASTORE_PREFIX + ".connect.retry.delay")
      .defaultValue(1)
      .withDocumentation("Number of seconds for the client to wait between consecutive connection attempts");

  public static HoodieMetastoreConfig.Builder newBuilder() {
    return new HoodieMetastoreConfig.Builder();
  }

  public boolean enableMetastore() {
    return getBoolean(METASTORE_ENABLE);
  }

  public String getMetastoreUris() {
    return getStringOrDefault(METASTORE_URLS);
  }

  public int getConnectionRetryLimit() {
    return getIntOrDefault(METASTORE_CONNECTION_RETRIES);
  }

  public int getConnectionRetryDelay() {
    return getIntOrDefault(METASTORE_CONNECTION_RETRY_DELAY);
  }

  public static class Builder {
    private final HoodieMetastoreConfig config = new HoodieMetastoreConfig();

    public Builder fromProperties(Properties props) {
      this.config.getProps().putAll(props);
      return this;
    }

    public Builder setUris(String uris) {
      config.setValue(METASTORE_URLS, uris);
      return this;
    }

    public HoodieMetastoreConfig build() {
      config.setDefaults(HoodieMetastoreConfig.class.getName());
      return config;
    }
  }
}
