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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

@ConfigClassProperty(name = "Redis Index Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control indexing behavior "
        + "(when Redis based indexing is enabled), which tags incoming "
        + "records as either inserts or updates to older records.")
public class HoodieRedisIndexConfig extends HoodieConfig {

  public static final ConfigProperty<String> ADDRESS = ConfigProperty
      .key("hoodie.index.redis.address")
      .noDefaultValue()
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("The address of Redis master, format eg: 127.0.0.1:8080,127.0.0.2:8081.");

  public static final ConfigProperty<String> PASSWORD = ConfigProperty
      .key("hoodie.index.redis.password")
      .noDefaultValue()
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("The password for client connecting to server.");

  public static final ConfigProperty<String> CONNECT_TIMEOUT = ConfigProperty
      .key("hoodie.index.redis.connect-timeout")
      .defaultValue("5s")
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("The timeout value for redis client connecting to server.");

  public static final ConfigProperty<String> SOCKET_TIMEOUT = ConfigProperty
      .key("hoodie.index.redis.socket-timeout")
      .defaultValue("3s")
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("The timeout value for redis client "
          + "reading/writing to server.");

  public static final ConfigProperty<String> EXPIRE_TIME = ConfigProperty
      .key("hoodie.index.redis.expire-time")
      .defaultValue("0s")
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("The redis record expired time. If value set to "
          + "zero or negative, record in redis will never expired.");

  public static final ConfigProperty<Boolean> CLUSTER_MODE = ConfigProperty
      .key("hoodie.index.redis.cluster-mode")
      .defaultValue(false)
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("True if connect to the redis server(s) with the "
          + "cluster mode.");

  public static final ConfigProperty<Integer> MAX_RETRIES = ConfigProperty
      .key("hoodie.index.redis.max-retries")
      .defaultValue(5)
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("The maximum number of retries when an "
          + "exception is caught.");

  public static final ConfigProperty<Integer> CLUSTER_MAX_TOTAL = ConfigProperty
      .key("hoodie.index.redis.cluster.max-connections")
      .defaultValue(2)
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("Set the value for connection instances created "
          + "in pool.");

  public static final ConfigProperty<Integer> CLUSTER_MAX_IDLE = ConfigProperty
      .key("hoodie.index.redis.cluster.max-idle-connections")
      .defaultValue(1)
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("Set the value for max idle connection "
          + "instances created in pool.");

  public static final ConfigProperty<String> CLUSTER_MAX_WAIT = ConfigProperty
      .key("hoodie.index.redis.cluster.max-wait-time")
      .defaultValue("10s")
      .sinceVersion("0.12.0")
      .deprecatedAfter("0.13.0")
      .withDocumentation("Set the value for max waiting time if there is "
          + "no connection resource.");

  private HoodieRedisIndexConfig() {
    super();
  }

  public static HoodieRedisIndexConfig.Builder newBuilder() {
    return new HoodieRedisIndexConfig.Builder();
  }

  public static class Builder {

    private final HoodieRedisIndexConfig redisIndexConfig = new HoodieRedisIndexConfig();

    public HoodieRedisIndexConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (BufferedReader reader = Files.newBufferedReader(propertiesFile.toPath(), StandardCharsets.UTF_8)) {
        this.redisIndexConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieRedisIndexConfig.Builder fromProperties(Properties props) {
      this.redisIndexConfig.getProps().putAll(props);
      return this;
    }

    public HoodieRedisIndexConfig.Builder address(String address) {
      redisIndexConfig.setValue(ADDRESS.key(), address);
      return this;
    }

    public HoodieRedisIndexConfig.Builder password(String password) {
      redisIndexConfig.setValue(PASSWORD.key(), password);
      return this;
    }

    public HoodieRedisIndexConfig.Builder connectTimeout(String connectTimeout) {
      redisIndexConfig.setValue(CONNECT_TIMEOUT.key(), connectTimeout);
      return this;
    }

    public HoodieRedisIndexConfig.Builder socketTimeout(String socketTimeout) {
      redisIndexConfig.setValue(SOCKET_TIMEOUT.key(), socketTimeout);
      return this;
    }

    public HoodieRedisIndexConfig.Builder expireTime(String expireTime) {
      redisIndexConfig.setValue(EXPIRE_TIME.key(), expireTime);
      return this;
    }

    public HoodieRedisIndexConfig.Builder clusterMode(String clusterMode) {
      redisIndexConfig.setValue(CLUSTER_MODE.key(), clusterMode);
      return this;
    }

    public HoodieRedisIndexConfig.Builder maxRetries(String maxRetries) {
      redisIndexConfig.setValue(MAX_RETRIES.key(), maxRetries);
      return this;
    }

    public HoodieRedisIndexConfig.Builder clusterMaxTotal(String clusterMaxTotal) {
      redisIndexConfig.setValue(CLUSTER_MAX_TOTAL.key(), clusterMaxTotal);
      return this;
    }

    public HoodieRedisIndexConfig.Builder clusterMaxIdle(String clusterMaxIdle) {
      redisIndexConfig.setValue(CLUSTER_MAX_IDLE.key(), clusterMaxIdle);
      return this;
    }

    public HoodieRedisIndexConfig.Builder clusterMaxWait(String clusterMaxWait) {
      redisIndexConfig.setValue(CLUSTER_MAX_WAIT.key(), clusterMaxWait);
      return this;
    }

    public HoodieRedisIndexConfig build() {
      redisIndexConfig.setDefaults(HoodieRedisIndexConfig.class.getName());
      return redisIndexConfig;
    }
  }
}
