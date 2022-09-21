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

import org.apache.hudi.common.model.ActionType;

import javax.annotation.concurrent.Immutable;

import java.util.Properties;

/**
 * Configurations used by the HUDI Table Management Service.
 */
@Immutable
@ConfigClassProperty(name = "Table Management Service Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations used by the Hudi Table Management Service.")
public class HoodieTableManagerConfig extends HoodieConfig {

  public static final String TABLE_MANAGEMENT_SERVICE_PREFIX = "hoodie.table.management.service";

  public static final ConfigProperty<Boolean> TABLE_MANAGEMENT_SERVICE_ENABLE = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".enable")
      .defaultValue(false)
      .withDocumentation("Use metastore server to store hoodie table metadata");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_HOST = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".host")
      .defaultValue("localhost")
      .withDocumentation("Table management service host");

  public static final ConfigProperty<Integer> TABLE_MANAGEMENT_SERVICE_PORT = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".port")
      .defaultValue(26755)
      .withDocumentation("Table management service port");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_ACTIONS = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".actions")
      .defaultValue("")
      .withDocumentation("Which action deploy on table management service such as compaction:clean, default null");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_DEPLOY_USERNAME = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".deploy.username")
      .defaultValue("default")
      .withDocumentation("The user name to deploy for table service of this table");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_DEPLOY_QUEUE = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".deploy.queue")
      .defaultValue("default")
      .withDocumentation("The queue to deploy for table service of this table");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_DEPLOY_RESOURCE = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".deploy.resource")
      .defaultValue("4g:4g")
      .withDocumentation("The resource to deploy for table service of this table, default driver 4g, executor 4g");

  public static final ConfigProperty<Integer> TABLE_MANAGEMENT_SERVICE_DEPLOY_PARALLELISM = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".deploy.parallelism")
      .defaultValue(100)
      .withDocumentation("The max parallelism to deploy for table service of this table, default 100");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_DEPLOY_EXECUTION_ENGINE = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".execution.engine")
      .defaultValue("spark")
      .withDocumentation("The execution engine to deploy for table service of this table, default spark");

  public static final ConfigProperty<String> TABLE_MANAGEMENT_SERVICE_DEPLOY_EXTRA_PARAMS = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".deploy.extra.params")
      .defaultValue("")
      .withDocumentation("The extra params to deploy for table service of this table, split by ';'");

  public static final ConfigProperty<Integer> TABLE_MANAGEMENT_SERVICE_TIMEOUT = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".timeout")
      .defaultValue(300)
      .withDocumentation("Connection timeout for client");

  public static final ConfigProperty<Integer> TABLE_MANAGEMENT_SERVICE_RETRIES = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".connect.retries")
      .defaultValue(3)
      .withDocumentation("Number of retries while opening a connection to table management service");

  public static final ConfigProperty<Integer> TABLE_MANAGEMENT_SERVICE_RETRY_DELAY = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".connect.retry.delay")
      .defaultValue(1)
      .withDocumentation("Number of seconds for the client to wait between consecutive connection attempts");

  public static final ConfigProperty<Integer> TABLE_MANAGEMENT_SERVICE_TOLERABLE_NUM = ConfigProperty
      .key(TABLE_MANAGEMENT_SERVICE_PREFIX + ".tolerable.num")
      .defaultValue(0)
      .withDocumentation("Number of connection to table management service unsuccessful tolerable for the client");

  public static HoodieTableManagerConfig.Builder newBuilder() {
    return new HoodieTableManagerConfig.Builder();
  }

  public boolean enableTableManager() {
    return getBoolean(TABLE_MANAGEMENT_SERVICE_ENABLE);
  }

  public String getTableManagerHost() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_HOST);
  }

  public Integer getTableManagerPort() {
    return getIntOrDefault(TABLE_MANAGEMENT_SERVICE_PORT);
  }

  public String getTableManagerActions() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_ACTIONS);
  }

  public String getDeployUsername() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_DEPLOY_USERNAME);
  }

  public String getDeployQueue() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_DEPLOY_QUEUE);
  }

  public String getDeployResource() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_DEPLOY_RESOURCE);
  }

  public int getDeployParallelism() {
    return getIntOrDefault(TABLE_MANAGEMENT_SERVICE_DEPLOY_PARALLELISM);
  }

  public String getDeployExtraParams() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_DEPLOY_EXTRA_PARAMS);
  }

  public String getDeployExecutionEngine() {
    return getStringOrDefault(TABLE_MANAGEMENT_SERVICE_DEPLOY_EXECUTION_ENGINE);
  }

  public int getConnectionTimeout() {
    return getIntOrDefault(TABLE_MANAGEMENT_SERVICE_TIMEOUT);
  }

  public int getConnectionRetryLimit() {
    return getIntOrDefault(TABLE_MANAGEMENT_SERVICE_RETRIES);
  }

  public int getConnectionRetryDelay() {
    return getIntOrDefault(TABLE_MANAGEMENT_SERVICE_RETRY_DELAY);
  }

  public int getConnectionTolerableNum() {
    return getIntOrDefault(TABLE_MANAGEMENT_SERVICE_TOLERABLE_NUM);
  }

  public boolean isTableManagerSupportsAction(ActionType actionType) {
    return enableTableManager() && getTableManagerActions().contains(actionType.name());
  }

  public static class Builder {
    private final HoodieTableManagerConfig config = new HoodieTableManagerConfig();

    public Builder fromProperties(Properties props) {
      this.config.getProps().putAll(props);
      return this;
    }

    public Builder withHost(String host) {
      config.setValue(TABLE_MANAGEMENT_SERVICE_HOST, host);
      return this;
    }

    public Builder withPort(String port) {
      config.setValue(TABLE_MANAGEMENT_SERVICE_PORT, port);
      return this;
    }

    public HoodieTableManagerConfig build() {
      config.setDefaults(HoodieTableManagerConfig.class.getName());
      return config;
    }
  }
}
