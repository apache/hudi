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
 * Configurations used by the Hudi Table Service Manager.
 */
@Immutable
@ConfigClassProperty(name = "Table Service Manager Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations used by the Hudi Table Service Manager.")
public class HoodieTableServiceManagerConfig extends HoodieConfig {

  public static final String TABLE_SERVICE_MANAGER_PREFIX = "hoodie.table.service.manager";

  public static final ConfigProperty<Boolean> TABLE_SERVICE_MANAGER_ENABLE = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".enable")
      .defaultValue(false)
      .withDocumentation("Use table manager service to execute table service");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_URIS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".uris")
      .defaultValue("http://localhost:9091")
      .withDocumentation("Table service manager uris");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_ACTIONS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".actions")
      .defaultValue("")
      .withDocumentation("Which action deploy on table service manager such as compaction:clean, default null");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_USERNAME = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.username")
      .defaultValue("default")
      .withDocumentation("The user name to deploy for table service of this table");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_QUEUE = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.queue")
      .defaultValue("default")
      .withDocumentation("The queue to deploy for table service of this table");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_RESOURCE = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.resource")
      .defaultValue("4g:4g")
      .withDocumentation("The resource to deploy for table service of this table, default driver 4g, executor 4g");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_DEPLOY_PARALLELISM = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.parallelism")
      .defaultValue(100)
      .withDocumentation("The max parallelism to deploy for table service of this table, default 100");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_EXECUTION_ENGINE = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".execution.engine")
      .defaultValue("spark")
      .withDocumentation("The execution engine to deploy for table service of this table, default spark");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_EXTRA_PARAMS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.extra.params")
      .defaultValue("")
      .withDocumentation("The extra params to deploy for table service of this table, split by ';'");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_TIMEOUT = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".timeout")
      .defaultValue(300)
      .withDocumentation("Connection timeout for client");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_RETRIES = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".connect.retries")
      .defaultValue(3)
      .withDocumentation("Number of retries while opening a connection to table service manager");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_RETRY_DELAY = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".connect.retry.delay")
      .defaultValue(1)
      .withDocumentation("Number of seconds for the client to wait between consecutive connection attempts");

  public static final ConfigProperty<String> RETRY_EXCEPTIONS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".operation.retry.exceptions")
      .defaultValue("IOException")
      .withDocumentation("The class name of the Exception that needs to be re-tryed, separated by commas. "
          + "Default is empty which means retry all the IOException and RuntimeException from FileSystem");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_TOLERABLE_NUM = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".tolerable.num")
      .defaultValue(0)
      .withDocumentation("Number of connection to table manager service unsuccessful tolerable for the client");

  public static HoodieTableServiceManagerConfig.Builder newBuilder() {
    return new HoodieTableServiceManagerConfig.Builder();
  }

  public boolean enableTableServiceManager() {
    return getBoolean(TABLE_SERVICE_MANAGER_ENABLE);
  }

  public String getTableServiceManagerURIS() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_URIS);
  }

  public String getTableServiceManagerActions() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_ACTIONS);
  }

  public String getDeployUsername() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_USERNAME);
  }

  public String getDeployQueue() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_QUEUE);
  }

  public String getDeployResource() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_RESOURCE);
  }

  public int getDeployParallelism() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_PARALLELISM);
  }

  public String getDeployExtraParams() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_EXTRA_PARAMS);
  }

  public String getDeployExecutionEngine() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_EXECUTION_ENGINE);
  }

  public int getConnectionTimeout() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_TIMEOUT);
  }

  public int getConnectionRetryLimit() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_RETRIES);
  }

  public int getConnectionRetryDelay() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_RETRY_DELAY);
  }

  public int getConnectionTolerableNum() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_TOLERABLE_NUM);
  }

  public boolean isTableServiceManagerSupportsAction(ActionType actionType) {
    return enableTableServiceManager() && getTableServiceManagerActions().contains(actionType.name());
  }

  public static class Builder {
    private final HoodieTableServiceManagerConfig config = new HoodieTableServiceManagerConfig();

    public Builder fromProperties(Properties props) {
      this.config.getProps().putAll(props);
      return this;
    }

    public Builder setUris(String uris) {
      config.setValue(TABLE_SERVICE_MANAGER_URIS, uris);
      return this;
    }

    public HoodieTableServiceManagerConfig build() {
      config.setDefaults(HoodieTableServiceManagerConfig.class.getName());
      return config;
    }
  }
}
