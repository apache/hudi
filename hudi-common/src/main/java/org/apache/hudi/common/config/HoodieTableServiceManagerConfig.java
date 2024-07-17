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
 *
 * TODO: enable docs gen by adding {@link ConfigClassProperty} after TSM is landed (HUDI-3475)
 */
@Immutable
public class HoodieTableServiceManagerConfig extends HoodieConfig {

  public static final String TABLE_SERVICE_MANAGER_PREFIX = "hoodie.table.service.manager";

  public static final ConfigProperty<Boolean> TABLE_SERVICE_MANAGER_ENABLED = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".enabled")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("If true, use table service manager to execute table service");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_URIS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".uris")
      .defaultValue("http://localhost:9091")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Table service manager URIs (comma-delimited).");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_ACTIONS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".actions")
      .defaultValue("")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The actions deployed on table service manager, such as compaction or clean.");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_USERNAME = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.username")
      .defaultValue("default")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The user name for this table to deploy table services.");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_QUEUE = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.queue")
      .defaultValue("default")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The queue for this table to deploy table services.");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_RESOURCES = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.resources")
      .defaultValue("spark:4g,4g")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The resources for this table to use for deploying table services.");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_DEPLOY_PARALLELISM = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.parallelism")
      .defaultValue(100)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The parallelism for this table to deploy table services.");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_EXECUTION_ENGINE = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".execution.engine")
      .defaultValue("spark")
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The execution engine to deploy for table service of this table, default spark");

  public static final ConfigProperty<String> TABLE_SERVICE_MANAGER_DEPLOY_EXTRA_PARAMS = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".deploy.extra.params")
      .noDefaultValue()
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("The extra params to deploy for table service of this table, split by ';'");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_TIMEOUT_SEC = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".connection.timeout.sec")
      .defaultValue(300)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Timeout in seconds for connections to table service manager.");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_RETRIES = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".connection.retries")
      .defaultValue(3)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Number of retries while opening a connection to table service manager");

  public static final ConfigProperty<Integer> TABLE_SERVICE_MANAGER_RETRY_DELAY_SEC = ConfigProperty
      .key(TABLE_SERVICE_MANAGER_PREFIX + ".connection.retry.delay.sec")
      .defaultValue(1)
      .markAdvanced()
      .sinceVersion("0.13.0")
      .withDocumentation("Number of seconds for the client to wait between consecutive connection attempts");

  public static HoodieTableServiceManagerConfig.Builder newBuilder() {
    return new HoodieTableServiceManagerConfig.Builder();
  }

  public boolean isTableServiceManagerEnabled() {
    return getBoolean(TABLE_SERVICE_MANAGER_ENABLED);
  }

  public String getTableServiceManagerURIs() {
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

  public String getDeployResources() {
    return getStringOrDefault(TABLE_SERVICE_MANAGER_DEPLOY_RESOURCES);
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

  public int getConnectionTimeoutSec() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_TIMEOUT_SEC);
  }

  public int getConnectionRetryLimit() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_RETRIES);
  }

  public int getConnectionRetryDelay() {
    return getIntOrDefault(TABLE_SERVICE_MANAGER_RETRY_DELAY_SEC);
  }

  public boolean isEnabledAndActionSupported(ActionType actionType) {
    boolean isActionSupported = getTableServiceManagerActions().contains(actionType.name());
    if (actionType.equals(ActionType.clustering)) {
      isActionSupported = isActionSupported || getTableServiceManagerActions().contains(ActionType.replacecommit.name());
    }
    return isTableServiceManagerEnabled() && isActionSupported;
  }

  public static class Builder {
    private final HoodieTableServiceManagerConfig config = new HoodieTableServiceManagerConfig();

    public Builder fromProperties(Properties props) {
      this.config.getProps().putAll(props);
      return this;
    }

    public Builder setURIs(String uris) {
      config.setValue(TABLE_SERVICE_MANAGER_URIS, uris);
      return this;
    }

    public HoodieTableServiceManagerConfig build() {
      config.setDefaults(HoodieTableServiceManagerConfig.class.getName());
      return config;
    }
  }
}
