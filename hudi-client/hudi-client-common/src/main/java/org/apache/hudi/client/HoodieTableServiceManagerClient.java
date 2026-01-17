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

package org.apache.hudi.client;

import org.apache.hudi.common.config.HoodieTableServiceManagerConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieRemoteException;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Client that sends the table service action instants to the table service manager.
 */
@Slf4j
public class HoodieTableServiceManagerClient {

  public enum Action {
    REQUEST,
    CANCEL,
    REGISTER
  }

  private static final String BASE_URL = "/v1/hoodie/service";

  public static final String EXECUTE_COMPACTION = String.format("%s/%s", BASE_URL, "compact");

  public static final String EXECUTE_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster");

  public static final String EXECUTE_CLEAN = String.format("%s/%s", BASE_URL, "clean");

  public static final String ACTION = "action";
  public static final String DATABASE_NAME_PARAM = "db_name";
  public static final String TABLE_NAME_PARAM = "table_name";
  public static final String BASEPATH_PARAM = "basepath";
  public static final String INSTANT_PARAM = "instant";
  public static final String USERNAME = "username";
  public static final String CLUSTER = "cluster";
  public static final String QUEUE = "queue";
  public static final String RESOURCE = "resource";
  public static final String PARALLELISM = "parallelism";
  public static final String EXTRA_PARAMS = "extra_params";
  public static final String EXECUTION_ENGINE = "execution_engine";

  public static final String RETRY_EXCEPTIONS = "IOException";

  private final HoodieTableServiceManagerConfig config;
  private final HoodieTableMetaClient metaClient;
  private final String uri;
  private final String basePath;
  private final String dbName;
  private final String tableName;

  public HoodieTableServiceManagerClient(HoodieTableMetaClient metaClient, HoodieTableServiceManagerConfig config) {
    this.basePath = metaClient.getBasePath().toString();
    this.dbName = metaClient.getTableConfig().getDatabaseName();
    this.tableName = metaClient.getTableConfig().getTableName();
    this.uri = config.getTableServiceManagerURIs();
    this.config = config;
    this.metaClient = metaClient;
  }

  private String executeRequest(String requestPath, Map<String, String> queryParameters) throws IOException {
    URIBuilder builder = new URIBuilder(URI.create(uri)).setPath(requestPath);
    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    log.info("Sending request to table management service : (" + url + ")");
    int timeoutMs = this.config.getConnectionTimeoutSec() * 1000;
    int requestRetryLimit = config.getConnectionRetryLimit();
    int connectionRetryDelay = config.getConnectionRetryDelay();

    RetryHelper<String, IOException> retryHelper = new RetryHelper<>(connectionRetryDelay, requestRetryLimit, connectionRetryDelay, RETRY_EXCEPTIONS);
    return retryHelper.tryWith(() -> Request.Get(url).connectTimeout(timeoutMs).socketTimeout(timeoutMs).execute().returnContent().asString()).start();
  }

  private Map<String, String> getParamsWithAdditionalParams(String[] paramNames, String[] paramVals) {
    Map<String, String> paramsMap = new HashMap<>();
    paramsMap.put(BASEPATH_PARAM, basePath);
    ValidationUtils.checkArgument(paramNames.length == paramVals.length);
    for (int i = 0; i < paramNames.length; i++) {
      paramsMap.put(paramNames[i], paramVals[i]);
    }
    return paramsMap;
  }

  public Option<String> executeCompaction() {
    try {
      String instantRange = StringUtils.join(metaClient.reloadActiveTimeline()
          .filterPendingCompactionTimeline()
          .getInstantsAsStream()
          .map(HoodieInstant::requestedTime)
          .toArray(String[]::new), ",");

      executeRequest(EXECUTE_COMPACTION, getDefaultParams(Action.REQUEST, instantRange));
      return Option.of(instantRange);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public Option<String> executeClean() {
    try {
      String instantRange = StringUtils.join(metaClient.reloadActiveTimeline()
          .getCleanerTimeline()
          .filterInflightsAndRequested()
          .getInstantsAsStream()
          .map(HoodieInstant::requestedTime)
          .toArray(String[]::new), ",");

      executeRequest(EXECUTE_CLEAN, getDefaultParams(Action.REQUEST, instantRange));
      return Option.of(instantRange);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public Option<String> executeClustering() {
    try {
      metaClient.reloadActiveTimeline();
      String instantRange = StringUtils.join(ClusteringUtils.getPendingClusteringInstantTimes(metaClient)
          .stream()
          .map(HoodieInstant::requestedTime)
          .toArray(String[]::new), ",");

      executeRequest(EXECUTE_CLUSTERING, getDefaultParams(Action.REQUEST, instantRange));
      return Option.of(instantRange);
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  private Map<String, String> getDefaultParams(Action action, String instantRange) {
    return getParamsWithAdditionalParams(
        new String[] {ACTION, DATABASE_NAME_PARAM, TABLE_NAME_PARAM, INSTANT_PARAM, USERNAME, QUEUE, RESOURCE, PARALLELISM, EXTRA_PARAMS, EXECUTION_ENGINE},
        new String[] {action.name(), dbName, tableName, instantRange, config.getDeployUsername(), config.getDeployQueue(), config.getDeployResources(),
            String.valueOf(config.getDeployParallelism()), config.getDeployExtraParams(), config.getDeployExecutionEngine()});
  }
}
