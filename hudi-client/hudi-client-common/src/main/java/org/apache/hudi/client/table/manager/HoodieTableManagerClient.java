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

package org.apache.hudi.client.table.manager;

import org.apache.hudi.common.config.HoodieTableManagerConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieRemoteException;

import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Client which send the table service instants to the table management service.
 */
public class HoodieTableManagerClient {

  private static final String BASE_URL = "/v1/hoodie/service";

  public static final String REGISTER_ENDPOINT = String.format("%s/%s", BASE_URL, "register");

  public static final String EXECUTE_COMPACTION = String.format("%s/%s", BASE_URL, "compact/execute");
  public static final String DELETE_COMPACTION = String.format("%s/%s", BASE_URL, "compact/delete");

  public static final String EXECUTE_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster/execute");
  public static final String DELETE_CLUSTERING = String.format("%s/%s", BASE_URL, "cluster/delete");

  public static final String EXECUTE_CLEAN = String.format("%s/%s", BASE_URL, "clean/execute");
  public static final String DELETE_CLEAN = String.format("%s/%s", BASE_URL, "clean/delete");

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

  private final HoodieTableManagerConfig config;
  private final HoodieTableMetaClient metaClient;
  private final String host;
  private final int port;
  private final String basePath;
  private final String dbName;
  private final String tableName;

  private static final Logger LOG = LogManager.getLogger(HoodieTableManagerClient.class);

  public HoodieTableManagerClient(HoodieTableMetaClient metaClient, HoodieTableManagerConfig config) {
    this.basePath = metaClient.getBasePathV2().toString();
    this.dbName = metaClient.getTableConfig().getDatabaseName();
    this.tableName = metaClient.getTableConfig().getTableName();
    this.host = config.getTableManagerHost();
    this.port = config.getTableManagerPort();
    this.config = config;
    this.metaClient = metaClient;
  }

  private String executeRequest(String requestPath, Map<String, String> queryParameters) throws IOException {
    URIBuilder builder =
        new URIBuilder().setHost(host).setPort(port).setPath(requestPath).setScheme("http");
    queryParameters.forEach(builder::addParameter);

    String url = builder.toString();
    LOG.info("Sending request to table management service : (" + url + ")");
    Response response;
    int timeout = this.config.getConnectionTimeout() * 1000; // msec
    int requestRetryLimit = config.getConnectionRetryLimit();
    int retry = 0;

    while (retry < requestRetryLimit) {
      try {
        response = Request.Get(url).connectTimeout(timeout).socketTimeout(timeout).execute();
        return response.returnContent().asString();
      } catch (IOException e) {
        retry++;
        LOG.warn(String.format("Failed request to server %s, will retry for %d times", url, requestRetryLimit - retry), e);
        if (requestRetryLimit == retry) {
          throw e;
        }
      }

      try {
        TimeUnit.SECONDS.sleep(config.getConnectionRetryDelay());
      } catch (InterruptedException e) {
        // ignore
      }
    }

    throw new IOException(String.format("Failed request to table management service %s after retry %d times", url, requestRetryLimit));
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

  public void register() {
    try {
      executeRequest(REGISTER_ENDPOINT, getDefaultParams(null));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public void executeCompaction() {
    try {
      String instantRange = StringUtils.join(metaClient.reloadActiveTimeline()
          .filterPendingCompactionTimeline()
          .getInstants()
          .map(HoodieInstant::getTimestamp)
          .toArray(String[]::new), ",");

      executeRequest(EXECUTE_COMPACTION, getDefaultParams(instantRange));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public void executeClean() {
    try {
      String instantRange = StringUtils.join(metaClient.reloadActiveTimeline()
          .getCleanerTimeline()
          .filterInflightsAndRequested()
          .getInstants()
          .map(HoodieInstant::getTimestamp)
          .toArray(String[]::new), ",");

      executeRequest(EXECUTE_CLEAN, getDefaultParams(instantRange));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  public void executeClustering() {
    try {
      metaClient.reloadActiveTimeline();
      String instantRange = StringUtils.join(ClusteringUtils.getPendingClusteringInstantTimes(metaClient)
          .stream()
          .map(HoodieInstant::getTimestamp)
          .toArray(String[]::new), ",");

      executeRequest(EXECUTE_CLUSTERING, getDefaultParams(instantRange));
    } catch (IOException e) {
      throw new HoodieRemoteException(e);
    }
  }

  private Map<String, String> getDefaultParams(String instantRange) {
    return getParamsWithAdditionalParams(
        new String[] {DATABASE_NAME_PARAM, TABLE_NAME_PARAM, INSTANT_PARAM, USERNAME, QUEUE, RESOURCE, PARALLELISM, EXTRA_PARAMS, EXECUTION_ENGINE},
        new String[] {dbName, tableName, instantRange, config.getDeployUsername(), config.getDeployQueue(), config.getDeployResource(),
            String.valueOf(config.getDeployParallelism()), config.getDeployExtraParams(), config.getDeployExecutionEngine()});
  }
}
