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

package org.apache.hudi.common.util;

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Consts;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

public class RemotePartitionHelper implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RemotePartitionHelper.class);

  public static final String URL = "/v1/hoodie/partitioner/getpartitionindex";
  public static final String NUM_BUCKETS_PARAM = "numbuckets";
  public static final String PARTITION_PATH_PARAM = "partitionpath";
  public static final String PARTITION_NUM_PARAM = "partitionnum";
  private final RetryHelper retryHelper;
  private final String serverHost;
  private final Integer serverPort;
  private final ObjectMapper mapper;
  private final int timeoutMs;
  private final HashMap<String, Integer> cache; // dataPartition -> sparkPartitionIndex
  public RemotePartitionHelper(FileSystemViewStorageConfig viewConf) {
    this.retryHelper = new RetryHelper(
        viewConf.getRemoteTimelineClientMaxRetryIntervalMs(),
        viewConf.getRemoteTimelineClientMaxRetryNumbers(),
        viewConf.getRemoteTimelineInitialRetryIntervalMs(),
        viewConf.getRemoteTimelineClientRetryExceptions(),
        "Sending request");
    this.serverHost = viewConf.getRemoteViewServerHost();
    this.serverPort = viewConf.getRemoteViewServerPort();
    this.timeoutMs = viewConf.getRemoteTimelineClientTimeoutSecs() * 1000;
    this.mapper = new ObjectMapper();
    this.cache = new HashMap<>();
  }

  public int getPartition(int numBuckets, String partitionPath, int curBucket, int partitionNum) throws Exception {
    if (cache.containsKey(partitionPath)) {
      return computeActualPartition(cache.get(partitionPath), curBucket, partitionNum);
    }
    URIBuilder builder =
        new URIBuilder().setHost(serverHost).setPort(serverPort).setPath(URL).setScheme("http");

    builder.addParameter(NUM_BUCKETS_PARAM, String.valueOf(numBuckets));
    builder.addParameter(PARTITION_PATH_PARAM, partitionPath);
    builder.addParameter(PARTITION_NUM_PARAM, String.valueOf(partitionNum));

    String url = builder.toString();
    LOG.debug("Sending request : (" + url + ").");
    Response response = (Response)(retryHelper != null ? retryHelper.start(() -> Request.Get(url).connectTimeout(timeoutMs).socketTimeout(timeoutMs).execute())
        : Request.Get(url).connectTimeout(timeoutMs).socketTimeout(timeoutMs).execute());
    String content = response.returnContent().asString(Consts.UTF_8);
    int partitionIndex = Integer.parseInt(mapper.readValue(content, new TypeReference<String>() {}));
    cache.put(partitionPath, partitionIndex);
    return computeActualPartition(partitionIndex, curBucket, partitionNum);
  }

  private int computeActualPartition(int startOffset, int curBucket, int partitionNum) {
    int res = startOffset + curBucket;
    return res >= partitionNum ? res % partitionNum : res;
  }

}
