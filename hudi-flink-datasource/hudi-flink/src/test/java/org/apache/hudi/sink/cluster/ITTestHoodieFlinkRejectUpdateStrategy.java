/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sink.cluster;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.clustering.update.strategy.FlinkRejectUpdateStrategy;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestSQL;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 *
 */
public class ITTestHoodieFlinkRejectUpdateStrategy extends TestConsistentHashingClusteringBase {

  @Test
  public void testRejectUpdateStrategy() throws Exception {
    TableEnvironment tableEnv = setupTableEnv();
    prepareData(tableEnv);

    // Schedule clustering
    Configuration conf = getDefaultConfiguration();
    conf.setString(HoodieIndexConfig.BUCKET_INDEX_MIN_NUM_BUCKETS.key(), "1");
    HoodieFlinkWriteClient writeClient = StreamerUtil.createWriteClient(conf);
    Option<String> clusteringInstantOption = writeClient.scheduleClustering(Option.empty());
    Assertions.assertTrue(clusteringInstantOption.isPresent());

    // Upsert data and exception will occur
    Assertions.assertThrows(Exception.class, () -> tableEnv.executeSql(TestSQL.UPDATE_INSERT_T1).await());
  }

  protected Map<String, String> getDefaultConsistentHashingOption() {
    Map<String, String> options = super.getDefaultConsistentHashingOption();
    options.put(FlinkOptions.CLUSTERING_UPDATE_STRATEGY.key(), FlinkRejectUpdateStrategy.class.getName());

    return options;
  }
}
