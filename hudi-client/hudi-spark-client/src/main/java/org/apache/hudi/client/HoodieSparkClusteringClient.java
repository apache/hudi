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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Async clustering client for Spark datasource.
 */
@Slf4j
public class HoodieSparkClusteringClient<T> extends
    BaseClusterer<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public HoodieSparkClusteringClient(
      BaseHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> clusteringClient) {
    super(clusteringClient);
  }

  @Override
  public void cluster(String instantTime) throws IOException {
    log.info("Executing clustering instance {}", instantTime);
    SparkRDDWriteClient<T> writeClient = (SparkRDDWriteClient<T>) clusteringClient;
    Option<HoodieCommitMetadata> commitMetadata = writeClient.cluster(instantTime).getCommitMetadata();
    Stream<HoodieWriteStat> hoodieWriteStatStream = commitMetadata.get().getPartitionToWriteStats().entrySet().stream().flatMap(e ->
            e.getValue().stream());
    long errorsCount = hoodieWriteStatStream.mapToLong(HoodieWriteStat::getTotalWriteErrors).sum();
    if (errorsCount > 0) {
      // TODO: Should we treat this fatal and throw exception?
      log.error("Clustering for instant ({}) failed with write errors", instantTime);
    }
  }
}
