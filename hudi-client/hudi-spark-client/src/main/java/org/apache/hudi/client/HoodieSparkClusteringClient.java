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
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Async clustering client for Spark datasource.
 */
public class HoodieSparkClusteringClient<T> extends
    BaseClusterer<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkClusteringClient.class);

  public HoodieSparkClusteringClient(
      BaseHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> clusteringClient) {
    super(clusteringClient);
  }

  @Override
  public void cluster(HoodieInstant instant) throws IOException {
    LOG.info("Executing clustering instance " + instant);
    SparkRDDWriteClient<T> writeClient = (SparkRDDWriteClient<T>) clusteringClient;
    Option<HoodieCommitMetadata> commitMetadata = writeClient.cluster(instant.getTimestamp(), true).getCommitMetadata();
    Stream<HoodieWriteStat> hoodieWriteStatStream = commitMetadata.get().getPartitionToWriteStats().entrySet().stream().flatMap(e ->
            e.getValue().stream());
    long errorsCount = hoodieWriteStatStream.mapToLong(HoodieWriteStat::getTotalWriteErrors).sum();
    if (errorsCount > 0) {
      // TODO: Should we treat this fatal and throw exception?
      LOG.error("Clustering for instant (" + instant + ") failed with write errors");
    }
  }
}
