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

package org.apache.hudi.sink.bucket;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ConsistentBucketInitCoordinator implements OperatorCoordinator {

  public static class ConsistentBucketInitializationEvent implements OperatorEvent {
    public String partition;

    public ConsistentBucketInitializationEvent(String partition) {
      this.partition = partition;
    }
  }

  private final Configuration conf;
  private final int defaultNumBuckets;
  private Set<String> metadataInitialized;
  private transient HoodieTableMetaClient metaClient;

  public ConsistentBucketInitCoordinator(Configuration conf, Context context) {
    this.conf = conf;
    this.defaultNumBuckets = conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
  }

  @Override
  public void start() throws Exception {
    this.metadataInitialized = new HashSet<>();
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
    ValidationUtils.checkState(event instanceof ConsistentBucketInitializationEvent,
        "The coordinator can only handle WriteMetaEvent");

    String partition = ((ConsistentBucketInitializationEvent) event).partition;
    if (this.metadataInitialized.contains(partition)) {
      return;
    }

    if (metaClient == null) {
      this.metaClient = StreamerUtil.createMetaClient(this.conf);
    }

    ConsistentBucketIndexUtils.loadOrCreateMetadata(metaClient, partition, this.defaultNumBuckets);
    this.metadataInitialized.add(partition);
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
    resultFuture.complete(new byte[0]);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {

  }

  @Override
  public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {

  }

  @Override
  public void subtaskFailed(int subtask, @Nullable Throwable reason) {

  }

  @Override
  public void subtaskReset(int subtask, long checkpointId) {

  }

  @Override
  public void subtaskReady(int subtask, SubtaskGateway gateway) {

  }
}
