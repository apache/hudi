/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bucket;

import org.apache.hudi.adapter.OperatorCoordinatorAdapter;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.sink.event.CreatePartitionMetadataEvent;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.util.StreamerUtil.initTableIfNotExists;

/**
 * {@link OperatorCoordinator} for {@link ConsistentBucketAssignFunction}.
 *
 * <p>This coordinator to create hashing metadata for new partition,
 * which could avoid multiple subtasks are trying to initialize metadata for the same partition.
 */
public class ConsistentBucketAssignerCoordinator implements OperatorCoordinatorAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketAssignerCoordinator.class);

  private final Configuration conf;
  private final int bucketNum;
  private final Context context;
  private transient HoodieTable<?, ?, ?, ?> table;
  // The cache of created partitions set to avoid
  private Set<String> createdPartitions;

  /**
   * A single-thread executor to handle all the asynchronous jobs of the coordinator.
   */
  private NonThrownExecutor executor;

  /**
   * Constructs a StreamingSinkOperatorCoordinator.
   *
   * @param conf    The config options
   * @param context The coordinator context
   */
  public ConsistentBucketAssignerCoordinator(
      Configuration conf,
      Context context) {
    this.conf = conf;
    this.bucketNum = conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS);
    this.context = context;
    this.createdPartitions  = ConcurrentHashMap.newKeySet();
  }

  @Override
  public void start() throws Exception {
    // setup classloader for APIs that use reflection without taking ClassLoader param
    // reference: https://stackoverflow.com/questions/1771679/difference-between-threads-context-class-loader-and-normal-classloader
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    // init table, create if not exists.
    HoodieTableMetaClient metaClient = initTableIfNotExists(this.conf);
    HoodieWriteConfig writeConfig = FlinkWriteClients.getHoodieClientConfig(conf, true, false);
    this.table = HoodieFlinkTable.create(writeConfig, HoodieFlinkEngineContext.DEFAULT, metaClient);
    // start the executor
    this.executor = NonThrownExecutor.builder(LOG)
        .exceptionHook((errMsg, t) -> this.context.failJob(new HoodieException(errMsg, t)))
        .waitForTasksFinish(true).build();
  }

  @Override
  public void close() throws Exception {
    // teardown the resource
    if (executor != null) {
      executor.close();
    }
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
    executor.execute(
        () -> {
          try {
            result.complete(new byte[0]);
          } catch (Throwable e) {
            result.completeExceptionally(
                new CompletionException(
                    String.format(
                        "Failed to checkpoint for bucket assigner %s",
                        this.getClass().getSimpleName()),
                    e));
          }
        }, "taking checkpoint %d", checkpointId
    );
  }

  @Override
  public void notifyCheckpointComplete(long l) {
    // no operation
  }

  @Override
  public void resetToCheckpoint(long l, @Nullable byte[] bytes) throws Exception {
    // no operation
  }

  @Override
  public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
    ValidationUtils.checkState(
        operatorEvent instanceof CreatePartitionMetadataEvent,
        "The coordinator can only handle createConsistentBucketMetadataEvent");
    CreatePartitionMetadataEvent event = (CreatePartitionMetadataEvent) operatorEvent;

    executor.execute(
        () -> {
          String partition = event.getPartition();
          LOG.info("Received a create hashing metadata request of the partition {} from subtask #{}", partition, i);
          if (!createdPartitions.contains(partition)) {
            // TODO maybe retry if fail to create metadata for partition
            ConsistentBucketIndexUtils.loadOrCreateMetadata(table, partition, bucketNum);
            createdPartitions.add(partition);
            LOG.info("Success to handle the create hashing metadata request of the partition {} from subtask #{}", partition, i);
          }
        }, "handle create partition metadata event"
    );
  }

  @Override
  public void subtaskReset(int i, long l) {
    // no operation
  }

  @Override
  public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
    // no operation
  }

  @Override
  public void subtaskFailed(int i, @Nullable Throwable throwable) {
    LOG.warn("Reset the event for task [" + i + "]", throwable);
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  @VisibleForTesting
  public Context getContext() {
    return context;
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) throws Exception {
    if (this.executor != null) {
      this.executor.close();
    }
    this.executor = executor;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Provider for {@link ConsistentBucketAssignerCoordinator}.
   */
  public static class Provider implements OperatorCoordinator.Provider {
    private final OperatorID operatorId;
    private final Configuration conf;

    public Provider(OperatorID operatorId, Configuration conf) {
      this.operatorId = operatorId;
      this.conf = conf;
    }

    @Override
    public OperatorID getOperatorId() {
      return this.operatorId;
    }

    @Override
    public OperatorCoordinator create(Context context) {
      return new ConsistentBucketAssignerCoordinator(this.conf, context);
    }
  }
}
