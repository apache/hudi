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

package org.apache.hudi.source;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link MergeOnReadInputSplit}
 * , it is responsible for:
 *
 * <ol>
 *     <li>Monitoring a user-provided hoodie table path.</li>
 *     <li>Deciding which files(or split) should be further read and processed.</li>
 *     <li>Creating the {@link MergeOnReadInputSplit splits} corresponding to those files.</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link StreamReadOperator}
 * which can have parallelism greater than one.
 *
 * <p><b>IMPORTANT NOTE: </b> Splits are forwarded downstream for reading in ascending instant commits time order,
 * in each downstream task, the splits are also read in receiving sequence. We do not ensure split consuming sequence
 * among the downstream tasks.
 */
public class StreamReadMonitoringFunction
    extends RichSourceFunction<MergeOnReadInputSplit> implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(StreamReadMonitoringFunction.class);

  private static final long serialVersionUID = 1L;

  /**
   * The path to monitor.
   */
  private final Path path;

  /**
   * The interval between consecutive path scans.
   */
  private final long interval;

  private transient Object checkpointLock;

  private volatile boolean isRunning = true;

  private String issuedInstant;

  private transient ListState<String> instantState;

  private final Configuration conf;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private HoodieTableMetaClient metaClient;

  private final IncrementalInputSplits incrementalInputSplits;

  public StreamReadMonitoringFunction(
      Configuration conf,
      Path path,
      long maxCompactionMemoryInBytes,
      @Nullable Set<String> requiredPartitionPaths) {
    this.conf = conf;
    this.path = path;
    this.interval = conf.getInteger(FlinkOptions.READ_STREAMING_CHECK_INTERVAL);
    this.incrementalInputSplits = IncrementalInputSplits.builder()
        .conf(conf)
        .path(path)
        .maxCompactionMemoryInBytes(maxCompactionMemoryInBytes)
        .requiredPartitions(requiredPartitionPaths)
        .skipCompaction(conf.getBoolean(FlinkOptions.READ_STREAMING_SKIP_COMPACT))
        .build();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

    ValidationUtils.checkState(this.instantState == null,
        "The " + getClass().getSimpleName() + " has already been initialized.");

    this.instantState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "file-monitoring-state",
            StringSerializer.INSTANCE
        )
    );

    if (context.isRestored()) {
      LOG.info("Restoring state for the class {} with table {} and base path {}.",
          getClass().getSimpleName(), conf.getString(FlinkOptions.TABLE_NAME), path);

      List<String> retrievedStates = new ArrayList<>();
      for (String entry : this.instantState.get()) {
        retrievedStates.add(entry);
      }

      ValidationUtils.checkArgument(retrievedStates.size() <= 1,
          getClass().getSimpleName() + " retrieved invalid state.");

      if (retrievedStates.size() == 1 && issuedInstant != null) {
        // this is the case where we have both legacy and new state.
        // the two should be mutually exclusive for the operator, thus we throw the exception.

        throw new IllegalArgumentException(
            "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

      } else if (retrievedStates.size() == 1) {
        this.issuedInstant = retrievedStates.get(0);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} retrieved a issued instant of time {} for table {} with path {}.",
              getClass().getSimpleName(), issuedInstant, conf.get(FlinkOptions.TABLE_NAME), path);
        }
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.hadoopConf = FlinkOptions.getHadoopConf(parameters);
  }

  @Override
  public void run(SourceFunction.SourceContext<MergeOnReadInputSplit> context) throws Exception {
    checkpointLock = context.getCheckpointLock();
    while (isRunning) {
      synchronized (checkpointLock) {
        monitorDirAndForwardSplits(context);
      }
      TimeUnit.SECONDS.sleep(interval);
    }
  }

  @Nullable
  private HoodieTableMetaClient getOrCreateMetaClient() {
    if (this.metaClient != null) {
      return this.metaClient;
    }
    if (StreamerUtil.tableExists(this.path.toString(), hadoopConf)) {
      this.metaClient = StreamerUtil.createMetaClient(this.path.toString(), hadoopConf);
      return this.metaClient;
    }
    // fallback
    return null;
  }

  @VisibleForTesting
  public void monitorDirAndForwardSplits(SourceContext<MergeOnReadInputSplit> context) {
    HoodieTableMetaClient metaClient = getOrCreateMetaClient();
    if (metaClient == null) {
      // table does not exist
      return;
    }
    IncrementalInputSplits.Result result =
        incrementalInputSplits.inputSplits(metaClient, this.hadoopConf, this.issuedInstant);
    if (result.isEmpty()) {
      // no new instants, returns early
      return;
    }

    for (MergeOnReadInputSplit split : result.getInputSplits()) {
      context.collect(split);
    }
    // update the issues instant time
    this.issuedInstant = result.getEndInstant();
    LOG.info("\n"
            + "------------------------------------------------------------\n"
            + "---------- consumed to instant: {}\n"
            + "------------------------------------------------------------",
        this.issuedInstant);
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (checkpointLock != null) {
      synchronized (checkpointLock) {
        issuedInstant = null;
        isRunning = false;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Closed File Monitoring Source for path: " + path + ".");
    }
  }

  @Override
  public void cancel() {
    if (checkpointLock != null) {
      // this is to cover the case where cancel() is called before the run()
      synchronized (checkpointLock) {
        issuedInstant = null;
        isRunning = false;
      }
    } else {
      issuedInstant = null;
      isRunning = false;
    }
  }

  // -------------------------------------------------------------------------
  //  Checkpointing
  // -------------------------------------------------------------------------

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    this.instantState.clear();
    if (this.issuedInstant != null) {
      this.instantState.add(this.issuedInstant);
    }
  }
}
