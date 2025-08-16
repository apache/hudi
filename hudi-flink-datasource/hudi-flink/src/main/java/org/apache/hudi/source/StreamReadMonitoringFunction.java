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

import org.apache.hudi.adapter.RichSourceFunctionAdapter;
import org.apache.hudi.adapter.SourceFunctionAdapter;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.metrics.FlinkStreamReadMetrics;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
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
    extends RichSourceFunctionAdapter<MergeOnReadInputSplit> implements CheckpointedFunction {
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

  /**
   * Flag saying whether the change log capture is enabled.
   */
  private final boolean cdcEnabled;

  private transient Object checkpointLock;

  private volatile boolean isRunning = true;

  private String issuedInstant;

  private String issuedOffset;

  private transient ListState<String> instantState;

  private final Configuration conf;

  private HoodieTableMetaClient metaClient;

  private final IncrementalInputSplits incrementalInputSplits;

  private transient FlinkStreamReadMetrics readMetrics;

  public StreamReadMonitoringFunction(
      Configuration conf,
      Path path,
      RowType rowType,
      long maxCompactionMemoryInBytes,
      @Nullable PartitionPruners.PartitionPruner partitionPruner) {
    this.conf = conf;
    this.path = path;
    this.interval = conf.get(FlinkOptions.READ_STREAMING_CHECK_INTERVAL);
    this.cdcEnabled = conf.get(FlinkOptions.CDC_ENABLED);
    this.incrementalInputSplits = IncrementalInputSplits.builder()
        .conf(conf)
        .path(path)
        .rowType(rowType)
        .maxCompactionMemoryInBytes(maxCompactionMemoryInBytes)
        .partitionPruner(partitionPruner)
        .skipCompaction(conf.get(FlinkOptions.READ_STREAMING_SKIP_COMPACT))
        .skipClustering(conf.get(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING))
        .skipInsertOverwrite(conf.get(FlinkOptions.READ_STREAMING_SKIP_INSERT_OVERWRITE))
        .build();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

    ValidationUtils.checkState(this.instantState == null,
        "The " + getClass().getSimpleName() + " has already been initialized.");

    registerMetrics();

    this.instantState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "file-monitoring-state",
            StringSerializer.INSTANCE
        )
    );

    if (context.isRestored()) {
      LOG.info("Restoring state for the class {} with table {} and base path {}.",
          getClass().getSimpleName(), conf.get(FlinkOptions.TABLE_NAME), path);

      List<String> retrievedStates = new ArrayList<>();
      for (String entry : this.instantState.get()) {
        retrievedStates.add(entry);
      }

      ValidationUtils.checkArgument(retrievedStates.size() <= 2,
          getClass().getSimpleName() + " retrieved invalid state.");

      if (retrievedStates.size() == 1 && issuedInstant != null) {
        // this is the case where we have both legacy and new state.
        // the two should be mutually exclusive for the operator, thus we throw the exception.

        throw new IllegalArgumentException(
            "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

      } else if (retrievedStates.size() == 1) {
        // for forward compatibility
        this.issuedInstant = retrievedStates.get(0);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} retrieved an issued instant of time {} for table {} with path {}.",
              getClass().getSimpleName(), issuedInstant, conf.get(FlinkOptions.TABLE_NAME), path);
        }
      } else if (retrievedStates.size() == 2) {
        this.issuedInstant = retrievedStates.get(0);
        this.issuedOffset = retrievedStates.get(1);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} retrieved an issued instant of time [{}, {}] for table {} with path {}.",
              getClass().getSimpleName(), issuedInstant, issuedOffset, conf.get(FlinkOptions.TABLE_NAME), path);
        }
      }
    }
  }

  @Override
  public void run(SourceFunctionAdapter.SourceContext<MergeOnReadInputSplit> context) throws Exception {
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
    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    if (StreamerUtil.tableExists(this.path.toString(), hadoopConf)) {
      this.metaClient = StreamerUtil.createMetaClient(this.path.toString(), hadoopConf);
      return this.metaClient;
    }
    // fallback
    return null;
  }

  @VisibleForTesting
  public void monitorDirAndForwardSplits(SourceFunctionAdapter.SourceContext<MergeOnReadInputSplit> context) {
    HoodieTableMetaClient metaClient = getOrCreateMetaClient();
    if (metaClient == null) {
      // table does not exist
      return;
    }
    IncrementalInputSplits.Result result =
        incrementalInputSplits.inputSplits(metaClient, this.issuedOffset, this.cdcEnabled);

    if (result.isEmpty() && StringUtils.isNullOrEmpty(result.getEndInstant())) {
      // no new instants, returns early
      LOG.warn("No new instants to read for current run.");
      return;
    }

    for (MergeOnReadInputSplit split : result.getInputSplits()) {
      context.collect(split);
    }

    // update the issues instant time
    this.issuedInstant = result.getEndInstant();
    this.issuedOffset = result.getOffset();
    LOG.info("\n"
            + "------------------------------------------------------------\n"
            + "---------- table: {}\n"
            + "---------- consumed to instant: {}\n"
            + "------------------------------------------------------------",
        conf.get(FlinkOptions.TABLE_NAME), this.issuedInstant);
    if (result.isEmpty()) {
      LOG.warn("No new files to read for current run.");
    }
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

    LOG.debug("Closed File Monitoring Source for path: {}.", path);
  }

  @Override
  public void cancel() {
    if (checkpointLock != null) {
      // this is to cover the case where cancel() is called before the run()
      synchronized (checkpointLock) {
        isRunning = false;
      }
    } else {
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
      this.readMetrics.setIssuedInstant(this.issuedInstant);
    }
    if (this.issuedOffset != null) {
      this.instantState.add(this.issuedOffset);
    }
  }

  private void registerMetrics() {
    MetricGroup metrics = getRuntimeContext().getMetricGroup();
    readMetrics = new FlinkStreamReadMetrics(metrics);
    readMetrics.registerMetrics();
  }

  public String getIssuedOffset() {
    return issuedOffset;
  }

}
