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

package org.apache.hudi.sink;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Operator helps to generate globally unique instant. Before generate a new instant {@link InstantGenerateOperator}
 * will always check whether the last instant has completed. if it is completed and has records flows in, a new instant
 * will be generated immediately, otherwise, wait and check the state of last instant until time out and throw an exception.
 */
public class InstantGenerateOperator extends AbstractStreamOperator<HoodieRecord> implements OneInputStreamOperator<HoodieRecord, HoodieRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(InstantGenerateOperator.class);
  public static final String NAME = "InstantGenerateOperator";

  private FlinkStreamerConfig cfg;
  private HoodieFlinkWriteClient writeClient;
  private SerializableConfiguration serializableHadoopConf;
  private transient FileSystem fs;
  private String latestInstant = "";
  private List<String> latestInstantList = new ArrayList<>(1);
  private transient ListState<String> latestInstantState;
  private Integer retryTimes;
  private Integer retryInterval;
  private static final String DELIMITER = "_";
  private static final String INSTANT_MARKER_FOLDER_NAME = ".instant_marker";
  private transient boolean isMain = false;
  private AtomicLong recordCounter = new AtomicLong(0);
  private StreamingRuntimeContext runtimeContext;
  private int indexOfThisSubtask;

  @Override
  public void processElement(StreamRecord<HoodieRecord> streamRecord) throws Exception {
    if (streamRecord.getValue() != null) {
      output.collect(streamRecord);
      recordCounter.incrementAndGet();
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    // get configs from runtimeContext
    cfg = (FlinkStreamerConfig) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // retry times
    retryTimes = Integer.valueOf(cfg.instantRetryTimes);

    // retry interval
    retryInterval = Integer.valueOf(cfg.instantRetryInterval);

    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(StreamerUtil.getHadoopConf());

    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, serializableHadoopConf.get());

    if (isMain) {
      TaskContextSupplier taskContextSupplier = new FlinkTaskContextSupplier(null);

      // writeClient
      writeClient = new HoodieFlinkWriteClient(new HoodieFlinkEngineContext(taskContextSupplier), StreamerUtil.getHoodieClientConfig(cfg));

      // init table, create it if not exists.
      StreamerUtil.initTableIfNotExists(FlinkOptions.fromStreamerConfig(cfg));

      // create instant marker directory
      createInstantMarkerDir();
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    String instantMarkerFileName = String.format("%d%s%d%s%d", indexOfThisSubtask, DELIMITER, checkpointId, DELIMITER, recordCounter.get());
    Path path = generateCurrentMakerFilePath(instantMarkerFileName);
    // create marker file
    fs.create(path, true);
    LOG.info("Subtask [{}] at checkpoint [{}] created marker file [{}]", indexOfThisSubtask, checkpointId, instantMarkerFileName);
    if (isMain) {
      // check whether the last instant is completed, will try specific times until an exception is thrown
      if (!StringUtils.isNullOrEmpty(latestInstant)) {
        doCheck();
        // last instant completed, set it empty
        latestInstant = "";
      }
      boolean receivedDataInCurrentCP = checkReceivedData(checkpointId);
      // no data no new instant
      if (receivedDataInCurrentCP) {
        latestInstant = startNewInstant(checkpointId);
      }
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    runtimeContext = getRuntimeContext();
    indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
    isMain = indexOfThisSubtask == 0;

    if (isMain) {
      // instantState
      ListStateDescriptor<String> latestInstantStateDescriptor = new ListStateDescriptor<>("latestInstant", String.class);
      latestInstantState = context.getOperatorStateStore().getListState(latestInstantStateDescriptor);

      if (context.isRestored()) {
        Iterator<String> latestInstantIterator = latestInstantState.get().iterator();
        latestInstantIterator.forEachRemaining(x -> latestInstant = x);
        LOG.info("Restoring the latest instant [{}] from the state", latestInstant);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext functionSnapshotContext) throws Exception {
    long checkpointId = functionSnapshotContext.getCheckpointId();
    long recordSize = recordCounter.get();
    if (isMain) {
      LOG.info("Update latest instant [{}] records size [{}] checkpointId [{}]", latestInstant, recordSize, checkpointId);
      if (latestInstantList.isEmpty()) {
        latestInstantList.add(latestInstant);
      } else {
        latestInstantList.set(0, latestInstant);
      }
      latestInstantState.update(latestInstantList);
    } else {
      LOG.info("Task instance {} received {} records in checkpoint [{}]", indexOfThisSubtask, recordSize, checkpointId);
    }
    recordCounter.set(0);
  }

  /**
   * Create a new instant.
   *
   * @param checkpointId
   */
  private String startNewInstant(long checkpointId) {
    String newTime = writeClient.startCommit();
    this.writeClient.transitionRequestedToInflight(this.cfg.tableType, newTime);
    LOG.info("create instant [{}], at checkpoint [{}]", newTime, checkpointId);
    return newTime;
  }

  /**
   * Check the status of last instant.
   */
  private void doCheck() throws InterruptedException {
    // query the requested and inflight commit/deltacommit instants
    String commitType = cfg.tableType.equals(HoodieTableType.COPY_ON_WRITE.name()) ? HoodieTimeline.COMMIT_ACTION : HoodieTimeline.DELTA_COMMIT_ACTION;
    LOG.info("Query latest instant [{}]", latestInstant);
    List<String> rollbackPendingCommits = writeClient.getInflightsAndRequestedInstants(commitType);
    int tryTimes = 0;
    while (tryTimes < retryTimes) {
      tryTimes++;
      StringBuilder sb = new StringBuilder();
      if (rollbackPendingCommits.contains(latestInstant)) {
        rollbackPendingCommits.forEach(x -> sb.append(x).append(","));
        LOG.warn("Latest transaction [{}] is not completed! unCompleted transaction:[{}],try times [{}]", latestInstant, sb, tryTimes);
        TimeUnit.SECONDS.sleep(retryInterval);
        rollbackPendingCommits = writeClient.getInflightsAndRequestedInstants(commitType);
      } else {
        LOG.warn("Latest transaction [{}] is completed! Completed transaction, try times [{}]", latestInstant, tryTimes);
        return;
      }
    }
    throw new InterruptedException(String.format("Last instant costs more than %s second, stop task now", retryTimes * retryInterval));
  }

  @Override
  public void close() throws Exception {
    if (writeClient != null) {
      writeClient.close();
    }
    if (fs != null) {
      fs.close();
    }
  }

  private boolean checkReceivedData(long checkpointId) throws InterruptedException, IOException {
    int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
    FileStatus[] fileStatuses;
    Path instantMarkerPath = generateCurrentMakerDirPath();
    // waiting all subtask create marker file ready
    while (true) {
      Thread.sleep(500L);
      fileStatuses = fs.listStatus(instantMarkerPath, new PathFilter() {
        @Override
        public boolean accept(Path pathname) {
          return pathname.getName().contains(String.format("%s%d%s", DELIMITER, checkpointId, DELIMITER));
        }
      });

      // is ready
      if (fileStatuses != null && fileStatuses.length == numberOfParallelSubtasks) {
        break;
      }
    }

    boolean receivedData = false;
    // check whether has data in this checkpoint and delete maker file.
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      String name = path.getName();
      // has data
      if (Long.parseLong(name.split(DELIMITER)[2]) > 0) {
        receivedData = true;
        break;
      }
    }

    // delete all marker file
    cleanMarkerDir(instantMarkerPath);

    return receivedData;
  }

  private void createInstantMarkerDir() throws IOException {
    // Always create instantMarkerFolder which is needed for InstantGenerateOperator
    final Path instantMarkerFolder = new Path(new Path(cfg.targetBasePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME), INSTANT_MARKER_FOLDER_NAME);
    if (!fs.exists(instantMarkerFolder)) {
      fs.mkdirs(instantMarkerFolder);
    } else {
      // Clean marker dir.
      cleanMarkerDir(instantMarkerFolder);
    }
  }

  private void cleanMarkerDir(Path instantMarkerFolder) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(instantMarkerFolder);
    for (FileStatus fileStatus : fileStatuses) {
      fs.delete(fileStatus.getPath(), true);
    }
  }

  private Path generateCurrentMakerDirPath() {
    Path auxPath = new Path(cfg.targetBasePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    return new Path(auxPath, INSTANT_MARKER_FOLDER_NAME);
  }

  private Path generateCurrentMakerFilePath(String instantMarkerFileName) {
    return new Path(generateCurrentMakerDirPath(), instantMarkerFileName);
  }
}
