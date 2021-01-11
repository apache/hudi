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

package org.apache.hudi.operator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.HoodieFlinkStreamer;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.common.TaskContextSupplier;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.util.StreamerUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Operator helps to generate globally unique instant, it must be executed in one parallelism. Before generate a new
 * instant , {@link InstantGenerateOperator} will always check whether the last instant has completed. if it is
 * completed, a new instant will be generated immediately, otherwise, wait and check the state of last instant until
 * time out and throw an exception.
 */
public class InstantGenerateOperator extends AbstractStreamOperator<HoodieRecord> implements OneInputStreamOperator<HoodieRecord, HoodieRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(InstantGenerateOperator.class);
  public static final String NAME = "InstantGenerateOperator";
  private static final String UNDERLINE = "_";
  private HoodieFlinkStreamer.Config cfg;
  private HoodieFlinkWriteClient writeClient;
  private SerializableConfiguration serializableHadoopConf;
  private transient FileSystem fs;
  private String latestInstant = "";
  private List<String> latestInstantList = new ArrayList<>(1);
  private transient ListState<String> latestInstantState;
  private Integer retryTimes;
  private Integer retryInterval;
  private transient boolean isMain = false;
  private transient volatile long batchSize = 0;

  @Override
  public void processElement(StreamRecord<HoodieRecord> streamRecord) throws Exception {
    if (streamRecord.getValue() != null) {
      output.collect(streamRecord);
      batchSize++;
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
    isMain = getRuntimeContext().getIndexOfThisSubtask() == 0;

    // get configs from runtimeContext
    cfg = (HoodieFlinkStreamer.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(StreamerUtil.getHadoopConf());

    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, serializableHadoopConf.get());

    if (isMain) {
      // retry times
      retryTimes = Integer.valueOf(cfg.blockRetryTime);

      // retry interval
      retryInterval = Integer.valueOf(cfg.blockRetryInterval);

      TaskContextSupplier taskContextSupplier = new FlinkTaskContextSupplier(null);

      // writeClient
      writeClient = new HoodieFlinkWriteClient(new HoodieFlinkEngineContext(taskContextSupplier), StreamerUtil.getHoodieClientConfig(cfg), true);

      // init table, create it if not exists.
      initTable();
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    String instantGenerateInfoFileName = indexOfThisSubtask + UNDERLINE + checkpointId + UNDERLINE + batchSize;
    Path path = new Path(HoodieTableMetaClient.INSTANT_GENERATE_FOLDER_NAME,instantGenerateInfoFileName);
    // mk generate file by each subtask
    fs.create(path,true);
    LOG.info("subtask [{}] at checkpoint [{}] created generate file [{}]",indexOfThisSubtask,checkpointId,instantGenerateInfoFileName);
    if (isMain) {
      boolean hasData = generateFilePasre(checkpointId);
      // check whether the last instant is completed, if not, wait 10s and then throws an exception
      if (!StringUtils.isNullOrEmpty(latestInstant)) {
        doCheck();
        // last instant completed, set it empty
        latestInstant = "";
      }

      // no data no new instant
      if (hasData) {
        latestInstant = startNewInstant(checkpointId);
      }
    }

  }

  private boolean generateFilePasre(long checkpointId) throws InterruptedException, IOException {
    int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    FileStatus[] fileStatuses = null;
    Path generatePath = new Path(HoodieTableMetaClient.INSTANT_GENERATE_FOLDER_NAME);
    int tryTimes = 1;
    // waiting all subtask create generate file ready
    while (true) {
      Thread.sleep(500L);
      fileStatuses = fs.listStatus(generatePath, new PathFilter() {
        @Override
        public boolean accept(Path pathname) {
          return pathname.getName().contains(UNDERLINE + checkpointId + UNDERLINE);
        }
      });

      // is ready
      if (fileStatuses != null && fileStatuses.length == numberOfParallelSubtasks) {
        break;
      }

      if (tryTimes >= 5) {
        LOG.warn("waiting generate file, checkpointId [{}]",checkpointId);
        tryTimes = 0;
      }
    }

    boolean hasData = false;
    // judge whether has data in this checkpoint and delete tmp file.
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      String name = path.getName();
      // has data
      if (Long.parseLong(name.split(UNDERLINE)[2]) > 0) {
        hasData = true;
        break;
      }
    }

    // delete all tmp file
    fileStatuses = fs.listStatus(generatePath);
    for (FileStatus fileStatus : fileStatuses) {
      fs.delete(fileStatus.getPath());
    }

    return hasData;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    isMain = getRuntimeContext().getIndexOfThisSubtask() == 0;
    if (isMain) {
      // instantState
      ListStateDescriptor<String> latestInstantStateDescriptor = new ListStateDescriptor<String>("latestInstant", String.class);
      latestInstantState = context.getOperatorStateStore().getListState(latestInstantStateDescriptor);

      if (context.isRestored()) {
        Iterator<String> latestInstantIterator = latestInstantState.get().iterator();
        latestInstantIterator.forEachRemaining(x -> latestInstant = x);
        LOG.info("InstantGenerateOperator initializeState get latestInstant [{}]", latestInstant);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext functionSnapshotContext) throws Exception {
    LOG.info("Update latest instant [{}] records size [{}]", latestInstant,batchSize);
    batchSize = 0;
    if (isMain) {
      if (latestInstantList.isEmpty()) {
        latestInstantList.add(latestInstant);
      } else {
        latestInstantList.set(0, latestInstant);
      }
      latestInstantState.update(latestInstantList);
    }
  }

  /**
   * Create a new instant.
   *
   * @param checkpointId
   */
  private String startNewInstant(long checkpointId) {
    String newTime = writeClient.startCommit();
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
      StringBuffer sb = new StringBuffer();
      if (rollbackPendingCommits.contains(latestInstant)) {
        rollbackPendingCommits.forEach(x -> sb.append(x).append(","));
        LOG.warn("Latest transaction [{}] is not completed! unCompleted transaction:[{}],try times [{}]", latestInstant, sb.toString(), tryTimes);
        TimeUnit.SECONDS.sleep(retryInterval);
        rollbackPendingCommits = writeClient.getInflightsAndRequestedInstants(commitType);
      } else {
        LOG.warn("Latest transaction [{}] is completed! Completed transaction, try times [{}]", latestInstant, tryTimes);
        return;
      }
    }
    throw new InterruptedException(String.format("Last instant costs more than %s second, stop task now", retryTimes * retryInterval));
  }


  /**
   * Create table if not exists.
   */
  private void initTable() throws IOException {
    if (!fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient.initTableType(new Configuration(serializableHadoopConf.get()), cfg.targetBasePath,
          HoodieTableType.valueOf(cfg.tableType), cfg.targetTableName, "archived", cfg.payloadClassName, 1);
      LOG.info("Table initialized");
    } else {
      LOG.info("Table already [{}/{}] exists, do nothing here", cfg.targetBasePath, cfg.targetTableName);
    }
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
}
