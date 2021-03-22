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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieFlinkStreamerException;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Function helps to execute commit operation. this operation should be executed only once.
 */
public class CommitSink extends RichSinkFunction<Tuple3<String, List<WriteStatus>, Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(CommitSink.class);
  /**
   * Job conf.
   */
  private FlinkStreamerConfig cfg;

  /**
   * Write client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Write result buffer.
   */
  private Map<String, List<List<WriteStatus>>> bufferedWriteStatus = new HashMap<>();

  /**
   * Parallelism of this job.
   */
  private Integer writeParallelSize = 0;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // Get configs from runtimeContext
    cfg = (FlinkStreamerConfig) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    writeParallelSize = getRuntimeContext().getExecutionConfig().getParallelism();

    // writeClient
    writeClient = new HoodieFlinkWriteClient<>(new HoodieFlinkEngineContext(new FlinkTaskContextSupplier(null)), StreamerUtil.getHoodieClientConfig(cfg));
  }

  @Override
  public void invoke(Tuple3<String, List<WriteStatus>, Integer> writeStatues, Context context) {
    LOG.info("Receive records, instantTime = [{}], subtaskId = [{}], WriteStatus size = [{}]", writeStatues.f0, writeStatues.f2, writeStatues.f1.size());
    try {
      if (bufferedWriteStatus.containsKey(writeStatues.f0)) {
        bufferedWriteStatus.get(writeStatues.f0).add(writeStatues.f1);
      } else {
        List<List<WriteStatus>> oneBatchData = new ArrayList<>(writeParallelSize);
        oneBatchData.add(writeStatues.f1);
        bufferedWriteStatus.put(writeStatues.f0, oneBatchData);
      }
      // check and commit
      checkAndCommit(writeStatues.f0);
    } catch (Exception e) {
      throw new HoodieFlinkStreamerException("Invoke sink error", e);
    }
  }

  /**
   * Check and commit if all subtask completed.
   *
   * @throws Exception
   */
  private void checkAndCommit(String instantTime) throws Exception {
    if (bufferedWriteStatus.get(instantTime).size() == writeParallelSize) {
      LOG.info("Instant [{}] process complete, start commit！", instantTime);
      doCommit(instantTime);
      bufferedWriteStatus.clear();
      LOG.info("Instant [{}] commit completed!", instantTime);
    } else {
      LOG.info("Instant [{}], can not commit yet, subtask completed : [{}/{}]", instantTime, bufferedWriteStatus.get(instantTime).size(), writeParallelSize);
    }
  }

  private void doCommit(String instantTime) {
    // get the records to commit
    List<WriteStatus> writeResults = bufferedWriteStatus.get(instantTime).stream().flatMap(Collection::stream).collect(Collectors.toList());

    // commit and rollback
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
    boolean hasErrors = totalErrorRecords > 0;

    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (hasErrors) {
        LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = writeClient.commit(instantTime, writeResults, Option.of(checkpointCommitMetadata));
      if (success) {
        LOG.warn("Commit " + instantTime + " successful!");
      } else {
        LOG.warn("Commit " + instantTime + " failed!");
        throw new HoodieException("Commit " + instantTime + " failed!");
      }
    } else {
      LOG.error("Streamer sync found errors when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("Printing out the top 100 errors");
      writeResults.stream().filter(WriteStatus::hasErrors).limit(100).forEach(ws -> {
        LOG.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " is " + value));
        }
      });
      // Rolling back instant
      writeClient.rollback(instantTime);
      throw new HoodieException("Commit " + instantTime + " failed and rolled-back !");
    }
  }
}