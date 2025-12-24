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

package org.apache.hudi.sink.clustering;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.hudi.adapter.MaskingOutputAdapter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.FsCacheCleanUtil;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusteringPlanReadFooterOperator extends AbstractStreamOperator<ClusteringFileEvent>
    implements OneInputStreamOperator<ClusteringFileEvent, ClusteringFileEvent> {

  protected static final Logger LOG = LoggerFactory.getLogger(ClusteringPlanReadFooterOperator.class);
  private final Configuration conf;

  public static final BaseFileUtils PARQUET_UTILS = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  private org.apache.hadoop.conf.Configuration hadoopConf;
  private NonThrownExecutor executor;
  private Cache<String, HoodieLSMLogFile> cache; //  partitionPath + logFile.getFileName()
  private transient StreamRecordCollector<ClusteringFileEvent> collector;

  public ClusteringPlanReadFooterOperator(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ClusteringFileEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.hadoopConf = FlinkTables.createTable(conf, getRuntimeContext()).getHadoopConf();
    this.executor = NonThrownExecutor.builder(LOG).build();
    this.cache = Caffeine.newBuilder().weakValues().maximumSize(1024).build();
    this.collector = new StreamRecordCollector<>(output);
  }

  @Override
  public void processElement(StreamRecord<ClusteringFileEvent> record) {
    executor.execute(() -> {
      ClusteringFileEvent event = record.getValue();
      if (event instanceof ClusteringEndFileEvent) {
        collector.collect(event);
      } else {
        HoodieLSMLogFile logFile = event.getLogFile();
        String fileID = event.getFileID();
        String partitionPath = event.getPartitionPath();
        String cacheKey = partitionPath + logFile.getFileName();
        HoodieLSMLogFile cacheLog = cache.getIfPresent(cacheKey);
        if (cacheLog != null) {
          collector.collect(new ClusteringFileEvent(cacheLog, fileID, partitionPath, event.getFileStatus()));
        } else {
          String[] minMax = PARQUET_UTILS.readMinMaxRecordKeys(hadoopConf, logFile.getPath());
          logFile.setMin(minMax[0]);
          logFile.setMax(minMax[1]);
          cache.put(cacheKey, logFile);
          collector.collect(new ClusteringFileEvent(logFile, fileID, partitionPath, event.getFileStatus()));
        }
      }
    }, "Read parquet footer");
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<ClusteringFileEvent>> output) {
    this.output = output;
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // no op
  }

  @Override
  public void close() throws Exception {
    FsCacheCleanUtil.cleanChubaoFsCacheIfNecessary();
  }
}
