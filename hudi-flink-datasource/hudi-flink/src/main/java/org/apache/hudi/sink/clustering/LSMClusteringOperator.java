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

import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.hudi.adapter.MaskingOutputAdapter;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.lsm.RecordReader;
import org.apache.hudi.io.storage.row.LSMHoodieRowDataCreateHandle;
import org.apache.hudi.metrics.FlinkClusteringMetrics;
import org.apache.hudi.metrics.enums.FlinkClusteringMetricEnum;
import org.apache.hudi.sink.bulk.BulkInsertWriterHelper;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.mor.lsm.FlinkLsmUtils;
import org.apache.hudi.table.format.mor.lsm.LsmMergeIterator;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.FsCacheCleanUtil;

import org.apache.avro.Schema;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.LSM_CLUSTERING_OUT_PUT_LEVEL;

/**
 * Operator to execute the actual clustering task assigned by the clustering plan task.
 * In order to execute scalable, the input should shuffle by the clustering event {@link ClusteringPlanEvent}.
 */
public class LSMClusteringOperator extends TableStreamOperator<ClusteringCommitEvent> implements
    OneInputStreamOperator<ClusteringPlanEvent, ClusteringCommitEvent>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(LSMClusteringOperator.class);

  private final Configuration conf;
  private final RowType rowType;
  private DataType rowDataType;
  private int taskID;
  private transient HoodieWriteConfig writeConfig;
  private transient Schema schema;
  private transient Schema readerSchema;
  private transient int[] requiredPos;
  private transient HoodieFlinkWriteClient writeClient;
  private transient StreamRecordCollector<ClusteringCommitEvent> collector;
  private transient ClosableIterator<RowData> iterator;
  private int spillTreshold;

  /**
   * Whether to execute clustering asynchronously.
   */
  private final boolean asyncClustering;

  /**
   * Executor service to execute the clustering task.
   */
  private transient NonThrownExecutor executor;

  /**
   * clustering metric
   */
  private transient FlinkClusteringMetrics clusteringMetrics;

  private transient HoodieTableMetaClient metaClient;

  public LSMClusteringOperator(Configuration conf, RowType rowType) {
    // copy a conf let following modification not to impact the global conf
    this.conf = new Configuration(conf);
    this.conf.setString(FlinkOptions.OPERATION.key(), WriteOperationType.CLUSTER.value());
    this.rowType = BulkInsertWriterHelper.addMetadataFields(rowType, false);
    this.asyncClustering = OptionsResolver.needsAsyncClustering(conf);
    this.spillTreshold = this.conf.getInteger(FlinkOptions.LSM_SORT_MERGE_SPILL_THRESHOLD);

    // enable parquet bloom filter for record key fields
    this.conf.setBoolean(HoodieStorageConfig.PARQUET_RECORDKEY_BLOOM_FILTER_ENABLED.key(),
        this.conf.getBoolean(FlinkOptions.PARQUET_RECORDKEY_CLUSTERING_BLOOM_FILTER_ENABLED));
  }

  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<ClusteringCommitEvent>> output) {
    super.setup(containingTask, config, new MaskingOutputAdapter<>(output));
  }

  @Override
  public void open() throws Exception {
    super.open();
    registerMetrics();
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.writeConfig = FlinkWriteClients.getHoodieClientConfig(this.conf);
    this.writeClient = FlinkWriteClients.createWriteClient(conf, getRuntimeContext());
    this.metaClient = writeClient.getHoodieTable().getMetaClient();
    this.schema = AvroSchemaCache.intern(AvroSchemaConverter.convertToSchema(rowType));
    this.rowDataType = AvroSchemaConverter.convertToDataType(schema);
    this.readerSchema = this.schema;
    this.requiredPos = getRequiredPositions();
    if (this.asyncClustering) {
      this.executor = NonThrownExecutor.builder(LOG).build();
    }
    this.collector = new StreamRecordCollector<>(output);
  }

  @Override
  public void processElement(StreamRecord<ClusteringPlanEvent> element) throws Exception {
    ClusteringPlanEvent event = element.getValue();
    final String instantTime = event.getClusteringInstantTime();
    if (this.asyncClustering) {
      // executes the compaction task asynchronously to not block the checkpoint barrier propagate.
      executor.execute(
          () -> doClustering(instantTime, event),
          (errMsg, t) -> collector.collect(new ClusteringCommitEvent(instantTime, taskID)),
          "Execute clustering for instant %s from task %d", instantTime, taskID);
    } else {
      // executes the clustering task synchronously for batch mode.
      LOG.info("Execute clustering for instant {} from task {}", instantTime, taskID);
      doClustering(instantTime, event);
    }
  }

  @Override
  public void close() throws Exception {
    if (null != this.executor) {
      this.executor.close();
    }
    if (this.writeClient != null) {
      this.writeClient.close();
      this.writeClient = null;
    }
    if (this.clusteringMetrics != null) {
      this.clusteringMetrics.shutDown();
    }
    if (iterator != null) {
      iterator.close();
      iterator = null;
    }
    FsCacheCleanUtil.cleanChubaoFsCacheIfNecessary();
  }

  /**
   * End input action for batch source.
   */
  public void endInput() {
    FsCacheCleanUtil.cleanChubaoFsCacheIfNecessary();
  }

  private void doClustering(String instantTime, ClusteringPlanEvent event) throws Exception {
    final ClusteringGroupInfo clusteringGroupInfo = event.getClusteringGroupInfo();
    Map<String, String> meta = clusteringGroupInfo.getExtraMeta();
    String outputLevel = meta.get(LSM_CLUSTERING_OUT_PUT_LEVEL);
    String partitionPath = clusteringGroupInfo.getOperations().get(0).getPartitionPath();
    String fileID = FSUtils.getFileIdFromLogPath(new Path(clusteringGroupInfo.getOperations().get(0).getDataFilePath()));

    HoodieFlinkTable table = writeClient.getHoodieTable();
    LSMHoodieRowDataCreateHandle rowCreateHandle = new LSMHoodieRowDataCreateHandle(this.writeClient.getHoodieTable(),
        this.writeClient.getConfig(), partitionPath, fileID, instantTime,
        getRuntimeContext().getNumberOfParallelSubtasks(), taskID, getRuntimeContext().getAttemptNumber(),
        rowType, true, Integer.parseInt(outputLevel), 0);

    clusteringMetrics.startClustering();

    List<ClusteringOperation> clusteringOps = clusteringGroupInfo.getOperations();

    // 记录Level 1文件的下标位置, 防止后续Level 1文件被溢写到磁盘
    int level1Index = -1;
    for (int i = 0; i < clusteringOps.size(); i++) {
      int level = FSUtils.getLevelNumFromLog(new Path(clusteringOps.get(i).getDataFilePath()));
      if (level == 1) {
        level1Index = i;
        break;
      }
    }

    List<ClosableIterator<RowData>> iterators = clusteringOps.stream().map(ClusteringOperation::getDataFilePath).map(path -> {
      try {
        return FlinkLsmUtils.getBaseFileIterator(path, FlinkLsmUtils.getLsmRequiredPositions(schema, schema),
            rowType.getFieldNames(), rowDataType.getChildren(), conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME), conf,
            table.getHadoopConf(), table.getMetaClient().getTableConfig(), InternalSchemaManager.get(conf, table.getMetaClient()),
            new ArrayList<>());
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }).collect(Collectors.toList());

    List<RecordReader<HoodieRecord>> readers = FlinkLsmUtils.createLsmRecordReaders(iterators, spillTreshold, new RowDataSerializer(rowType),
        getContainingTask().getEnvironment().getIOManager(), getContainingTask().getEnvironment().getMemoryManager().getPageSize(),
        FlinkLsmUtils.getRecordKeyIndex(schema), level1Index);

    // clean spilled iterator by gc
    iterators.clear();

    boolean isIgnoreDelete = "1".equals(outputLevel);
    iterator = new LsmMergeIterator(isIgnoreDelete, conf, writeConfig.getPayloadConfig().getProps(),
        schema, rowType, table.getHadoopConf(), requiredPos, writeConfig.getBasePath(), readers, true);

    try {
      while (iterator.hasNext()) {
        RowData record = iterator.next();
        rowCreateHandle.write(record.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD).toString(), partitionPath, record, false);
      }

      clusteringMetrics.endClustering();
      List<WriteStatus> writeStatuses = Collections.singletonList(rowCreateHandle.close().toWriteStatus());
      collector.collect(new ClusteringCommitEvent(instantTime, writeStatuses, this.taskID));
    } finally {
      iterator.close();
      iterator = null;
    }
  }

  private int[] getRequiredPositions() {
    final List<String> fieldNames = readerSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    return schema.getFields().stream()
        .map(field -> fieldNames.indexOf(field.name()))
        .mapToInt(i -> i)
        .toArray();
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) {
    this.executor = executor;
  }

  @VisibleForTesting
  public void setOutput(Output<StreamRecord<ClusteringCommitEvent>> output) {
    this.output = output;
  }

  private void registerMetrics() {
    this.clusteringMetrics = new FlinkClusteringMetrics(FlinkWriteClients.getHoodieClientConfig(this.conf));
    this.clusteringMetrics.registerMetrics(FlinkClusteringMetricEnum.notClusteringPlanOperator.value, FlinkClusteringMetricEnum.isClusteringOperator.value);
  }

}
