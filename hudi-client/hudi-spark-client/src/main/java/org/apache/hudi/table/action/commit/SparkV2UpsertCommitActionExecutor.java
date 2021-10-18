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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieRowWriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.row.HoodieRowMergeHandle;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.HoodieBaseTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.CollectionUtils.toMap;

public class SparkV2UpsertCommitActionExecutor implements Serializable {

  protected final transient HoodieEngineContext context;
  protected final transient Configuration hadoopConf;
  protected final HoodieWriteConfig config;
  protected final HoodieBaseTable<Dataset<Row>, Dataset<HoodieKey>, Dataset<HoodieRowWriteStatus>> table;
  protected final Dataset<Row> inputDataFrame;
  protected final String instantTime;
  protected final WriteOperationType operationType;
  protected final Option<Map<String, String>> extraMetadata;
  protected final TaskContextSupplier taskContextSupplier;
  protected final TransactionManager txnManager;
  protected Option<Pair<HoodieInstant, Map<String, String>>> lastCompletedTxn;

  protected Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();

  public SparkV2UpsertCommitActionExecutor(HoodieSparkEngineContext context,
      HoodieWriteConfig config,
      HoodieBaseTable<Dataset<Row>, Dataset<HoodieKey>, Dataset<HoodieRowWriteStatus>> table,
      Dataset<Row> inputDataFrame,
      String instantTime,
      WriteOperationType operationType,
      Option<Map<String, String>> extraMetadata) {
    this.context = context;
    this.hadoopConf = context.getHadoopConf().get();
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
    this.inputDataFrame = inputDataFrame;
    this.operationType = operationType;
    this.extraMetadata = extraMetadata;
    this.taskContextSupplier = context.getTaskContextSupplier();
    // TODO : Remove this once we refactor and move out autoCommit method from here, since the TxnManager is held in {@link AbstractHoodieWriteClient}.
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
    this.lastCompletedTxn = TransactionUtils.getLastCompletedTxnInstantAndMetadata(table.getMetaClient());
    initKeyGenIfNeeded(config.populateMetaFields());
  }

  private void initKeyGenIfNeeded(boolean populateMetaFields) {
    if (!populateMetaFields) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(new TypedProperties(config.getProps())));
      } catch (IOException e) {
        throw new HoodieIOException("Only BaseKeyGenerators are supported when meta columns are disabled ", e);
      }
    }
  }

  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> execute() {
    return SparkV2WriteHelper.newInstance().write(instantTime, inputDataFrame, context, table,
        config.shouldCombineBeforeInsert(), config.getInsertShuffleParallelism(), this, true);
  }

  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> execute(Dataset<Row> inputDf) {
    inputDf.cache();

    final StructType schema = inputDf.schema();
    final Map<String, String> options = toMap(config.getProps());
    if (extraMetadata.isPresent()) {
      options.putAll(extraMetadata.get());
    }
    Dataset<HoodieRowWriteStatus> writeStatuses = clusteringHandleUpdate(inputDf)
        .mapPartitions((MapPartitionsFunction<Row, HoodieRowWriteStatus>) rows -> {
          try (HoodieRowMergeHandle mergeHandle = new HoodieRowMergeHandle(
              table,
              schema,
              taskContextSupplier.getPartitionIdSupplier().get(),
              taskContextSupplier.getStageIdSupplier().get(),
              taskContextSupplier.getAttemptIdSupplier().get(),
              instantTime,
              config)) {
            rows.forEachRemaining(mergeHandle::handle);
            return Stream.of(mergeHandle.flush()).iterator();
          }
        }, Encoders.bean(HoodieRowWriteStatus.class));

    HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> result = new HoodieWriteMetadata<>();
    result.setWriteStatuses(writeStatuses);
    return result;
  }

  private Dataset<Row> clusteringHandleUpdate(Dataset<Row> inputDf) {
    if (config.isClusteringEnabled()) {
      Set<HoodieFileGroupId> fileGroupsInPendingClustering =
          table.getFileSystemView().getFileGroupsInPendingClustering().map(entry -> entry.getKey()).collect(Collectors.toSet());
      UpdateStrategy updateStrategy = (UpdateStrategy) ReflectionUtils
          .loadClass(config.getClusteringUpdatesStrategyClass(), this.context, fileGroupsInPendingClustering);
      // TODO(rxu) handle clustering for df
      return inputDf;
    } else {
      return inputDf;
    }
  }

}
