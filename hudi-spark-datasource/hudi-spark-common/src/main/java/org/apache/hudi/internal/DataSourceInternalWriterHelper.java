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

package org.apache.hudi.internal;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class for HoodieDataSourceInternalWriter used by Spark datasource v2.
 */
public class DataSourceInternalWriterHelper implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DataSourceInternalWriterHelper.class);
  public static final String INSTANT_TIME_OPT_KEY = "hoodie.instant.time";

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final SparkRDDWriteClient writeClient;
  private final HoodieTable hoodieTable;
  private final WriteOperationType operationType;
  private final Map<String, String> extraMetadata;
  private final HoodieWriteConfig writeConfig;

  public DataSourceInternalWriterHelper(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
                                        SparkSession sparkSession, StorageConfiguration<?> storageConf, Map<String, String> extraMetadata, WriteOperationType writeOperationType) {
    this.instantTime = instantTime;
    this.operationType = writeOperationType;
    this.extraMetadata = extraMetadata;
    this.writeConfig = writeConfig;
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf.newInstance()).setBasePath(writeConfig.getBasePath()).build();
    this.writeClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext())), writeConfig);
    this.writeClient.setOperationType(operationType);
    this.writeClient.startCommitWithTime(instantTime, CommitUtils.getCommitActionType(operationType, metaClient.getTableType()));
    this.hoodieTable = this.writeClient.initTable(operationType, Option.of(instantTime));

    this.writeClient.validateAgainstTableProperties(this.metaClient.getTableConfig(), writeConfig);
    this.writeClient.preWrite(instantTime, operationType, metaClient);
  }

  public boolean useCommitCoordinator() {
    return true;
  }

  public void onDataWriterCommit(String message) {
    LOG.info("Received commit of a data writer = " + message);
  }

  public void commit(List<WriteStatus> writeStatuses) {
    try {
      List<HoodieWriteStat> writeStatList = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      writeClient.commitStats(instantTime, writeStatList, Option.of(extraMetadata),
          CommitUtils.getCommitActionType(operationType, metaClient.getTableType()), getReplacedFileIds(writeStatuses), Option.empty());
    } catch (Exception ioe) {
      throw new HoodieException(ioe.getMessage(), ioe);
    } finally {
      writeClient.close();
    }
  }

  private Map<String, List<String>> getReplacedFileIds(List<WriteStatus> writeStatuses) {
    if (operationType == WriteOperationType.BULK_INSERT) {
      return Collections.emptyMap();
    }
    if (operationType == WriteOperationType.INSERT_OVERWRITE || operationType == WriteOperationType.BUCKET_RESCALE) {
      String staticOverwritePartition = writeConfig.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS);
      if (StringUtils.nonEmpty(staticOverwritePartition)) {
        // static insert overwrite partitions
        List<String> partitionPaths = Arrays.asList(staticOverwritePartition.split(","));
        hoodieTable.getContext().setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of matching static partitions");
        return HoodieJavaPairRDD.getJavaPairRDD(hoodieTable.getContext().parallelize(partitionPaths, partitionPaths.size()).mapToPair(
            partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
      } else {
        // dynamic insert overwrite partitions
        return writeStatuses.stream().map(status -> status.getStat().getPartitionPath()).distinct().collect(Collectors.toMap(partitionPath -> partitionPath, this::getAllExistingFileIds));
      }
    }
    if (operationType == WriteOperationType.INSERT_OVERWRITE_TABLE) {
      HoodieEngineContext context = writeClient.getEngineContext();
      List<String> partitionPaths = FSUtils.getAllPartitionPaths(context,
          hoodieTable.getStorage(),
          writeConfig.getMetadataConfig(),
          hoodieTable.getMetaClient().getBasePath());

      if (partitionPaths == null || partitionPaths.isEmpty()) {
        return Collections.emptyMap();
      }

      context.setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of all partitions");
      return HoodieJavaPairRDD.getJavaPairRDD(context.parallelize(partitionPaths, partitionPaths.size()).mapToPair(
          partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    }
    throw new HoodieException("Unsupported operation type " + operationType);
  }

  public void abort() {
    LOG.error("Commit " + instantTime + " aborted ");
    writeClient.close();
  }

  public void createInflightCommit() {
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        metaClient.createNewInstant(State.REQUESTED,
            CommitUtils.getCommitActionType(operationType, metaClient.getTableType()), instantTime), Option.empty());
  }

  public HoodieTable getHoodieTable() {
    return hoodieTable;
  }

  public WriteOperationType getWriteOperationType() {
    return operationType;
  }

  private List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return hoodieTable.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }
}
