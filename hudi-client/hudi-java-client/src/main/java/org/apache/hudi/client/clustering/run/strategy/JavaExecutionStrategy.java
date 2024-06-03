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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.JavaTaskContextSupplier;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.bulkinsert.JavaBulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.JavaCustomColumnsSortPartitioner;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering strategy for Java engine.
 */
public abstract class JavaExecutionStrategy<T>
    extends ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {

  private static final Logger LOG = LoggerFactory.getLogger(JavaExecutionStrategy.class);

  public JavaExecutionStrategy(
      HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(
      HoodieClusteringPlan clusteringPlan, Schema schema, String instantTime) {
    // execute clustering for each group and collect WriteStatus
    List<WriteStatus> writeStatusList = new ArrayList<>();
    clusteringPlan.getInputGroups().forEach(
        inputGroup -> writeStatusList.addAll(runClusteringForGroup(
            inputGroup, clusteringPlan.getStrategy().getStrategyParams(),
            Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false),
            instantTime)));
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(HoodieListData.eager(writeStatusList));
    return writeMetadata;
  }

  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters.
   * The number of new file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   *
   * @param inputRecords           List of {@link HoodieRecord}.
   * @param numOutputGroups        Number of output file groups.
   * @param instantTime            Clustering (replace commit) instant time.
   * @param strategyParams         Strategy parameters containing columns to sort the data by when clustering.
   * @param schema                 Schema of the data including metadata fields.
   * @param fileGroupIdList        File group id corresponding to each out group.
   * @param preserveHoodieMetadata Whether to preserve commit metadata while clustering.
   * @return List of {@link WriteStatus}.
   */
  public abstract List<WriteStatus> performClusteringWithRecordList(
      final List<HoodieRecord<T>> inputRecords, final int numOutputGroups, final String instantTime,
      final Map<String, String> strategyParams, final Schema schema,
      final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata);

  /**
   * Create {@link BulkInsertPartitioner} based on strategy params.
   *
   * @param strategyParams Strategy parameters containing columns to sort the data by when clustering.
   * @param schema         Schema of the data including metadata fields.
   * @return partitioner for the java engine
   */
  protected BulkInsertPartitioner<List<HoodieRecord<T>>> getPartitioner(Map<String, String> strategyParams, Schema schema) {
    if (strategyParams.containsKey(PLAN_STRATEGY_SORT_COLUMNS.key())) {
      return new JavaCustomColumnsSortPartitioner(
          strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()).split(","),
          HoodieAvroUtils.addMetadataFields(schema), getWriteConfig());
    } else {
      return JavaBulkInsertInternalPartitionerFactory.get(getWriteConfig().getBulkInsertSortMode());
    }
  }

  /**
   * Executes clustering for the group.
   */
  private List<WriteStatus> runClusteringForGroup(
      HoodieClusteringGroup clusteringGroup, Map<String, String> strategyParams,
      boolean preserveHoodieMetadata, String instantTime) {
    List<HoodieRecord<T>> inputRecords = readRecordsForGroup(clusteringGroup, instantTime);
    Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
    List<HoodieFileGroupId> inputFileIds = clusteringGroup.getSlices().stream()
        .map(info -> new HoodieFileGroupId(info.getPartitionPath(), info.getFileId()))
        .collect(Collectors.toList());
    return performClusteringWithRecordList(inputRecords, clusteringGroup.getNumOutputFileGroups(), instantTime, strategyParams, readerSchema, inputFileIds, preserveHoodieMetadata);
  }

  /**
   * Get a list of all records for the group. This includes all records from file slice
   * (Apply updates from log files, if any).
   */
  private List<HoodieRecord<T>> readRecordsForGroup(HoodieClusteringGroup clusteringGroup, String instantTime) {
    List<ClusteringOperation> clusteringOps = clusteringGroup.getSlices().stream().map(ClusteringOperation::create).collect(Collectors.toList());
    boolean hasLogFiles = clusteringOps.stream().anyMatch(op -> op.getDeltaFilePaths().size() > 0);
    if (hasLogFiles) {
      // if there are log files, we read all records into memory for a file group and apply updates.
      return readRecordsForGroupWithLogs(clusteringOps, instantTime);
    } else {
      // We want to optimize reading records for case there are no log files.
      return readRecordsForGroupBaseFiles(clusteringOps);
    }
  }

  /**
   * Read records from baseFiles and apply updates.
   */
  private List<HoodieRecord<T>> readRecordsForGroupWithLogs(List<ClusteringOperation> clusteringOps,
                                                            String instantTime) {
    HoodieWriteConfig config = getWriteConfig();
    HoodieTable table = getHoodieTable();
    List<HoodieRecord<T>> records = new ArrayList<>();

    clusteringOps.forEach(clusteringOp -> {
      long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(new JavaTaskContextSupplier(), config);
      LOG.info("MaxMemoryPerCompaction run as part of clustering => " + maxMemoryPerCompaction);
      Option<HoodieFileReader> baseFileReader = Option.empty();
      HoodieMergedLogRecordScanner scanner = null;
      try {
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
        scanner = HoodieMergedLogRecordScanner.newBuilder()
            .withStorage(table.getStorage())
            .withBasePath(table.getMetaClient().getBasePath())
            .withLogFilePaths(clusteringOp.getDeltaFilePaths())
            .withReaderSchema(readerSchema)
            .withLatestInstantTime(instantTime)
            .withMaxMemorySizeInBytes(maxMemoryPerCompaction)
            .withReverseReader(config.getCompactionReverseLogReadEnabled())
            .withBufferSize(config.getMaxDFSStreamBufferSize())
            .withSpillableMapBasePath(config.getSpillableMapBasePath())
            .withPartition(clusteringOp.getPartitionPath())
            .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
            .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
            .withRecordMerger(config.getRecordMerger())
            .build();

        baseFileReader = StringUtils.isNullOrEmpty(clusteringOp.getDataFilePath())
            ? Option.empty()
            : Option.of(HoodieIOFactory.getIOFactory(table.getStorage()).getReaderFactory(recordType)
            .getFileReader(config, new StoragePath(clusteringOp.getDataFilePath())));
        HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
        Iterator<HoodieRecord<T>> fileSliceReader = new HoodieFileSliceReader(baseFileReader, scanner, readerSchema, tableConfig.getPreCombineField(), writeConfig.getRecordMerger(),
            tableConfig.getProps(),
            tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(),
                tableConfig.getPartitionFieldProp())));
        fileSliceReader.forEachRemaining(records::add);
      } catch (IOException e) {
        throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
            + " and " + clusteringOp.getDeltaFilePaths(), e);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
        if (baseFileReader.isPresent()) {
          baseFileReader.get().close();
        }
      }
    });
    return records;
  }

  /**
   * Read records from baseFiles.
   */
  private List<HoodieRecord<T>> readRecordsForGroupBaseFiles(List<ClusteringOperation> clusteringOps) {
    List<HoodieRecord<T>> records = new ArrayList<>();
    clusteringOps.forEach(clusteringOp -> {
      try (HoodieFileReader baseFileReader = HoodieIOFactory.getIOFactory(getHoodieTable().getStorage())
          .getReaderFactory(recordType)
          .getFileReader(getHoodieTable().getConfig(), new StoragePath(clusteringOp.getDataFilePath()))) {
        Schema readerSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(getWriteConfig().getSchema()));
        Iterator<HoodieRecord> recordIterator = baseFileReader.getRecordIterator(readerSchema);
        // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
        //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
        //       it since these records will be put into the records(List).
        recordIterator.forEachRemaining(record -> records.add(record.copy().wrapIntoHoodieRecordPayloadWithKeyGen(readerSchema, new Properties(), Option.empty())));
      } catch (IOException e) {
        throw new HoodieClusteringException("Error reading input data for " + clusteringOp.getDataFilePath()
            + " and " + clusteringOp.getDeltaFilePaths(), e);
      }
    });
    return records;
  }
}
