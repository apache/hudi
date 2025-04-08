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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.HoodieFileSliceReader;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Pluggable implementation for writing data into new file groups based on ClusteringPlan.
 */
public abstract class ClusteringExecutionStrategy<T, I, K, O> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ClusteringExecutionStrategy.class);

  private final HoodieTable<T, I, K, O> hoodieTable;
  private final transient HoodieEngineContext engineContext;
  protected final HoodieWriteConfig writeConfig;
  protected final HoodieRecordType recordType;
  protected final Schema readerSchemaWithMetaFields;

  public ClusteringExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
    this.hoodieTable = table;
    this.engineContext = engineContext;
    this.recordType = table.getConfig().getRecordMerger().getRecordType();
    this.readerSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(writeConfig.getSchema()));
  }

  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters. The number of new
   * file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   */
  public abstract HoodieWriteMetadata<O> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime);

  protected HoodieTable<T, I, K, O> getHoodieTable() {
    return this.hoodieTable;
  }

  protected HoodieEngineContext getEngineContext() {
    return this.engineContext;
  }

  protected HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }

  protected ClosableIterator<HoodieRecord<T>> getRecordIteratorWithLogFiles(ClusteringOperation operation, String instantTime, long maxMemory,
                                                                            Option<BaseKeyGenerator> keyGeneratorOpt, Option<HoodieFileReader> baseFileReaderOpt) {
    HoodieWriteConfig config = getWriteConfig();
    HoodieTable table = getHoodieTable();
    HoodieTableConfig tableConfig = table.getMetaClient().getTableConfig();
    HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(table.getStorage())
        .withBasePath(table.getMetaClient().getBasePath())
        .withLogFilePaths(operation.getDeltaFilePaths())
        .withReaderSchema(readerSchemaWithMetaFields)
        .withLatestInstantTime(instantTime)
        .withMaxMemorySizeInBytes(maxMemory)
        .withReverseReader(config.getCompactionReverseLogReadEnabled())
        .withBufferSize(config.getMaxDFSStreamBufferSize())
        .withSpillableMapBasePath(config.getSpillableMapBasePath())
        .withPartition(operation.getPartitionPath())
        .withOptimizedLogBlocksScan(config.enableOptimizedLogBlocksScan())
        .withDiskMapType(config.getCommonConfig().getSpillableDiskMapType())
        .withBitCaskDiskMapCompressionEnabled(config.getCommonConfig().isBitCaskDiskMapCompressionEnabled())
        .withRecordMerger(config.getRecordMerger())
        .withTableMetaClient(table.getMetaClient())
        .build();

    try {
      return new HoodieFileSliceReader(baseFileReaderOpt, scanner, readerSchemaWithMetaFields, tableConfig.getPreCombineField(), config.getRecordMerger(),
          tableConfig.getProps(),
          tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(),
              tableConfig.getPartitionFieldProp())), keyGeneratorOpt);
    } catch (IOException e) {
      throw new HoodieClusteringException("Error reading file slices", e);
    }
  }

  protected ClosableIterator<HoodieRecord<T>> getRecordIteratorWithBaseFileOnly(Option<BaseKeyGenerator> keyGeneratorOpt, HoodieFileReader baseFileReader) {
    // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
    //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
    //       it since these records will be shuffled later.
    ClosableIterator<HoodieRecord> baseRecordsIterator;
    try {
      baseRecordsIterator = baseFileReader.getRecordIterator(readerSchemaWithMetaFields);
    } catch (IOException e) {
      throw new HoodieClusteringException("Error reading base file", e);
    }
    return new CloseableMappingIterator(
        baseRecordsIterator,
        rec -> ((HoodieRecord) rec).copy().wrapIntoHoodieRecordPayloadWithKeyGen(readerSchemaWithMetaFields, writeConfig.getProps(), keyGeneratorOpt));
  }
}
