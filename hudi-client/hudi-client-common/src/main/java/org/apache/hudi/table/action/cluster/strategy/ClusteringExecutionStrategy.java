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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;

/**
 * Pluggable implementation for writing data into new file groups based on ClusteringPlan.
 */
public abstract class ClusteringExecutionStrategy<T, I, K, O> implements Serializable {

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

  protected ClosableIterator<HoodieRecord<T>> getRecordIterator(ReaderContextFactory<T> readerContextFactory, ClusteringOperation operation, String instantTime, long maxMemory) {
    HoodieWriteConfig config = getWriteConfig();
    TypedProperties props = TypedProperties.copy(config.getProps());
    props.setProperty(MAX_MEMORY_FOR_MERGE.key(), Long.toString(maxMemory));

    HoodieTable table = getHoodieTable();

    FileSlice fileSlice = clusteringOperationToFileSlice(table.getMetaClient().getBasePath().toString(), operation);
    final boolean usePosition = getWriteConfig().getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchema = SerDeHelper.fromJson(getWriteConfig().getInternalSchema());
    try {
      return getFileGroupReader(table.getMetaClient(), fileSlice, readerSchemaWithMetaFields, internalSchema, readerContextFactory, instantTime, usePosition).getClosableHoodieRecordIterator();
    } catch (IOException e) {
      throw new HoodieClusteringException("Error reading file slices", e);
    }
  }

  /**
   * Construct FileSlice from a given clustering operation {@code clusteringOperation}.
   */
  protected FileSlice clusteringOperationToFileSlice(String basePath, ClusteringOperation clusteringOperation) {
    String partitionPath = clusteringOperation.getPartitionPath();
    boolean baseFileExists = !StringUtils.isNullOrEmpty(clusteringOperation.getDataFilePath());
    HoodieBaseFile baseFile = baseFileExists ? new HoodieBaseFile(new StoragePath(basePath, clusteringOperation.getDataFilePath()).toString()) : null;
    List<HoodieLogFile> logFiles = clusteringOperation.getDeltaFilePaths().stream().map(p ->
            new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
                basePath, partitionPath), p)))
        .sorted(new HoodieLogFile.LogFileComparator())
        .collect(Collectors.toList());

    ValidationUtils.checkState(baseFileExists || !logFiles.isEmpty(), "Both base file and log files are missing from this clustering operation " + clusteringOperation);
    String baseInstantTime = baseFileExists ? baseFile.getCommitTime() : logFiles.get(0).getDeltaCommitTime();
    FileSlice fileSlice = new FileSlice(partitionPath, baseInstantTime, clusteringOperation.getFileId());
    fileSlice.setBaseFile(baseFile);
    logFiles.forEach(fileSlice::addLogFile);
    return fileSlice;
  }

  protected static <R> HoodieFileGroupReader<R> getFileGroupReader(HoodieTableMetaClient metaClient, FileSlice fileSlice, Schema readerSchema, Option<InternalSchema> internalSchemaOption,
                                                                   ReaderContextFactory<R> readerContextFactory, String instantTime, boolean usePosition) {
    HoodieReaderContext<R> readerContext = readerContextFactory.getContext();
    return HoodieFileGroupReader.<R>newBuilder()
        .withReaderContext(readerContext).withHoodieTableMetaClient(metaClient).withLatestCommitTime(instantTime)
        .withFileSlice(fileSlice).withDataSchema(readerSchema).withRequestedSchema(readerSchema).withInternalSchema(internalSchemaOption)
        .withShouldUseRecordPosition(usePosition).withProps(metaClient.getTableConfig().getProps()).build();
  }
}
