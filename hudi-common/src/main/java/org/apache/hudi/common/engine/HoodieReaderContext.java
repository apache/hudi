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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableFilterIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

/**
 * An abstract reader context class for {@code HoodieFileGroupReader} to use, containing APIs for
 * engine-specific implementation on reading data files, getting field values from a record,
 * transforming a record, etc.
 * <p>
 * For each query engine, this class should be extended and plugged into {@code HoodieFileGroupReader}
 * to realize the file group reading.
 *
 * @param <T> The type of engine-specific record representation, e.g.,{@code InternalRow} in Spark
 *            and {@code RowData} in Flink.
 */
public abstract class HoodieReaderContext<T> {
  private final StorageConfiguration<?> storageConfiguration;
  protected final HoodieFileFormat baseFileFormat;
  // For general predicate pushdown.
  protected final Option<Predicate> keyFilterOpt;
  protected final HoodieTableConfig tableConfig;
  private String tablePath = null;
  private String latestCommitTime = null;
  private Option<HoodieRecordMerger> recordMerger = null;
  private Boolean hasLogFiles = null;
  private Boolean hasBootstrapBaseFile = null;
  private Boolean needsBootstrapMerge = null;

  // should we do position based merging for mor
  private Boolean shouldMergeUseRecordPosition = null;
  protected Option<InstantRange> instantRangeOpt = Option.empty();
  private RecordMergeMode mergeMode;
  protected RecordContext<T> recordContext;
  private FileGroupReaderSchemaHandler<T> schemaHandler = null;
  // the default iterator mode is engine-specific record mode
  private IteratorMode iteratorMode = IteratorMode.ENGINE_RECORD;

  protected HoodieReaderContext(StorageConfiguration<?> storageConfiguration,
                                HoodieTableConfig tableConfig,
                                Option<InstantRange> instantRangeOpt,
                                Option<Predicate> keyFilterOpt,
                                RecordContext<T> recordContext) {
    this.tableConfig = tableConfig;
    this.storageConfiguration = storageConfiguration;
    this.baseFileFormat = tableConfig.getBaseFileFormat();
    this.instantRangeOpt = instantRangeOpt;
    this.keyFilterOpt = keyFilterOpt;
    this.recordContext = recordContext;
  }

  // Getter and Setter for schemaHandler
  public FileGroupReaderSchemaHandler<T> getSchemaHandler() {
    return schemaHandler;
  }

  public void setSchemaHandler(FileGroupReaderSchemaHandler<T> schemaHandler) {
    this.schemaHandler = schemaHandler;
  }

  public void setIteratorMode(IteratorMode iteratorMode) {
    this.iteratorMode = iteratorMode;
  }

  public IteratorMode getIteratorMode() {
    ValidationUtils.checkArgument(iteratorMode != null, "iterator mode should not be null!");
    return this.iteratorMode;
  }

  public String getTablePath() {
    if (tablePath == null) {
      throw new IllegalStateException("Table path not set in reader context.");
    }
    return tablePath;
  }

  public void setTablePath(String tablePath) {
    this.tablePath = tablePath;
  }

  public String getLatestCommitTime() {
    return latestCommitTime;
  }

  public void setLatestCommitTime(String latestCommitTime) {
    this.latestCommitTime = latestCommitTime;
  }

  public Option<HoodieRecordMerger> getRecordMerger() {
    return recordMerger;
  }

  public void setRecordMerger(Option<HoodieRecordMerger> recordMerger) {
    this.recordMerger = recordMerger;
  }

  // Getter and Setter for hasLogFiles
  public boolean getHasLogFiles() {
    return hasLogFiles;
  }

  public void setHasLogFiles(boolean hasLogFiles) {
    this.hasLogFiles = hasLogFiles;
  }

  // Getter and Setter for hasBootstrapBaseFile
  public boolean getHasBootstrapBaseFile() {
    return hasBootstrapBaseFile;
  }

  public void setHasBootstrapBaseFile(boolean hasBootstrapBaseFile) {
    this.hasBootstrapBaseFile = hasBootstrapBaseFile;
  }

  // Getter and Setter for needsBootstrapMerge
  public boolean getNeedsBootstrapMerge() {
    return needsBootstrapMerge;
  }

  public void setNeedsBootstrapMerge(boolean needsBootstrapMerge) {
    this.needsBootstrapMerge = needsBootstrapMerge;
  }

  // Getter and Setter for useRecordPosition
  public boolean getShouldMergeUseRecordPosition() {
    return shouldMergeUseRecordPosition;
  }

  public void setShouldMergeUseRecordPosition(boolean shouldMergeUseRecordPosition) {
    this.shouldMergeUseRecordPosition = shouldMergeUseRecordPosition;
  }

  public StorageConfiguration<?> getStorageConfiguration() {
    return storageConfiguration;
  }

  public Option<Predicate> getKeyFilterOpt() {
    return keyFilterOpt;
  }

  public SizeEstimator<BufferedRecord<T>> getRecordSizeEstimator() {
    return new HoodieRecordSizeEstimator<>(getSchemaHandler().getRequiredSchema());
  }

  public CustomSerializer<BufferedRecord<T>> getRecordSerializer() {
    return new DefaultSerializer<>();
  }

  public RecordContext<T> getRecordContext() {
    return recordContext;
  }

  /**
   * Gets the record iterator based on the type of engine-specific record representation from the
   * file.
   *
   * @param filePath       {@link StoragePath} instance of a file.
   * @param start          Starting byte to start reading.
   * @param length         Bytes to read.
   * @param dataSchema     Schema of records in the file in {@link Schema}.
   * @param requiredSchema Schema containing required fields to read in {@link Schema} for projection.
   * @param storage        {@link HoodieStorage} for reading records.
   * @return {@link ClosableIterator<T>} that can return all records through iteration.
   */
  public abstract ClosableIterator<T> getFileRecordIterator(
      StoragePath filePath, long start, long length, Schema dataSchema, Schema requiredSchema,
      HoodieStorage storage) throws IOException;

  /**
   * Gets the record iterator based on the type of engine-specific record representation from the
   * file.
   *
   * @param storagePathInfo {@link StoragePathInfo} instance of a file.
   * @param start           Starting byte to start reading.
   * @param length          Bytes to read.
   * @param dataSchema      Schema of records in the file in {@link Schema}.
   * @param requiredSchema  Schema containing required fields to read in {@link Schema} for projection.
   * @param storage         {@link HoodieStorage} for reading records.
   * @return {@link ClosableIterator<T>} that can return all records through iteration.
   */
  public ClosableIterator<T> getFileRecordIterator(
      StoragePathInfo storagePathInfo, long start, long length, Schema dataSchema, Schema requiredSchema,
      HoodieStorage storage) throws IOException {
    return getFileRecordIterator(storagePathInfo.getPath(), start, length, dataSchema, requiredSchema, storage);
  }

  /**
   * @param mergeMode        record merge mode
   * @param mergeStrategyId  record merge strategy ID
   * @param mergeImplClasses custom implementation classes for record merging
   *
   * @return {@link HoodieRecordMerger} to use.
   */
  protected abstract Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses);

  /**
   * Initializes the record merger based on the table configuration and properties.
   * @param properties the properties for the reader.
   */
  public void initRecordMerger(TypedProperties properties) {
    initRecordMerger(properties, false);
  }

  public void initRecordMergerForIngestion(TypedProperties properties) {
    initRecordMerger(properties, true);
  }

  /**
   * Initializes the record merger based on the table configuration and properties.
   * @param properties the properties for the reader.
   * @param isIngestion indicates if the context is used in ingestion path.
   */
  private void initRecordMerger(TypedProperties properties, boolean isIngestion) {
    Option<String> providedPayloadClass = HoodieRecordPayload.getPayloadClassNameIfPresent(properties);
    RecordMergeMode recordMergeMode = tableConfig.getRecordMergeMode();
    String mergeStrategyId = tableConfig.getRecordMergeStrategyId();
    HoodieTableVersion tableVersion = tableConfig.getTableVersion();
    // If the provided payload class differs from the table's payload class, we need to infer the correct merging behavior.
    if (isIngestion && providedPayloadClass.map(className -> !className.equals(tableConfig.getPayloadClass())).orElse(false)) {
      Triple<RecordMergeMode, String, String> triple = HoodieTableConfig.inferCorrectMergingBehavior(null, providedPayloadClass.get(), null,
          tableConfig.getPreCombineFieldsStr().orElse(null), tableVersion, true);
      recordMergeMode = triple.getLeft();
      mergeStrategyId = triple.getRight();
    } else if (tableVersion.lesserThan(HoodieTableVersion.EIGHT)) {
      Triple<RecordMergeMode, String, String> triple = HoodieTableConfig.inferCorrectMergingBehavior(
          recordMergeMode, tableConfig.getPayloadClass(),
          mergeStrategyId, tableConfig.getPreCombineFieldsStr().orElse(null), tableVersion);
      recordMergeMode = triple.getLeft();
      mergeStrategyId = triple.getRight();
    }
    this.mergeMode = recordMergeMode;
    this.recordMerger = getRecordMerger(recordMergeMode, mergeStrategyId,
        properties.getString(RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY,
            properties.getString(RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY, "")));
  }

  public RecordMergeMode getMergeMode() {
    return mergeMode;
  }

  /**
   * Get the {@link InstantRange} filter.
   */
  public Option<InstantRange> getInstantRange() {
    return instantRangeOpt;
  }

  /**
   * Apply the {@link InstantRange} filter to the file record iterator.
   *
   * @param fileRecordIterator File record iterator.
   *
   * @return File record iterator filter by {@link InstantRange}.
   */
  public ClosableIterator<T> applyInstantRangeFilter(ClosableIterator<T> fileRecordIterator) {
    // For metadata table, no need to apply instant range to base file.
    if (HoodieTableMetadata.isMetadataTable(tablePath)) {
      return fileRecordIterator;
    }
    InstantRange instantRange = getInstantRange().get();
    final Schema.Field commitTimeField = getSchemaHandler().getRequiredSchema().getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    final int commitTimePos = commitTimeField.pos();
    java.util.function.Predicate<T> instantFilter =
        row -> instantRange.isInRange(recordContext.getMetaFieldValue(row, commitTimePos));
    return new CloseableFilterIterator<>(fileRecordIterator, instantFilter);
  }

  /**
   * Merge the skeleton file and data file iterators into a single iterator that will produce rows that contain all columns from the
   * skeleton file iterator, followed by all columns in the data file iterator
   *
   * @param skeletonFileIterator iterator over bootstrap skeleton files that contain hudi metadata columns
   * @param skeletonRequiredSchema the schema of the skeleton file iterator
   * @param dataFileIterator iterator over data files that were bootstrapped into the hudi table
   * @param dataRequiredSchema the schema of the data file iterator
   * @param requiredPartitionFieldAndValues the partition field names and their values that are required by the query
   * @return iterator that concatenates the skeletonFileIterator and dataFileIterator
   */
  public abstract ClosableIterator<T> mergeBootstrapReaders(ClosableIterator<T> skeletonFileIterator,
                                                            Schema skeletonRequiredSchema,
                                                            ClosableIterator<T> dataFileIterator,
                                                            Schema dataRequiredSchema,
                                                            List<Pair<String, Object>> requiredPartitionFieldAndValues);
}
