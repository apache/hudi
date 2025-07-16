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
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.LocalAvroSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableFilterIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;

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
  private final BiFunction<T, Schema, String> recordKeyExtractor;
  protected final HoodieFileFormat baseFileFormat;
  // For general predicate pushdown.
  protected final Option<Predicate> keyFilterOpt;
  protected final HoodieTableConfig tableConfig;
  private FileGroupReaderSchemaHandler<T> schemaHandler = null;
  private String tablePath = null;
  private String latestCommitTime = null;
  private Option<HoodieRecordMerger> recordMerger = null;
  private Boolean hasLogFiles = null;
  private Boolean hasBootstrapBaseFile = null;
  private Boolean needsBootstrapMerge = null;

  // should we do position based merging for mor
  private Boolean shouldMergeUseRecordPosition = null;
  protected String partitionPath;
  protected Option<InstantRange> instantRangeOpt = Option.empty();
  private RecordMergeMode mergeMode;
  protected ReaderContextTypeConverter typeConverter;

  // for encoding and decoding schemas to the spillable map
  private final LocalAvroSchemaCache localAvroSchemaCache = LocalAvroSchemaCache.getInstance();

  protected HoodieReaderContext(StorageConfiguration<?> storageConfiguration,
                                HoodieTableConfig tableConfig,
                                Option<InstantRange> instantRangeOpt,
                                Option<Predicate> keyFilterOpt) {
    this.tableConfig = tableConfig;
    this.storageConfiguration = storageConfiguration;
    this.recordKeyExtractor = tableConfig.populateMetaFields() ? metadataKeyExtractor() : virtualKeyExtractor(tableConfig.getRecordKeyFields()
        .orElseThrow(() -> new IllegalArgumentException("No record keys specified and meta fields are not populated")));
    this.baseFileFormat = tableConfig.getBaseFileFormat();
    this.instantRangeOpt = instantRangeOpt;
    this.keyFilterOpt = keyFilterOpt;
    this.typeConverter = new ReaderContextTypeConverter();
  }

  // Getter and Setter for schemaHandler
  public FileGroupReaderSchemaHandler<T> getSchemaHandler() {
    return schemaHandler;
  }

  public void setSchemaHandler(FileGroupReaderSchemaHandler<T> schemaHandler) {
    this.schemaHandler = schemaHandler;
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

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
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
    return new HoodieRecordSizeEstimator<>(schemaHandler.getRequiredSchema());
  }

  public CustomSerializer<BufferedRecord<T>> getRecordSerializer() {
    return new DefaultSerializer<>();
  }

  public ReaderContextTypeConverter getTypeConverter() {
    return typeConverter;
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
   * Converts an Avro record, e.g., serialized in the log files, to an engine-specific record.
   *
   * @param avroRecord The Avro record.
   * @return An engine-specific record in Type {@link T}.
   */
  public abstract T convertAvroRecord(IndexedRecord avroRecord);

  public abstract GenericRecord convertToAvroRecord(T record, Schema schema);

  /**
   * There are two cases to handle:
   * 1). Return the delete record if it's not null;
   * 2). otherwise fills an empty row with record key fields and returns.
   *
   * <p>For case2, when `emitDelete` is true for FileGroup reader and payload for DELETE record is empty,
   * a record key row is emitted to downstream to delete data from storage by record key with the best effort.
   * Returns null if the primary key semantics been lost: the requested schema does not include all the record key fields.
   *
   * @param record    delete record
   * @param recordKey record key
   *
   * @return Engine specific row which contains record key fields.
   */
  @Nullable
  public abstract T getDeleteRow(T record, String recordKey);
  
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
    RecordMergeMode recordMergeMode = tableConfig.getRecordMergeMode();
    String mergeStrategyId = tableConfig.getRecordMergeStrategyId();
    if (!tableConfig.getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      Triple<RecordMergeMode, String, String> triple = HoodieTableConfig.inferCorrectMergingBehavior(
          recordMergeMode, tableConfig.getPayloadClass(),
          mergeStrategyId, null, tableConfig.getTableVersion());
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
   * Gets the field value.
   *
   * @param record    The record in engine-specific type.
   * @param schema    The Avro schema of the record.
   * @param fieldName The field name. A dot separated string if a nested field.
   * @return The field value.
   */
  public abstract Object getValue(T record, Schema schema, String fieldName);

  /**
   * Get value of metadata field in a more efficient way than #getValue.
   *
   * @param record The record in engine-specific type.
   * @param pos    The position of the metadata field.
   *
   * @return The value for the target metadata field.
   */
  public abstract String getMetaFieldValue(T record, int pos);

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
    final Schema.Field commitTimeField = schemaHandler.getRequiredSchema().getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    final int commitTimePos = commitTimeField.pos();
    java.util.function.Predicate<T> instantFilter =
        row -> instantRange.isInRange(getMetaFieldValue(row, commitTimePos));
    return new CloseableFilterIterator<>(fileRecordIterator, instantFilter);
  }

  /**
   * Gets the record key in String.
   *
   * @param record The record in engine-specific type.
   * @param schema The Avro schema of the record.
   * @return The record key in String.
   */
  public String getRecordKey(T record, Schema schema) {
    return recordKeyExtractor.apply(record, schema);
  }

  private BiFunction<T, Schema, String> metadataKeyExtractor() {
    return (record, schema) -> getValue(record, schema, RECORD_KEY_METADATA_FIELD).toString();
  }

  private BiFunction<T, Schema, String> virtualKeyExtractor(String[] recordKeyFields) {
    return (record, schema) -> {
      BiFunction<String, Integer, String> valueFunction = (recordKeyField, index) -> {
        Object result = getValue(record, schema, recordKeyField);
        return result != null ? result.toString() : null;
      };
      return KeyGenerator.constructRecordKey(recordKeyFields, valueFunction);
    };
  }

  /**
   * Gets the ordering value in particular type.
   *
   * @param record An option of record.
   * @param schema The Avro schema of the record.
   * @param orderingFieldName name of the ordering field
   * @return The ordering value.
   */
  public Comparable getOrderingValue(T record,
                                     Schema schema,
                                     Option<String> orderingFieldName) {
    if (orderingFieldName.isEmpty()) {
      return DEFAULT_ORDERING_VALUE;
    }

    Object value = getValue(record, schema, orderingFieldName.get());
    Comparable finalOrderingVal = value != null ? convertValueToEngineType((Comparable) value) : DEFAULT_ORDERING_VALUE;
    return finalOrderingVal;
  }

  /**
   * Constructs a new {@link HoodieRecord} based on the given buffered record {@link BufferedRecord}.
   *
   * @param bufferedRecord  The {@link BufferedRecord} object with engine-specific row
   * @return A new instance of {@link HoodieRecord}.
   */
  public abstract HoodieRecord<T> constructHoodieRecord(BufferedRecord<T> bufferedRecord);

  /**
   * Constructs a new Engine based record based on a given schema, base record and update values.
   *
   * @param schema           The schema of the new record.
   * @param updateValues     The map recording field index and its corresponding update value.
   * @param baseRecord       The record based on which the engine record is built.
   * @return A new instance of engine record type {@link T}.
   */
  public abstract T constructEngineRecord(Schema schema,
                                          Map<Integer, Object> updateValues,
                                          BufferedRecord<T> baseRecord);

  /**
   * Seals the engine-specific record to make sure the data referenced in memory do not change.
   *
   * @param record The record.
   * @return The record containing the same data that do not change in memory over time.
   */
  public abstract T seal(T record);

  /**
   * Converts engine specific row into binary format.
   *
   * @param avroSchema The avro schema of the row
   * @param record     The engine row
   *
   * @return row in binary format
   */
  public abstract T toBinaryRow(Schema avroSchema, T record);

  /**
   * Gets the schema encoded in the buffered record {@code BufferedRecord}.
   *
   * @param record {@link BufferedRecord} object with engine-specific type
   *
   * @return The avro schema if it is encoded in the metadata map, else null
   */
  public Schema getSchemaFromBufferRecord(BufferedRecord<T> record) {
    return decodeAvroSchema(record.getSchemaId());
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

  /**
   * Creates a function that will reorder records of schema "from" to schema of "to"
   * all fields in "to" must be in "from", but not all fields in "from" must be in "to"
   *
   * @param from           the schema of records to be passed into UnaryOperator
   * @param to             the schema of records produced by UnaryOperator
   * @param renamedColumns map of renamed columns where the key is the new name from the query and
   *                       the value is the old name that exists in the file
   * @return a function that takes in a record and returns the record with reordered columns
   */
  public abstract UnaryOperator<T> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns);

  public final UnaryOperator<T> projectRecord(Schema from, Schema to) {
    return projectRecord(from, to, Collections.emptyMap());
  }

  /**
   * Returns the value to a type representation in a specific engine.
   * <p>
   * This can be overridden by the reader context implementation on a specific engine to handle
   * engine-specific field type system.  For example, Spark uses {@code UTF8String} to represent
   * {@link String} field values, so we need to convert the values to {@code UTF8String} type
   * in Spark for proper value comparison.
   *
   * @param value {@link Comparable} value to be converted.
   *
   * @return the converted value in a type representation in a specific engine.
   */
  public Comparable convertValueToEngineType(Comparable value) {
    return value;
  }

  /**
   * Extracts the record position value from the record itself.
   *
   * @return the record position in the base file.
   */
  public long extractRecordPosition(T record, Schema schema, String fieldName, long providedPositionIfNeeded) {
    if (supportsParquetRowIndex()) {
      Object position = getValue(record, schema, fieldName);
      if (position != null) {
        return (long) position;
      } else {
        throw new IllegalStateException("Record position extraction failed");
      }
    }
    return providedPositionIfNeeded;
  }

  public boolean supportsParquetRowIndex() {
    return false;
  }

  /**
   * Encodes the given avro schema for efficient serialization.
   */
  public Integer encodeAvroSchema(Schema schema) {
    return this.localAvroSchemaCache.cacheSchema(schema);
  }

  /**
   * Decodes the avro schema with given version ID.
   */
  @Nullable
  protected Schema decodeAvroSchema(Object versionId) {
    return this.localAvroSchemaCache.getSchema((Integer) versionId).orElse(null);
  }
}
