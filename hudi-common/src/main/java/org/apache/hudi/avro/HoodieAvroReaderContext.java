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

package org.apache.hudi.avro;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestMerger;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordSerializer;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * An implementation of {@link HoodieReaderContext} that reads data from the base files as {@link IndexedRecord}.
 * This implementation does not rely on a specific engine and can be used in any JVM environment as a result.
 */
public class HoodieAvroReaderContext extends HoodieReaderContext<IndexedRecord> {
  private final Map<StoragePath, HoodieAvroFileReader> reusableFileReaders;
  private final boolean isMultiFormat;

  /**
   * Constructs an instance of the reader context that will read data into Avro records.
   * @param storageConfiguration the storage configuration to use for reading files
   * @param tableConfig the configuration of the Hudi table being read
   * @param instantRangeOpt the set of valid instants for this read
   * @param filterOpt an optional filter to apply on the record keys
   */
  public HoodieAvroReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt,
      TypedProperties props) {
    this(storageConfiguration, tableConfig, instantRangeOpt, filterOpt, Collections.emptyMap(), tableConfig.getPayloadClass(), new HoodieConfig(props));
  }

  public HoodieAvroReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt) {
    this(storageConfiguration, tableConfig, instantRangeOpt, filterOpt, Collections.emptyMap(), tableConfig.getPayloadClass(), ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER);
  }

  /**
   * Constructs an instance of the reader context with an optional cache of reusable file readers.
   * This provides an opportunity for increased performance when repeatedly reading from the same files.
   * The caller of this constructor is responsible for managing the lifecycle of the reusable file readers.
   * @param storageConfiguration the storage configuration to use for reading files
   * @param tableConfig the configuration of the Hudi table being read
   * @param instantRangeOpt the set of valid instants for this read
   * @param filterOpt an optional filter to apply on the record keys
   * @param reusableFileReaders a map of reusable file readers, keyed by their storage paths.
   */
  public HoodieAvroReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt,
      Map<StoragePath, HoodieAvroFileReader> reusableFileReaders,
      TypedProperties props) {
    this(storageConfiguration, tableConfig, instantRangeOpt, filterOpt, reusableFileReaders, tableConfig.getPayloadClass(), new HoodieConfig(props));
  }

  /**
   * Constructs an instance of the reader context for writer workflows
   *
   * @param storageConfiguration the storage configuration to use for reading files
   * @param tableConfig          the configuration of the Hudi table being read
   * @param payloadClassName     the payload class for the writer
   * @param props                the reader configurations that should be used when performing reads
   */
  public HoodieAvroReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      String payloadClassName,
      TypedProperties props) {
    this(storageConfiguration, tableConfig, Option.empty(), Option.empty(), Collections.emptyMap(), payloadClassName, new HoodieConfig(props));
  }

  private HoodieAvroReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt,
      Option<Predicate> filterOpt,
      Map<StoragePath, HoodieAvroFileReader> reusableFileReaders,
      String payloadClassName,
      HoodieConfig hoodieReaderConfig) {
    super(storageConfiguration, tableConfig, instantRangeOpt, filterOpt, new AvroRecordContext(tableConfig, payloadClassName), hoodieReaderConfig);
    this.reusableFileReaders = reusableFileReaders;
    this.isMultiFormat = tableConfig.isMultipleBaseFileFormatsEnabled();
  }

  @Override
  public ClosableIterator<IndexedRecord> getFileRecordIterator(
      StoragePathInfo storagePathInfo, long start, long length, Schema dataSchema, Schema requiredSchema,
      HoodieStorage storage) throws IOException {
    boolean isLogFile = FSUtils.isLogFile(storagePathInfo.getPath());
    HoodieAvroFileReader reader = getOrCreateFileReader(storagePathInfo.getPath(), isLogFile, format -> {
      try {
        return (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
            .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(hoodieReaderConfig,
                storagePathInfo, format, Option.empty());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to create avro records iterator from file path " + storagePathInfo.getPath(), e);
      }
    });

    return getFileRecordIterator(reader, storagePathInfo.getPath(), isLogFile, dataSchema, requiredSchema);
  }

  @Override
  public ClosableIterator<IndexedRecord> getFileRecordIterator(
      StoragePath filePath,
      long start,
      long length,
      Schema dataSchema,
      Schema requiredSchema,
      HoodieStorage storage) throws IOException {
    boolean isLogFile = FSUtils.isLogFile(filePath);
    HoodieAvroFileReader reader = getOrCreateFileReader(filePath, isLogFile, format -> {
      try {
        return (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
            .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(hoodieReaderConfig,
                filePath, format, Option.empty());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to create avro records iterator from file path " + filePath, e);
      }
    });

    return getFileRecordIterator(reader, filePath, isLogFile, dataSchema, requiredSchema);
  }

  private HoodieAvroFileReader getOrCreateFileReader(
      StoragePath path, boolean isLogFile, Function<HoodieFileFormat, HoodieAvroFileReader> func) throws IOException {
    if (reusableFileReaders.containsKey(path)) {
      return reusableFileReaders.get(path);
    } else {
      HoodieFileFormat fileFormat = isMultiFormat && !isLogFile ? HoodieFileFormat.fromFileExtension(path.getFileExtension()) : baseFileFormat;
      try {
        return func.apply(fileFormat);
      } catch (HoodieIOException e) {
        throw e.getIOException();
      }
    }
  }

  public ClosableIterator<IndexedRecord> getFileRecordIterator(
      HoodieAvroFileReader reader,
      StoragePath filePath,
      boolean isLogFile,
      Schema dataSchema,
      Schema requiredSchema) throws IOException {
    Schema fileOutputSchema;
    Map<String, String> renamedColumns;
    if (isLogFile) {
      fileOutputSchema = requiredSchema;
      renamedColumns = Collections.emptyMap();
    } else {
      Pair<Schema, Map<String, String>> requiredSchemaForFileAndRenamedColumns = getSchemaHandler().getRequiredSchemaForFileAndRenamedColumns(filePath);
      fileOutputSchema = requiredSchemaForFileAndRenamedColumns.getLeft();
      renamedColumns = requiredSchemaForFileAndRenamedColumns.getRight();
    }
    if (keyFilterOpt.isEmpty()) {
      return reader.getIndexedRecordIterator(dataSchema, fileOutputSchema, renamedColumns);
    }
    if (reader.supportKeyPredicate()) {
      List<String> keys = reader.extractKeys(keyFilterOpt);
      if (!keys.isEmpty()) {
        return reader.getIndexedRecordsByKeysIterator(keys, requiredSchema);
      }
    }
    if (reader.supportKeyPrefixPredicate()) {
      List<String> keyPrefixes = reader.extractKeyPrefixes(keyFilterOpt);
      if (!keyPrefixes.isEmpty()) {
        return reader.getIndexedRecordsByKeyPrefixIterator(keyPrefixes, requiredSchema);
      }
    }
    return reader.getIndexedRecordIterator(dataSchema, fileOutputSchema, renamedColumns);
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new HoodieAvroRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new OverwriteWithLatestMerger());
      case CUSTOM:
      default:
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.JAVA, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new IllegalArgumentException("No valid merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  @Override
  public SizeEstimator<BufferedRecord<IndexedRecord>> getRecordSizeEstimator() {
    return new AvroRecordSizeEstimator(getSchemaHandler().getRequiredSchema());
  }

  @Override
  public CustomSerializer<BufferedRecord<IndexedRecord>> getRecordSerializer() {
    return new BufferedRecordSerializer<>(new AvroRecordSerializer(versionId -> getRecordContext().decodeAvroSchema(versionId)));
  }

  @Override
  public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator,
                                                               Schema skeletonRequiredSchema,
                                                               ClosableIterator<IndexedRecord> dataFileIterator,
                                                               Schema dataRequiredSchema,
                                                               List<Pair<String, Object>> partitionFieldAndValues) {
    return new BootstrapIterator(skeletonFileIterator, skeletonRequiredSchema, dataFileIterator, dataRequiredSchema, partitionFieldAndValues);
  }

  /**
   * Iterator that traverses the skeleton file and the base file in tandem.
   * The iterator will only extract the fields requested in the provided schemas.
   */
  private static class BootstrapIterator implements ClosableIterator<IndexedRecord> {
    private final ClosableIterator<IndexedRecord> skeletonFileIterator;
    private final Schema skeletonRequiredSchema;
    private final ClosableIterator<IndexedRecord> dataFileIterator;
    private final Schema dataRequiredSchema;
    private final Schema mergedSchema;
    private final int skeletonFields;
    private final int[] partitionFieldPositions;
    private final Object[] partitionValues;

    public BootstrapIterator(ClosableIterator<IndexedRecord> skeletonFileIterator, Schema skeletonRequiredSchema,
                             ClosableIterator<IndexedRecord> dataFileIterator, Schema dataRequiredSchema,
                             List<Pair<String, Object>> partitionFieldAndValues) {
      this.skeletonFileIterator = skeletonFileIterator;
      this.skeletonRequiredSchema = skeletonRequiredSchema;
      this.dataFileIterator = dataFileIterator;
      this.dataRequiredSchema = dataRequiredSchema;
      this.mergedSchema = AvroSchemaUtils.mergeSchemas(skeletonRequiredSchema, dataRequiredSchema);
      this.skeletonFields = skeletonRequiredSchema.getFields().size();
      this.partitionFieldPositions = partitionFieldAndValues.stream().map(Pair::getLeft).map(field -> mergedSchema.getField(field).pos()).mapToInt(Integer::intValue).toArray();
      this.partitionValues = partitionFieldAndValues.stream().map(Pair::getValue).toArray();
    }

    @Override
    public void close() {
      skeletonFileIterator.close();
      dataFileIterator.close();
    }

    @Override
    public boolean hasNext() {
      checkState(dataFileIterator.hasNext() == skeletonFileIterator.hasNext(),
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!");
      return skeletonFileIterator.hasNext();
    }

    @Override
    public IndexedRecord next() {
      IndexedRecord skeletonRecord = skeletonFileIterator.next();
      IndexedRecord dataRecord = dataFileIterator.next();
      GenericRecord mergedRecord = new GenericData.Record(mergedSchema);

      for (Schema.Field skeletonField : skeletonRequiredSchema.getFields()) {
        Schema.Field sourceField = skeletonRecord.getSchema().getField(skeletonField.name());
        mergedRecord.put(skeletonField.pos(), skeletonRecord.get(sourceField.pos()));
      }
      for (Schema.Field dataField : dataRequiredSchema.getFields()) {
        Schema.Field sourceField = dataRecord.getSchema().getField(dataField.name());
        mergedRecord.put(dataField.pos() + skeletonFields, dataRecord.get(sourceField.pos()));
      }
      for (int i = 0; i < partitionFieldPositions.length; i++) {
        if (mergedRecord.get(partitionFieldPositions[i]) == null) {
          mergedRecord.put(partitionFieldPositions[i], partitionValues[i]);
        }
      }
      return mergedRecord;
    }
  }
}
