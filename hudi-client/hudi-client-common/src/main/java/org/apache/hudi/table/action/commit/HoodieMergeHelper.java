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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.CompactionContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.compaction.SortMergeCompactionHelper;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.SampleEstimator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.SortEngine;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.SortedAppendOnlyExternalSpillableMap;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.ExecutorFactory;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.AvroSchemaUtils.isStrictProjectionOf;

public class HoodieMergeHelper<T> extends BaseMergeHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeHelper.class);

  private HoodieMergeHelper() {
  }

  private static class MergeHelperHolder {
    private static final HoodieMergeHelper HOODIE_MERGE_HELPER = new HoodieMergeHelper<>();
  }

  public static HoodieMergeHelper newInstance() {
    return MergeHelperHolder.HOODIE_MERGE_HELPER;
  }

  private ClosableIterator<HoodieRecord> getBaseFileRecordIteratorInSortMergeCompaction(CompactionContext context, HoodieWriteConfig writeConfig, HoodieFileReader baseFileReader, Schema readSchema,
                                                                                        HoodieMergeHandle mergeHandle, String sorterBasePath,
                                                                                        SortEngine sortEngine, boolean bootstrap) throws IOException {
    StoragePath oldFilePath = mergeHandle.getOldFilePath();
    ClosableIterator rawRecordIter = baseFileReader.getRecordIterator(readSchema);
    if (!bootstrap && context.isBaseFileSorted()) {
      LOG.info("Base file: {} is sorted. No need to sort the records before merging", oldFilePath);
      return rawRecordIter;
    }
    ClosableIterator<HoodieRecord> wrapRecordItr = new CloseableMappingIterator<HoodieRecord, HoodieRecord>(rawRecordIter, record -> {
      HoodieTableConfig tableConfig = mergeHandle.getHoodieTableMetaClient().getTableConfig();
      try {
        Option<Pair<String, String>> simpleKeyGenFieldsOpt =
            tableConfig.populateMetaFields() ? Option.empty() : Option.of(Pair.of(tableConfig.getRecordKeyFieldProp(), tableConfig.getPartitionFieldProp()));
        return record.copy()
            .wrapIntoHoodieRecordPayloadWithParams(readSchema, tableConfig.getProps(), simpleKeyGenFieldsOpt, writeConfig.allowOperationMetadataField(), Option.of(mergeHandle.getPartitionPath()),
              tableConfig.populateMetaFields(), Option.empty());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to wrap record into HoodieRecordPayload", e);
      }
    });
    long fileSize = context.getBaseFileRealSize();
    long maxMemoryForBaseFileSorting = context.getMaxMemoryForCompaction() - context.getMaxMemoryForLogScanner();
    LOG.info("Base file: {} is not sorted. And it is bootstrap base file: {}."
        + "Sort the records before merging, its file-size: {}, max memory for sorting: {}", oldFilePath, bootstrap, fileSize, maxMemoryForBaseFileSorting);
    // sort the base file records
    // TODO: configure the sample related argument
    SizeEstimator<HoodieRecord> valueSizeEstimator = new SampleEstimator<>(new HoodieRecordSizeEstimator<>(readSchema));
    SizeEstimator<String> keySizeEstimator = new SampleEstimator<>(new DefaultSizeEstimator<>());
    return new SortedIterator(oldFilePath, wrapRecordItr, sorterBasePath, maxMemoryForBaseFileSorting, SortMergeCompactionHelper.DEFAULT_RECORD_COMPACTOR,
        keySizeEstimator, valueSizeEstimator, sortEngine);
  }

  static class SortedIterator implements ClosableIterator<HoodieRecord> {

    private final ClosableIterator<HoodieRecord> rawIterator;

    private final SortedAppendOnlyExternalSpillableMap<String, HoodieRecord> map;

    private ClosableIterator<HoodieRecord> sortedIterator;

    private final StoragePath baseFilePath;

    SortedIterator(StoragePath filePath, ClosableIterator<HoodieRecord> rawIterator, String sorterBasePath, long maxMemoryInBytes, Comparator<HoodieRecord> comparator,
                   SizeEstimator<String> keySizeEstimator, SizeEstimator<HoodieRecord> valueSizeEstimator, SortEngine sortEngine) {
      this.baseFilePath = filePath;
      this.rawIterator = rawIterator;
      Function<HoodieRecord, String> keyGetterFromRecord = HoodieRecord::getRecordKey;
      try {
        this.map = new SortedAppendOnlyExternalSpillableMap(sorterBasePath, maxMemoryInBytes, comparator, keySizeEstimator, valueSizeEstimator, sortEngine, keyGetterFromRecord);
      } catch (IOException e) {
        throw new HoodieException("Failed to initialize sorter", e);
      }
      init();
    }

    private void init() {
      LOG.info("Start to sort base-file: {}", baseFilePath);
      HoodieTimer hoodieTimer = HoodieTimer.create();
      hoodieTimer.startTimer();
      // scan all records and sort them
      this.map.putAll(rawIterator);
      this.rawIterator.close();
      this.sortedIterator = map.iterator();
      LOG.info("Finished sorting base-file: {} in {} ms", baseFilePath, hoodieTimer.endTimer());
    }

    @Override
    public boolean hasNext() {
      return sortedIterator.hasNext();
    }

    @Override
    public HoodieRecord next() {
      return sortedIterator.next();
    }

    @Override
    public void close() {
      sortedIterator.close();
    }
  }

  @Override
  public void runMerge(HoodieTable<?, ?, ?, ?> table, HoodieMergeHandle<?, ?, ?, ?> mergeHandle,
                       Option<CompactionContext> compactionContextOptionOpt) throws IOException {
    HoodieWriteConfig writeConfig = table.getConfig();
    HoodieBaseFile baseFile = mergeHandle.baseFileForMerge();

    HoodieRecord.HoodieRecordType recordType = table.getConfig().getRecordMerger().getRecordType();
    HoodieFileReader baseFileReader = HoodieIOFactory.getIOFactory(
            table.getStorage().newInstance(mergeHandle.getOldFilePath(), table.getStorageConf().newInstance()))
        .getReaderFactory(recordType)
        .getFileReader(writeConfig, mergeHandle.getOldFilePath());
    HoodieFileReader bootstrapFileReader = null;

    Schema writerSchema = mergeHandle.getWriterSchemaWithMetaFields();
    Schema readerSchema = baseFileReader.getSchema();

    // In case Advanced Schema Evolution is enabled we might need to rewrite currently
    // persisted records to adhere to an evolved schema
    Option<Function<HoodieRecord, HoodieRecord>> schemaEvolutionTransformerOpt =
        composeSchemaEvolutionTransformer(readerSchema, writerSchema, baseFile, writeConfig, table.getMetaClient());

    // Check whether the writer schema is simply a projection of the file's one, ie
    //   - Its field-set is a proper subset (of the reader schema)
    //   - There's no schema evolution transformation necessary
    boolean isPureProjection = schemaEvolutionTransformerOpt.isEmpty()
        && isStrictProjectionOf(readerSchema, writerSchema);
    // Check whether we will need to rewrite target (already merged) records into the
    // writer's schema
    boolean shouldRewriteInWriterSchema = !isPureProjection
        || baseFile.getBootstrapBaseFile().isPresent()
        || writeConfig.shouldUseExternalSchemaTransformation();

    HoodieExecutor<Void> executor = null;

    try {
      ClosableIterator<HoodieRecord> recordIterator;
      Schema recordSchema;
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        StoragePath bootstrapFilePath = baseFile.getBootstrapBaseFile().get().getStoragePath();
        HoodieStorage storage = table.getStorage().newInstance(
            bootstrapFilePath, table.getStorageConf().newInstance());
        bootstrapFileReader = HoodieIOFactory.getIOFactory(storage).getReaderFactory(recordType).newBootstrapFileReader(
            baseFileReader,
            HoodieIOFactory.getIOFactory(storage).getReaderFactory(recordType)
                .getFileReader(writeConfig, bootstrapFilePath),
            mergeHandle.getPartitionFields(),
            mergeHandle.getPartitionValues());
        recordSchema = mergeHandle.getWriterSchemaWithMetaFields();
        // Only open sorted merge compaction when merge-handle is for compaction and sorted merge is enabled
        if (compactionContextOptionOpt.isPresent() && compactionContextOptionOpt.get().isSortMergeCompaction()) {
          String externalSorterBasePath = writeConfig.getExternalSorterBasePath();
          recordIterator = getBaseFileRecordIteratorInSortMergeCompaction(compactionContextOptionOpt.get(), writeConfig,
              bootstrapFileReader, recordSchema, mergeHandle, externalSorterBasePath, SortEngine.HEAP, true);
        } else {
          recordIterator = (ClosableIterator<HoodieRecord>) bootstrapFileReader.getRecordIterator(recordSchema);
        }
      } else {
        // In case writer's schema is simply a projection of the reader's one we can read
        // the records in the projected schema directly
        recordSchema = isPureProjection ? writerSchema : readerSchema;
        // Only open sorted merge compaction when merge-handle is for compaction and sorted merge is enabled
        if (compactionContextOptionOpt.isPresent() && compactionContextOptionOpt.get().isSortMergeCompaction()) {
          String externalSorterBasePath = writeConfig.getExternalSorterBasePath();
          recordIterator = getBaseFileRecordIteratorInSortMergeCompaction(compactionContextOptionOpt.get(), writeConfig,
              baseFileReader, recordSchema, mergeHandle, externalSorterBasePath, SortEngine.HEAP, false);
        } else {
          recordIterator = (ClosableIterator<HoodieRecord>) baseFileReader.getRecordIterator(recordSchema);
        }
      }

      boolean isBufferingRecords = ExecutorFactory.isBufferingRecords(writeConfig);

      executor = ExecutorFactory.create(writeConfig, recordIterator, new UpdateHandler(mergeHandle), record -> {
        HoodieRecord newRecord;
        if (schemaEvolutionTransformerOpt.isPresent()) {
          newRecord = schemaEvolutionTransformerOpt.get().apply(record);
        } else if (shouldRewriteInWriterSchema) {
          newRecord = record.rewriteRecordWithNewSchema(recordSchema, writeConfig.getProps(), writerSchema);
        } else {
          newRecord = record;
        }

        // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
        //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
        //       it since these records will be put into queue of QueueBasedExecutorFactory.
        return isBufferingRecords ? newRecord.copy() : newRecord;
      }, table.getPreExecuteRunnable());

      executor.execute();
      // close base record iterator
      recordIterator.close();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      // NOTE: If executor is initialized it's responsible for gracefully shutting down
      //       both producer and consumer
      if (executor != null) {
        executor.shutdownNow();
        executor.awaitTermination();
      } else {
        baseFileReader.close();
        if (bootstrapFileReader != null) {
          bootstrapFileReader.close();
        }
        mergeHandle.close();
      }
    }
  }

  private Option<Function<HoodieRecord, HoodieRecord>> composeSchemaEvolutionTransformer(Schema recordSchema,
                                                                                         Schema writerSchema,
                                                                                         HoodieBaseFile baseFile,
                                                                                         HoodieWriteConfig writeConfig,
                                                                                         HoodieTableMetaClient metaClient) {
    Option<InternalSchema> querySchemaOpt = SerDeHelper.fromJson(writeConfig.getInternalSchema());
    // TODO support bootstrap
    if (querySchemaOpt.isPresent() && !baseFile.getBootstrapBaseFile().isPresent()) {
      // check implicitly add columns, and position reorder(spark sql may change cols order)
      InternalSchema querySchema = AvroSchemaEvolutionUtils.reconcileSchema(writerSchema, querySchemaOpt.get(), writeConfig.getBooleanOrDefault(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS));
      long commitInstantTime = Long.parseLong(baseFile.getCommitTime());
      InternalSchema fileSchema = InternalSchemaCache.getInternalSchemaByVersionId(commitInstantTime, metaClient);
      if (fileSchema.isEmptySchema() && writeConfig.getBoolean(HoodieCommonConfig.RECONCILE_SCHEMA)) {
        TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
        try {
          fileSchema = AvroInternalSchemaConverter.convert(tableSchemaResolver.getTableAvroSchema(true));
        } catch (Exception e) {
          throw new HoodieException(String.format("Failed to get InternalSchema for given versionId: %s", commitInstantTime), e);
        }
      }
      final InternalSchema writeInternalSchema = fileSchema;
      List<String> colNamesFromQuerySchema = querySchema.getAllColsFullName();
      List<String> colNamesFromWriteSchema = writeInternalSchema.getAllColsFullName();
      List<String> sameCols = colNamesFromWriteSchema.stream()
          .filter(f -> {
            int writerSchemaFieldId = writeInternalSchema.findIdByName(f);
            int querySchemaFieldId = querySchema.findIdByName(f);

            return colNamesFromQuerySchema.contains(f)
                && writerSchemaFieldId == querySchemaFieldId
                && writerSchemaFieldId != -1
                && Objects.equals(writeInternalSchema.findType(writerSchemaFieldId), querySchema.findType(querySchemaFieldId));
          })
          .collect(Collectors.toList());
      InternalSchema mergedSchema = new InternalSchemaMerger(writeInternalSchema, querySchema,
          true, false, false).mergeSchema();
      Schema newWriterSchema = AvroInternalSchemaConverter.convert(mergedSchema, writerSchema.getFullName());
      Schema writeSchemaFromFile = AvroInternalSchemaConverter.convert(writeInternalSchema, newWriterSchema.getFullName());
      boolean needToReWriteRecord = sameCols.size() != colNamesFromWriteSchema.size()
          || SchemaCompatibility.checkReaderWriterCompatibility(newWriterSchema, writeSchemaFromFile).getType() == org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
      if (needToReWriteRecord) {
        Map<String, String> renameCols = InternalSchemaUtils.collectRenameCols(writeInternalSchema, querySchema);
        return Option.of(record -> {
          return record.rewriteRecordWithNewSchema(
              recordSchema,
              writeConfig.getProps(),
              newWriterSchema, renameCols);
        });
      } else {
        return Option.empty();
      }
    } else {
      return Option.empty();
    }
  }
}
