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

package org.apache.hudi.index;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.CloseableFilterIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.exception.HoodieMultipleMatchesException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BaseWriteHelper;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.config.HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP;
import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.apache.hudi.common.util.HoodieRecordUtils.getOrderingFieldNames;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.core.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION;
import static org.apache.hudi.core.index.expression.HoodieExpressionIndex.IDENTITY_TRANSFORM;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.table.action.commit.HoodieDeleteHelper.createDeleteRecord;

/**
 * Hoodie Index Utilities.
 */
@Slf4j
public class HoodieIndexUtils {

  private static final String EXPRESSION_PAYLOAD_CLASS_NAME = "org.apache.spark.sql.hudi.command.payload.ExpressionPayload";

  /**
   * Fetches Pair of partition path and {@link HoodieBaseFile}s for interested partitions.
   *
   * @param partition   Partition of interest
   * @param hoodieTable Instance of {@link HoodieTable} of interest
   * @return the list of {@link HoodieBaseFile}
   */
  public static List<HoodieBaseFile> getLatestBaseFilesForPartition(String partition,
                                                                    HoodieTable hoodieTable) {
    Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    if (latestCommitTime.isPresent()) {
      return hoodieTable.getBaseFileOnlyView()
          .getLatestBaseFilesBeforeOrOn(partition, latestCommitTime.get().requestedTime())
          .collect(toList());
    }
    return Collections.emptyList();
  }

  /**
   * Check if the given schema type is supported for secondary index.
   * Supported types are: String (including CHAR), Integer types (Int, BigInt, Long, Short), and timestamp
   */
  @VisibleForTesting
  static boolean isSecondaryIndexSupportedType(HoodieSchema schema) {
    // Handle union types (nullable fields)
    if (schema.getType() == HoodieSchemaType.UNION) {
      // For union types, check if any of the types is supported
      return schema.getTypes().stream()
          .anyMatch(s -> s.getType() != HoodieSchemaType.NULL && isSecondaryIndexSupportedType(s));
    }

    // Check basic types
    switch (schema.getType()) {
      case STRING:
      case INT:
      case LONG:
      case DOUBLE:
      case FLOAT:
      case DATE:
      case TIME:
        return true;
      case TIMESTAMP:
        // LOCAL timestamps are not supported
        return ((HoodieSchema.Timestamp) schema).isUtcAdjusted();
      default:
        return false;
    }
  }

  /**
   * Fetches Pair of partition path and {@link FileSlice}s for interested partitions.
   *
   * @param partition   Partition of interest
   * @param hoodieTable Instance of {@link HoodieTable} of interest
   * @return the list of {@link FileSlice}
   */
  public static List<FileSlice> getLatestFileSlicesForPartition(
      final String partition,
      final HoodieTable hoodieTable) {
    Option<HoodieInstant> latestCommitTime = hoodieTable.getMetaClient().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    if (latestCommitTime.isPresent()) {
      return hoodieTable.getHoodieView()
          .getLatestFileSlicesBeforeOrOn(partition, latestCommitTime.get().requestedTime(), true)
          .collect(toList());
    }
    return Collections.emptyList();
  }

  /**
   * Fetches Pair of partition path and {@link HoodieBaseFile}s for interested partitions.
   *
   * @param partitions  list of partitions of interest
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @return the list of Pairs of partition path and fileId
   */
  public static List<Pair<String, HoodieBaseFile>> getLatestBaseFilesForAllPartitions(final List<String> partitions,
                                                                                      final HoodieEngineContext context,
                                                                                      final HoodieTable hoodieTable) {
    context.setJobStatus(HoodieIndexUtils.class.getSimpleName(), "Load latest base files from all partitions: " + hoodieTable.getConfig().getTableName());
    return context.flatMap(partitions, partitionPath -> {
      List<Pair<String, HoodieBaseFile>> filteredFiles =
          getLatestBaseFilesForPartition(partitionPath, hoodieTable).stream()
              .map(baseFile -> Pair.of(partitionPath, baseFile))
              .collect(toList());

      return filteredFiles.stream();
    }, Math.max(partitions.size(), 1));
  }

  /**
   * Get tagged record for the passed in {@link HoodieRecord}.
   *
   * @param record   instance of {@link HoodieRecord} for which tagging is requested
   * @param location {@link HoodieRecordLocation} for the passed in {@link HoodieRecord}
   * @return the tagged {@link HoodieRecord}
   */
  public static <R> HoodieRecord<R> tagAsNewRecordIfNeeded(HoodieRecord<R> record, Option<HoodieRecordLocation> location) {
    if (location.isPresent()) {
      // When you have a record in multiple files in the same partition, then <row key, record> collection
      // will have 2 entries with the same exact in memory copy of the HoodieRecord and the 2
      // separate filenames that the record is found in. This will result in setting
      // currentLocation 2 times and it will fail the second time. So creating a new in memory
      // copy of the hoodie record.
      HoodieRecord<R> newRecord = record.newInstance();
      newRecord.unseal();
      newRecord.setCurrentLocation(location.get());
      newRecord.seal();
      return newRecord;
    } else {
      return record;
    }
  }

  /**
   * Tag the record to an existing location. Not creating any new instance.
   */
  public static <R> HoodieRecord<R> tagRecord(HoodieRecord<R> record, HoodieRecordLocation location) {
    record.unseal();
    record.setCurrentLocation(location);
    record.seal();
    return record;
  }

  /**
   * Given a list of row keys and one file, return only row keys existing in that file.
   *
   * @param filePath            - File to filter keys from
   * @param candidateRecordKeys - Candidate keys to filter
   * @param storage
   * @return List of pairs of candidate keys and positions that are available in the file
   */
  public static Collection<Pair<String, Long>> filterKeysFromFile(StoragePath filePath,
                                                                  Set<String> candidateRecordKeys,
                                                                  HoodieStorage storage) throws HoodieIndexException {
    checkArgument(FSUtils.isBaseFile(filePath));
    if (candidateRecordKeys.isEmpty()) {
      return Collections.emptyList();
    }
    log.info("Going to filter {} keys from file {}", candidateRecordKeys.size(), filePath);
    try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, filePath)) {
      // Load all rowKeys from the file, to double-confirm
      HoodieTimer timer = HoodieTimer.start();
      Set<Pair<String, Long>> fileRowKeys = fileReader.filterRowKeys(candidateRecordKeys);
      log.info("Checked keys against file {}, in {} ms. #candidates ({}) #found ({})", filePath,
          timer.endTimer(), candidateRecordKeys.size(), fileRowKeys.size());
      log.debug("Keys matching for file {} => {}", filePath, fileRowKeys);
      return fileRowKeys;
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
  }

  /**
   * Check if the given commit timestamp is valid for the timeline.
   * <p>
   * The commit timestamp is considered to be valid if:
   * 1. the commit timestamp is present in the timeline, or
   * 2. the commit timestamp is less than the first commit timestamp in the timeline
   *
   * @param commitTimeline The timeline
   * @param commitTs       The commit timestamp to check
   * @return true if the commit timestamp is valid for the timeline
   */
  public static boolean checkIfValidCommit(HoodieTimeline commitTimeline, String commitTs) {
    return !commitTimeline.empty() && commitTimeline.containsOrBeforeTimelineStarts(commitTs);
  }

  public static HoodieIndex createUserDefinedIndex(HoodieWriteConfig config) {
    Object instance = ReflectionUtils.loadClass(config.getIndexClass(), config);
    if (!(instance instanceof HoodieIndex)) {
      throw new HoodieIndexException(config.getIndexClass() + " is not a subclass of HoodieIndex");
    }
    return (HoodieIndex) instance;
  }

  /**
   * Read existing records based on the given partition path and {@link HoodieRecordLocation} info.
   * <p>
   * This will perform merged read for MOR table, in case a FileGroup contains log files.
   *
   * @param locationsAndKeys the partition path and file ID pairs to read. With record keys, only the records with these
   *                         keys are returned, and the current location of a record holds the position of its row in
   *                         the base file
   * @return {@link HoodieRecord}s that have the current location being set.
   */
  private static <R> HoodieData<HoodieRecord<R>> getExistingRecords(
      HoodieData<Pair<Pair<String, String>, Option<Set<String>>>> locationsAndKeys, HoodieWriteConfig config, HoodieTable hoodieTable,
      ReaderContextFactory<R> readerContextFactory, HoodieSchema dataSchema) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    final Option<String> instantTime = metaClient
        .getActiveTimeline() // we need to include all actions and completed
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);
    if (instantTime.isEmpty()) {
      return hoodieTable.getContext().emptyHoodieData();
    }
    return locationsAndKeys.flatMap(locationAndKeys -> {
      Pair<String, String> p = locationAndKeys.getLeft();
      boolean withRowPositions = locationAndKeys.getRight().isPresent();
      Option<FileSlice> fileSliceOption = Option.fromJavaOptional(hoodieTable
          .getHoodieView()
          .getLatestMergedFileSlicesBeforeOrOn(p.getLeft(), instantTime.get())
          .filter(fileSlice -> fileSlice.getFileId().equals(p.getRight()))
          .findFirst());
      if (fileSliceOption.isEmpty()) {
        return Collections.emptyIterator();
      }
      Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
      FileSlice fileSlice = fileSliceOption.get();
      HoodieReaderContext<R> readerContext = readerContextFactory.getContext();
      RowPositionCounter<R> rowPositions = new RowPositionCounter<>(fileSlice.getBaseFile().isPresent());
      HoodieFileGroupReader<R> fileGroupReader = HoodieFileGroupReader.<R>builder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(metaClient)
          .withLatestCommitTime(instantTime.get())
          .withBaseFileOption(fileSlice.getBaseFile())
          .withLogFiles(fileSlice.getLogFiles())
          .withPartitionPath(fileSlice.getPartitionPath())
          .withDataSchema(dataSchema)
          .withRequestedSchema(dataSchema)
          .withInternalSchemaOpt(internalSchemaOption)
          .withProps(metaClient.getTableConfig().getProps())
          .withShouldUseRecordPosition(withRowPositions && config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS))
          .withFileGroupUpdateCallback(withRowPositions ? Option.of(rowPositions) : Option.empty())
          .build();
      try {
        if (withRowPositions) {
          Set<String> recordKeys = locationAndKeys.getRight().get();
          // The records with other keys are left out after their rows are counted
          return new CloseableFilterIterator<>(new CloseableMappingIterator<>(fileGroupReader.getClosableHoodieRecordIterator(), hoodieRecord -> {
            hoodieRecord.unseal();
            hoodieRecord.setCurrentLocation(
                new HoodieRecordLocation(fileSlice.getBaseInstantTime(), fileSlice.getFileId(), rowPositions.next()));
            hoodieRecord.seal();
            return hoodieRecord;
          }), hoodieRecord -> recordKeys.contains(hoodieRecord.getRecordKey()));
        }
        final HoodieRecordLocation currentLocation = new HoodieRecordLocation(fileSlice.getBaseInstantTime(), fileSlice.getFileId());
        return new CloseableMappingIterator<>(fileGroupReader.getClosableHoodieRecordIterator(), hoodieRecord -> {
          hoodieRecord.unseal();
          hoodieRecord.setCurrentLocation(currentLocation);
          hoodieRecord.seal();
          return hoodieRecord;
        });
      } catch (IOException ex) {
        throw new HoodieIOException("Unable to read file slice " + fileSlice, ex);
      }
    });
  }

  /**
   * Gives the records a file group reader returns the position of their row in the base file. The reader returns the
   * rows of the base file in order, before the records only the log files hold, and drops a deleted row after telling
   * this callback, so a returned record's row comes after every row returned or deleted before it.
   */
  private static class RowPositionCounter<R> implements BaseFileUpdateCallback<R> {
    private final boolean hasBaseFile;
    private long nextPosition = 0L;

    RowPositionCounter(boolean hasBaseFile) {
      this.hasBaseFile = hasBaseFile;
    }

    long next() {
      return hasBaseFile ? nextPosition++ : HoodieRecordLocation.INVALID_POSITION;
    }

    @Override
    public void onUpdate(String recordKey, BufferedRecord<R> previousRecord, BufferedRecord<R> mergedRecord) {
    }

    @Override
    public void onInsert(String recordKey, BufferedRecord<R> newRecord) {
    }

    @Override
    public void onDelete(String recordKey, BufferedRecord<R> previousRecord, HoodieOperation hoodieOperation) {
      if (previousRecord != null) {
        next();
      }
    }

    @Override
    public void onFailure(String recordKey) {
    }
  }

  /**
   * getExistingRecords will create records with expression payload so we overwrite the config.
   * Additionally, we don't want to restore this value because the write will fail later on.
   * We also need the keygenerator so we can figure out the partition path after expression payload
   * evaluates the merge.
   */
  private static Pair<HoodieWriteConfig, BaseKeyGenerator> getKeygenAndUpdatedWriteConfig(HoodieWriteConfig config, HoodieTableConfig tableConfig, boolean isExpressionPayload) {
    HoodieWriteConfig writeConfig = config;
    if (isExpressionPayload) {
      TypedProperties typedProperties = TypedProperties.copy(config.getProps());
      // set the payload class to table's payload class and not expresison payload. this will be used to read the existing records
      typedProperties.setProperty(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), tableConfig.getPayloadClass());
      typedProperties.setProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), tableConfig.getPayloadClass());
      writeConfig = HoodieWriteConfig.newBuilder().withProperties(typedProperties).build();
    }
    try {
      return Pair.of(writeConfig, (BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(writeConfig.getProps()));
    } catch (IOException e) {
      throw new RuntimeException("KeyGenerator must inherit from BaseKeyGenerator to update a records partition path using spark sql merge into", e);
    }
  }

  /**
   * Special merge handling for MIT
   * We need to wait until after merging before we can add meta fields because
   * ExpressionPayload does not allow rewriting
   */
  private static <R> Option<HoodieRecord<R>> mergeIncomingWithExistingRecordWithExpressionPayload(
      HoodieRecord<R> incoming,
      HoodieRecord<R> existing,
      HoodieSchema writeSchema,
      HoodieSchema writeSchemaWithMetaFields,
      HoodieWriteConfig config,
      BufferedRecordMerger<R> recordMerger,
      BaseKeyGenerator keyGenerator,
      RecordContext<R> incomingRecordContext,
      RecordContext<R> existingRecordContext,
      String[] orderingFieldNames,
      TypedProperties properties,
      DeleteContext deleteContext) throws IOException {
    BufferedRecord<R> incomingBufferedRecord = BufferedRecords.fromHoodieRecord(incoming, writeSchemaWithMetaFields, incomingRecordContext, properties, orderingFieldNames, deleteContext);
    BufferedRecord<R> existingBufferedRecord = BufferedRecords.fromHoodieRecord(existing, writeSchemaWithMetaFields, existingRecordContext, properties, orderingFieldNames, false);
    BufferedRecord<R> mergeResult = recordMerger.finalMerge(existingBufferedRecord, incomingBufferedRecord);
    if (mergeResult.isDelete()) {
      //the record was deleted
      return Option.empty();
    }
    if (mergeResult.getRecord() == null || mergeResult == existingBufferedRecord) {
      // SENTINEL case: the record did not match the merge case and should not be modified or there is no update to the record
      return Option.of((HoodieRecord<R>) new HoodieAvroIndexedRecord(HoodieRecord.SENTINEL));
    }

    //record is inserted or updated
    String partitionPath = inferPartitionPath(incoming, existing, writeSchemaWithMetaFields, keyGenerator, existingRecordContext, mergeResult);
    HoodieRecord<R> result = existingRecordContext.constructHoodieRecord(mergeResult, partitionPath);
    HoodieRecord<R> withMeta = result.prependMetaFields(writeSchema, writeSchemaWithMetaFields,
        new MetadataValues().setRecordKey(incoming.getRecordKey()).setPartitionPath(partitionPath), properties);
    return Option.of(withMeta.wrapIntoHoodieRecordPayloadWithParams(writeSchemaWithMetaFields, properties, Option.empty(),
        config.allowOperationMetadataField(), Option.empty(), false, Option.of(writeSchema)));
  }

  private static <R> String inferPartitionPath(HoodieRecord<R> incoming, HoodieRecord<R> existing, HoodieSchema recordSchema, BaseKeyGenerator keyGenerator,
                                               RecordContext<R> recordContext, BufferedRecord<R> resultingBufferedRecord) {
    R record = resultingBufferedRecord.getRecord();
    if (record == incoming.getData()) {
      return incoming.getPartitionPath();
    } else if (record == existing.getData()) {
      return existing.getPartitionPath();
    } else {
      // the merged record is not the same as either incoming or existing, so we need to compute the partition path
      return keyGenerator.getPartitionPath(recordContext.convertToAvroRecord(record, recordSchema));
    }
  }

  /**
   * Merge the incoming record with the matching existing record loaded via {@link HoodieFileGroupReader}. The existing record is the latest version in the table.
   */
  private static <R> Option<HoodieRecord<R>> mergeIncomingWithExistingRecord(
      HoodieRecord<R> incoming,
      HoodieRecord<R> existing,
      HoodieSchema writeSchema,
      HoodieSchema writeSchemaWithMetaFields,
      HoodieWriteConfig config,
      BufferedRecordMerger<R> recordMerger,
      BaseKeyGenerator keyGenerator,
      RecordContext<R> incomingRecordContext,
      RecordContext<R> existingRecordContext,
      String[] orderingFieldNames,
      TypedProperties properties,
      boolean isExpressionPayload,
      DeleteContext deleteContext) throws IOException {
    if (isExpressionPayload) {
      return mergeIncomingWithExistingRecordWithExpressionPayload(incoming, existing, writeSchema,
          writeSchemaWithMetaFields, config, recordMerger, keyGenerator, incomingRecordContext, existingRecordContext, orderingFieldNames, properties, deleteContext);
    } else {
      // prepend the hoodie meta fields as the incoming record does not have them
      BufferedRecord<R> incomingBufferedRecord = BufferedRecords.fromHoodieRecord(incoming, writeSchema, incomingRecordContext, properties, orderingFieldNames, deleteContext);
      BufferedRecord<R> existingBufferedRecord = BufferedRecords.fromHoodieRecord(existing, writeSchemaWithMetaFields, existingRecordContext, properties, orderingFieldNames, false);
      existingBufferedRecord.project(existingRecordContext.projectRecord(writeSchemaWithMetaFields, writeSchema));
      BufferedRecord<R> mergeResult = recordMerger.finalMerge(existingBufferedRecord, incomingBufferedRecord);

      if (mergeResult.isDelete()) {
        // the record was deleted
        return Option.empty();
      }
      String partitionPath = inferPartitionPath(incoming, existing, writeSchemaWithMetaFields, keyGenerator, existingRecordContext, mergeResult);
      if (HoodieRecordUtils.isPayloadClassDeprecated(ConfigUtils.getPayloadClass(properties))) {
        return Option.of(existingRecordContext.constructHoodieRecord(mergeResult, partitionPath));
      } else {
        HoodieRecord<R> result = existingRecordContext.constructHoodieRecord(mergeResult, partitionPath);
        HoodieRecord<R> resultWithMetaFields = result.prependMetaFields(writeSchema, writeSchemaWithMetaFields,
            new MetadataValues().setRecordKey(incoming.getRecordKey()).setPartitionPath(partitionPath), properties);
        // the merged record needs to be converted back to the original payload
        return Option.of(resultWithMetaFields.wrapIntoHoodieRecordPayloadWithParams(writeSchemaWithMetaFields, properties, Option.empty(),
            config.allowOperationMetadataField(), Option.empty(), false, Option.of(writeSchema)));
      }
    }
  }

  /**
   * Merges incoming records with the records they match in the table, which are read from the latest merged file
   * slices with the payload class of the table, as the incoming records of a Spark SQL MERGE INTO carry an
   * expression payload.
   */
  private static class ExistingRecordMerger<R> implements Serializable {
    private final HoodieWriteConfig updatedConfig;
    private final BaseKeyGenerator keyGenerator;
    private final HoodieSchema writerSchema;
    private final HoodieSchema writerSchemaWithMetaFields;
    private final TypedProperties properties;
    private final RecordContext<R> incomingRecordContext;
    private final ReaderContextFactory<R> readerContextFactoryForExistingRecords;
    private final RecordContext<R> existingRecordContext;
    private final BufferedRecordMerger<R> recordMerger;
    private final String[] orderingFieldsArray;
    private final boolean isExpressionPayload;
    private final DeleteContext deleteContext;

    ExistingRecordMerger(HoodieWriteConfig config, HoodieTable hoodieTable, HoodieReaderContext<R> readerContext, HoodieSchema writerSchema) {
      this.isExpressionPayload = EXPRESSION_PAYLOAD_CLASS_NAME.equals(config.getPayloadClass());
      Pair<HoodieWriteConfig, BaseKeyGenerator> keyGeneratorWriteConfigOpt =
          getKeygenAndUpdatedWriteConfig(config, hoodieTable.getMetaClient().getTableConfig(), isExpressionPayload);
      this.updatedConfig = keyGeneratorWriteConfigOpt.getLeft();
      this.keyGenerator = keyGeneratorWriteConfigOpt.getRight();
      this.writerSchema = writerSchema;
      // define the buffered record merger.
      this.properties = readerContext.getMergeProps(updatedConfig.getProps());
      this.incomingRecordContext = readerContext.getRecordContext();
      // Create a reader context for the existing records. In the case of merge-into commands, the incoming records
      // can be using an expression payload so here we rely on the table's configured payload class if it is required.
      this.readerContextFactoryForExistingRecords = (ReaderContextFactory<R>) hoodieTable.getContext()
          .getReaderContextFactoryForWrite(hoodieTable.getMetaClient(), config.getRecordMerger().getRecordType(), hoodieTable.getMetaClient().getTableConfig().getProps());
      this.existingRecordContext = readerContextFactoryForExistingRecords.getContext().getRecordContext();
      this.writerSchemaWithMetaFields = HoodieSchemaUtils.addMetadataFields(writerSchema, updatedConfig.allowOperationMetadataField());
      HoodieSchemaCache.intern(writerSchema);
      HoodieSchemaCache.intern(writerSchemaWithMetaFields);
      List<String> orderingFieldNames = getOrderingFieldNames(
          readerContext.getMergeMode(), hoodieTable.getMetaClient());
      this.recordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          readerContext.getMergeMode(),
          false,
          readerContext.getRecordMerger(),
          writerSchema,
          Option.ofNullable(Pair.of(hoodieTable.getMetaClient().getTableConfig().getPayloadClass(), hoodieTable.getConfig().getPayloadClass())),
          properties,
          hoodieTable.getMetaClient().getTableConfig().getPartialUpdateMode());
      this.orderingFieldsArray = orderingFieldNames.toArray(new String[0]);
      this.deleteContext = DeleteContext.fromRecordSchema(properties, writerSchema);
    }

    /**
     * Reads the existing records with the meta fields and current writer schema as the output schema.
     *
     * @see HoodieIndexUtils#getExistingRecords
     */
    HoodieData<HoodieRecord<R>> getExistingRecords(HoodieData<Pair<Pair<String, String>, Option<Set<String>>>> locationsAndKeys, HoodieTable hoodieTable) {
      return HoodieIndexUtils.getExistingRecords(
          locationsAndKeys, updatedConfig, hoodieTable, readerContextFactoryForExistingRecords, writerSchemaWithMetaFields);
    }

    /**
     * Creates the record that deletes the existing record an incoming record matches. Unless the write orders records
     * by commit time, it has the ordering value of the incoming record, as the append of a delete clause writes, or
     * the default value when the existing record has none.
     */
    HoodieRecord<R> createDelete(HoodieRecord<R> incoming, HoodieRecord<R> existing) {
      return updatedConfig.getRecordMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING
          || OrderingValues.isCommitTimeOrderingValue(existing.getOrderingValue(writerSchemaWithMetaFields, properties, orderingFieldsArray))
          ? createDeleteRecord(updatedConfig, incoming.getKey())
          : new HoodieEmptyRecord<>(incoming.getKey(), HoodieOperation.DELETE,
              incoming.getOrderingValue(writerSchema, properties, orderingFieldsArray), HoodieRecordType.AVRO);
    }

    /**
     * @see #mergeIncomingWithExistingRecord
     */
    Option<HoodieRecord<R>> merge(HoodieRecord<R> incoming, HoodieRecord<R> existing) throws IOException {
      return mergeIncomingWithExistingRecord(incoming, existing, writerSchema, writerSchemaWithMetaFields, updatedConfig,
          recordMerger, keyGenerator, incomingRecordContext, existingRecordContext, orderingFieldsArray, properties, isExpressionPayload, deleteContext);
    }
  }

  /**
   * Keeps one incoming record per record key of the ones that wait for a merged lookup, as
   * {@link HoodieData#distinctWithKey} does, with the file groups of the rows that more than one incoming record
   * matches. The index pairs every incoming record with each row it locates the key at, and the rows of a key differ
   * in file group or position, so more than one incoming record shows as more than one pair for a row, while one
   * record located at several rows does not. A row the merged lookup reads may no longer be live (a merge-on-read
   * log can delete it), so those file groups are checked against the existing record after the lookup. The incoming
   * records the index already tagged count as well, as one of them and one that waits for the merged lookup can match
   * the same row, and fail with a {@link HoodieMultipleMatchesException} right away when they are all the matches of
   * the row. See {@link HoodieWriteConfig#UPSERT_FAIL_ON_MULTIPLE_MATCHES}.
   */
  private static <R> HoodieData<Pair<HoodieRecord<R>, Set<Pair<String, String>>>> distinctLocatedRecordsWithMultipleMatches(
      HoodieData<Pair<HoodieRecord<R>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations, int parallelism) {
    return incomingRecordsAndLocations
        .filter(p -> p.getRight().isPresent() || p.getLeft().isCurrentLocationKnown())
        .mapToPair(p -> {
          HoodieRecord<R> record = p.getLeft();
          boolean forMergedLookup = p.getRight().isPresent();
          HoodieRecordLocation location = forMergedLookup ? p.getRight().get() : record.getCurrentLocation();
          String partitionPath = forMergedLookup ? p.getRight().get().getPartitionPath() : record.getPartitionPath();
          // per row: the number of matches, and whether one of them waits for the merged lookup
          Map<Pair<Pair<String, String>, Long>, Pair<Long, Boolean>> rowMatches = new HashMap<>();
          rowMatches.put(Pair.of(Pair.of(partitionPath, location.getFileId()), location.getPosition()), Pair.of(1L, forMergedLookup));
          return Pair.of(record.getRecordKey(), Pair.of(forMergedLookup ? Option.of(record) : Option.<HoodieRecord<R>>empty(), rowMatches));
        })
        .reduceByKey((left, right) -> {
          Map<Pair<Pair<String, String>, Long>, Pair<Long, Boolean>> rowMatches = new HashMap<>(left.getRight());
          right.getRight().forEach((row, matches) -> rowMatches.merge(row, matches,
              (a, b) -> Pair.of(a.getLeft() + b.getLeft(), a.getRight() || b.getRight())));
          return Pair.of(left.getLeft().isPresent() ? left.getLeft() : right.getLeft(), rowMatches);
        }, parallelism)
        .map(p -> {
          Set<Pair<String, String>> fileGroupsMatchedMoreThanOnce = new HashSet<>();
          for (Map.Entry<Pair<Pair<String, String>, Long>, Pair<Long, Boolean>> rowMatches : p.getRight().getRight().entrySet()) {
            if (rowMatches.getValue().getLeft() > 1) {
              Pair<String, String> fileGroup = rowMatches.getKey().getLeft();
              if (!rowMatches.getValue().getRight()) {
                throw BaseWriteHelper.multipleMatchesException(p.getLeft(), fileGroup.getLeft(), fileGroup.getRight());
              }
              fileGroupsMatchedMoreThanOnce.add(fileGroup);
            }
          }
          return p.getRight().getLeft().map(record -> Pair.of(record, fileGroupsMatchedMoreThanOnce));
        })
        .filter(Option::isPresent)
        .map(Option::get);
  }

  /**
   * Merge tagged incoming records with existing records in case of partition path updated.
   */
  public static <R> HoodieData<HoodieRecord<R>> mergeForPartitionUpdatesAndDeletionsIfNeeded(HoodieData<Pair<HoodieRecord<R>,
                                                                                             Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations,
                                                                                             boolean shouldUpdatePartitionPath,
                                                                                             HoodieWriteConfig config,
                                                                                             HoodieTable hoodieTable,
                                                                                             HoodieReaderContext<R> readerContext,
                                                                                             HoodieSchema writerSchema) {
    ExistingRecordMerger<R> merger = new ExistingRecordMerger<>(config, hoodieTable, readerContext, writerSchema);
    HoodieWriteConfig updatedConfig = merger.updatedConfig;
    // completely new records
    HoodieData<HoodieRecord<R>> taggedNewRecords = incomingRecordsAndLocations.filter(p -> !p.getRight().isPresent()).map(Pair::getLeft);
    // the records found in existing base files
    boolean failOnMultipleMatches = updatedConfig.shouldFailUpsertOnMultipleMatches() && hoodieTable.getIndex().locatesOnlyMatchingRecords();
    HoodieData<Pair<HoodieRecord<R>, Set<Pair<String, String>>>> untaggedUpdatingRecords = failOnMultipleMatches
        ? distinctLocatedRecordsWithMultipleMatches(incomingRecordsAndLocations, updatedConfig.getGlobalIndexReconcileParallelism())
        : incomingRecordsAndLocations.filter(p -> p.getRight().isPresent()).map(Pair::getLeft)
            .distinctWithKey(HoodieRecord::getRecordKey, updatedConfig.getGlobalIndexReconcileParallelism())
            .map(record -> Pair.of(record, Collections.<Pair<String, String>>emptySet()));
    // the tagging partitions and locations
    // NOTE: The incoming records may only differ in record position, however, for the purpose of
    //       merging in case of partition updates, it is safe to ignore the record positions.
    HoodieData<Pair<String, String>> globalLocations = incomingRecordsAndLocations
        .filter(p -> p.getRight().isPresent())
        .map(p -> Pair.of(p.getRight().get().getPartitionPath(), p.getRight().get().getFileId()))
        .distinct(updatedConfig.getGlobalIndexReconcileParallelism());
    // merged existing records with current locations being set
    HoodieData<HoodieRecord<R>> existingRecords =
        merger.getExistingRecords(globalLocations.map(location -> Pair.of(location, Option.empty())), hoodieTable);
    HoodieData<HoodieRecord<R>> taggedUpdatingRecords = untaggedUpdatingRecords.mapToPair(r -> Pair.of(r.getLeft().getRecordKey(), r))
        .leftOuterJoin(existingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
        .values().flatMap(entry -> {
          HoodieRecord<R> incoming = entry.getLeft().getLeft();
          Option<HoodieRecord<R>> existingOpt = entry.getRight();
          if (!existingOpt.isPresent()) {
            // existing record not found (e.g., due to delete log not merged to base file): tag as a new record
            return Collections.singletonList(incoming).iterator();
          }
          HoodieRecord<R> existing = existingOpt.get();
          Pair<String, String> existingFileGroup = Pair.of(existing.getPartitionPath(), existing.getCurrentLocation().getFileId());
          if (entry.getLeft().getRight().contains(existingFileGroup)) {
            throw BaseWriteHelper.multipleMatchesException(incoming.getRecordKey(), existingFileGroup.getLeft(), existingFileGroup.getRight());
          }

          Option<HoodieRecord<R>> mergedOpt = merger.merge(incoming, existing);
          if (!mergedOpt.isPresent()) {
            // merge resulted in delete: force tag the incoming to the old partition
            return Collections.singletonList(tagRecord(incoming.newInstance(existing.getKey()), existing.getCurrentLocation())).iterator();
          }
          HoodieRecord<R> merged = mergedOpt.get();
          if (merged.getData().equals(HoodieRecord.SENTINEL)) {
            //if MIT update and it doesn't match any merge conditions, we omit the record
            return Collections.emptyIterator();
          }
          if (Objects.equals(merged.getPartitionPath(), existing.getPartitionPath())) {
            // merged record has the same partition: route the merged result to the current location as an update
            return Collections.singletonList(tagRecord(merged, existing.getCurrentLocation())).iterator();
          } else if (shouldUpdatePartitionPath) {
            // merged record has a different partition: issue a delete to the old partition and insert the merged record to the new partition
            HoodieRecord<R> deleteRecord = createDeleteRecord(updatedConfig, existing.getKey());
            deleteRecord.setIgnoreIndexUpdate(true);
            return Arrays.asList(tagRecord(deleteRecord, existing.getCurrentLocation()), merged).iterator();
          } else {
            // merged record has a different partition path but shouldUpdatePartitionPath is false, treat as an update to the existing location
            return Collections.singletonList(merged.newInstance(existing.getKey())).iterator();
          }
        });
    if (failOnMultipleMatches) {
      // The check leaves one incoming record per key. Merged with each row of a key that a file group holds more
      // than once, it gives a record per row, which the check after tagging would take for more than one incoming
      // record. The write of a file group keeps one record per key, so keep one per file group.
      taggedUpdatingRecords = taggedUpdatingRecords.distinctWithKey(
          r -> Pair.of(Pair.of(r.getRecordKey(), r.getPartitionPath()), r.isCurrentLocationKnown() ? r.getCurrentLocation().getFileId() : ""),
          updatedConfig.getGlobalIndexReconcileParallelism());
    }
    return taggedUpdatingRecords.union(taggedNewRecords);
  }

  /**
   * Resolves each located record of a Spark SQL MERGE INTO write against the record it matches, read from the
   * latest merged file slice of the located file group, so that its matched clauses see the fields of the target
   * table. A record the clauses update is replaced with its new version and a record they delete with a delete
   * record, both at the location and row position the index tagged, and a record none of them accepts is dropped.
   * A record whose location holds no live record with its key, as a deleted copy in a base file or the bucket of a
   * bucket index can get it tagged, is replaced with what its not-matched clauses make of it at the same location,
   * and dropped when none of them accepts it or the write does not order records by commit time. The records of any
   * other write are returned as they are.
   */
  public static <R> HoodieData<HoodieRecord<R>> mergeLocatedExpressionPayloadRecords(HoodieData<HoodieRecord<R>> taggedRecords,
                                                                                     HoodieWriteConfig config,
                                                                                     HoodieTable hoodieTable,
                                                                                     int parallelism) {
    if (!EXPRESSION_PAYLOAD_CLASS_NAME.equals(config.getPayloadClass())) {
      return taggedRecords;
    }
    HoodieReaderContext<R> readerContext = ((ReaderContextFactory<R>) hoodieTable.getContext()
        .getReaderContextFactoryForWrite(hoodieTable.getMetaClient(), config.getRecordMerger().getRecordType(), config.getProps())).getContext();
    readerContext.initRecordMergerForIngestion(config.getProps());
    HoodieSchema writerSchema = HoodieSchema.parse(config.getWriteSchema());
    ExistingRecordMerger<R> merger = new ExistingRecordMerger<>(config, hoodieTable, readerContext, writerSchema);
    TypedProperties notMatchedProps = TypedProperties.copy(config.getProps());
    notMatchedProps.setProperty(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, "false");
    HoodieData<HoodieRecord<R>> locatedRecords = taggedRecords.filter(HoodieIndexUtils::isLocatedExpressionPayloadRecord);
    HoodieData<Pair<Pair<String, String>, Option<Set<String>>>> locationsAndKeys = locatedRecords
        .mapToPair(record -> {
          Set<String> recordKeys = new HashSet<>();
          recordKeys.add(record.getRecordKey());
          return Pair.of(partitionPathAndFileId(record), recordKeys);
        })
        .reduceByKey((left, right) -> {
          left.addAll(right);
          return left;
        }, parallelism)
        .map(entry -> Pair.of(entry.getLeft(), Option.of(entry.getRight())));
    HoodieData<HoodieRecord<R>> resolvedRecords = locatedRecords.mapToPair(record -> Pair.of(fileGroupAndRecordKey(record), record))
        .groupByKey()
        .leftOuterJoin(merger.getExistingRecords(locationsAndKeys, hoodieTable)
            .mapToPair(record -> Pair.of(fileGroupAndRecordKey(record), record)).groupByKey())
        .values()
        .flatMap(entry -> pairWithExistingRecords(entry.getLeft(), entry.getRight()).iterator())
        .flatMap(entry -> {
          HoodieRecord<R> incoming = entry.getLeft();
          if (!entry.getRight().isPresent()) {
            if (config.getRecordMergeMode() != RecordMergeMode.COMMIT_TIME_ORDERING) {
              // An insert at the location could lose to the deleted copy of the key the location holds
              return Collections.emptyIterator();
            }
            Option<IndexedRecord> inserted = ((HoodieRecordPayload<?>) incoming.getData()).getInsertValue(writerSchema.toAvroSchema(), notMatchedProps);
            if (!inserted.isPresent() || inserted.get().equals(HoodieRecord.SENTINEL)) {
              return Collections.emptyIterator();
            }
            HoodieRecord<R> insertRecord = (HoodieRecord<R>) new HoodieAvroIndexedRecord(incoming.getKey(), inserted.get());
            return Collections.singletonList(tagRecord(insertRecord, incoming.getCurrentLocation())).iterator();
          }
          Option<HoodieRecord<R>> mergedOpt = merger.merge(incoming, entry.getRight().get());
          if (mergedOpt.isPresent() && mergedOpt.get().getData().equals(HoodieRecord.SENTINEL)) {
            return Collections.emptyIterator();
          }
          HoodieRecord<R> resolved = mergedOpt.isPresent()
              ? mergedOpt.get().newInstance(incoming.getKey()) : merger.createDelete(incoming, entry.getRight().get());
          return Collections.singletonList(tagRecord(resolved, incoming.getCurrentLocation())).iterator();
        });
    return taggedRecords.filter(record -> !isLocatedExpressionPayloadRecord(record)).union(resolvedRecords);
  }

  /**
   * Pairs the located records of one record key in one file group with the records the file group holds for the key.
   * A file group can hold a key more than once, and an index that reads the base file locates the incoming record at
   * each of its rows, so each such row is paired with the located record at its position. When the positions do not
   * tell the rows apart, one located record is paired with one record of the key.
   */
  private static <R> List<Pair<HoodieRecord<R>, Option<HoodieRecord<R>>>> pairWithExistingRecords(
      Iterable<HoodieRecord<R>> locatedRecords, Option<Iterable<HoodieRecord<R>>> existingRecords) {
    HoodieRecord<R> firstLocated = locatedRecords.iterator().next();
    if (!existingRecords.isPresent()) {
      return Collections.singletonList(Pair.of(firstLocated, Option.empty()));
    }
    Map<Long, HoodieRecord<R>> locatedByPosition = new HashMap<>();
    for (HoodieRecord<R> located : locatedRecords) {
      long position = located.getCurrentLocation().getPosition();
      if (HoodieRecordLocation.isPositionValid(position)) {
        locatedByPosition.putIfAbsent(position, located);
      }
    }
    List<Pair<HoodieRecord<R>, Option<HoodieRecord<R>>>> pairs = new ArrayList<>();
    for (HoodieRecord<R> existing : existingRecords.get()) {
      HoodieRecord<R> located = locatedByPosition.get(existing.getCurrentLocation().getPosition());
      if (located != null) {
        pairs.add(Pair.of(located, Option.of(existing)));
      }
    }
    if (pairs.isEmpty()) {
      pairs.add(Pair.of(firstLocated, Option.of(existingRecords.get().iterator().next())));
    }
    return pairs;
  }

  private static boolean isLocatedExpressionPayloadRecord(HoodieRecord<?> record) {
    return record.isCurrentLocationKnown() && record.getData() instanceof HoodieRecordPayload
        && EXPRESSION_PAYLOAD_CLASS_NAME.equals(record.getData().getClass().getName());
  }

  private static Pair<String, String> partitionPathAndFileId(HoodieRecord<?> record) {
    return Pair.of(record.getPartitionPath(), record.getCurrentLocation().getFileId());
  }

  // The file IDs of the bucket index can repeat across partitions
  private static Pair<Pair<String, String>, String> fileGroupAndRecordKey(HoodieRecord<?> record) {
    return Pair.of(partitionPathAndFileId(record), record.getRecordKey());
  }

  /**
   * Tags the incoming records with their existing locations if any is found.
   * Global indexing can support moving records across partitions. To do so, the tagging logic will result in a delete to the old partition and an insert into the new partition. When this happens
   * with Merge-on-Read tables, the record key is present in multiple locations until the file slices are compacted. If the index relies on reading the keys directly from the file slices, then
   * `mayContainDuplicateLookup` must be set to true to account for this. If the lookup will only contain the latest location of the record key, then `mayContainDuplicateLookup` is set to false.
   * @param incomingRecords The new data being written to the table
   * @param keyAndExistingLocations The existing locations of the records in the table
   * @param mayContainDuplicateLookup Set to true if the existing locations may contain multiple entries for the same record key
   * @param shouldUpdatePartitionPath When true, the index will handle partition path updates by merging the incoming record with the existing record and determining if the partition path has changed.
   *                                  When false, the incoming record will always be tagged to the existing location's partition path.
   * @param config the writer's configuration
   * @param table the table that is being updated
   * @return the incoming records tagged with their existing locations if any is found
   * @param <R> the type of the data in the record
   */
  public static <R> HoodieData<HoodieRecord<R>> tagGlobalLocationBackToRecords(
      HoodieData<HoodieRecord<R>> incomingRecords,
      HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations,
      boolean mayContainDuplicateLookup,
      boolean shouldUpdatePartitionPath,
      HoodieWriteConfig config,
      HoodieTable table) {
    HoodiePairData<String, HoodieRecord<R>> keyAndIncomingRecords =
        incomingRecords.mapToPair(record -> Pair.of(record.getRecordKey(), record));

    ReaderContextFactory<R> readerContextFactory = (ReaderContextFactory<R>) table.getContext()
        .getReaderContextFactoryForWrite(table.getMetaClient(), config.getRecordMerger().getRecordType(), config.getProps());
    HoodieReaderContext<R> readerContext = readerContextFactory.getContext();
    readerContext.initRecordMergerForIngestion(config.getProps());
    TypedProperties properties = readerContext.getMergeProps(config.getProps());
    HoodieSchema writerSchema = HoodieSchema.parse(config.getWriteSchema());
    boolean isCommitTimeOrdered = readerContext.getMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING;
    // if the index is not updating the partition of the record, and the table is COW, then we do not need to do merging at
    // this phase since the writer path will merge when rewriting the files as part of the upsert operation.
    boolean requiresMergingWithOlderRecordVersion = shouldUpdatePartitionPath
        || (!isCommitTimeOrdered && table.getMetaClient().getTableConfig().getTableType() == HoodieTableType.MERGE_ON_READ);
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(properties, writerSchema);

    // Pair of incoming record and the global location if meant for merged lookup in later stage
    HoodieData<Pair<HoodieRecord<R>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations
        = keyAndIncomingRecords.leftOuterJoin(keyAndExistingLocations).values()
        .map(v -> {
          final HoodieRecord<R> incomingRecord = v.getLeft();
          Option<HoodieRecordGlobalLocation> currentLocOpt = Option.ofNullable(v.getRight().orElse(null));
          if (currentLocOpt.isPresent()) {
            HoodieRecordGlobalLocation currentLoc = currentLocOpt.get();
            boolean shouldDoMergedLookUpThenTag = mayContainDuplicateLookup // handle event time ordering updates
                || shouldUpdatePartitionPath && !Objects.equals(incomingRecord.getPartitionPath(), currentLoc.getPartitionPath())  // handle partition updates
                || (!isCommitTimeOrdered && incomingRecord.isDelete(deleteContext, properties)); // handle event time ordering deletes
            if (requiresMergingWithOlderRecordVersion && shouldDoMergedLookUpThenTag) {
              // the pair's right side is a non-empty Option, which indicates that a merged lookup will be performed
              // at a later stage.
              return Pair.of(incomingRecord, currentLocOpt);
            } else {
              // - When update partition path is set to false,
              //   the incoming record will be tagged to the existing record's partition regardless of being equal or not.
              // - When update partition path is set to true,
              //   the incoming record will be tagged to the existing record's partition
              //   when partition is not updated and the look-up won't have duplicates (e.g. COW, or using RLI).
              return Pair.of(createNewTaggedHoodieRecord(incomingRecord, currentLoc), Option.empty());
            }
          } else {
            return Pair.of(incomingRecord, Option.empty());
          }
        });
    return requiresMergingWithOlderRecordVersion
        ? mergeForPartitionUpdatesAndDeletionsIfNeeded(incomingRecordsAndLocations, shouldUpdatePartitionPath, config, table, readerContext, writerSchema)
        : incomingRecordsAndLocations.map(Pair::getLeft);
  }

  public static <R> HoodieRecord<R> createNewTaggedHoodieRecord(HoodieRecord<R> oldRecord, HoodieRecordGlobalLocation location) {
    HoodieKey recordKey = new HoodieKey(oldRecord.getRecordKey(), location.getPartitionPath());
    return tagRecord(oldRecord.newInstance(recordKey), location);
  }

  /**
   * Register a metadata index.
   * Index definitions are stored in user-specified path or, by default, in .hoodie/.index_defs/index.json.
   * For the first time, the index definition file will be created if not exists.
   * For the second time, the index definition file will be updated if exists.
   * Table Config is updated if necessary.
   */
  public static void register(HoodieTableMetaClient metaClient, HoodieIndexDefinition indexDefinition) {
    log.info("Registering index {} of using {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
    // build HoodieIndexMetadata and then add to index definition file
    metaClient.buildIndexDefinition(indexDefinition);
  }

  static HoodieIndexDefinition getSecondaryOrExpressionIndexDefinition(HoodieTableMetaClient metaClient, String userIndexName, String indexType, Map<String, Map<String, String>> columns,
                                                                       Map<String, String> options, Map<String, String> tableProperties) throws Exception {
    String fullIndexName = indexType.equals(PARTITION_NAME_SECONDARY_INDEX)
        ? PARTITION_NAME_SECONDARY_INDEX_PREFIX + userIndexName
        : PARTITION_NAME_EXPRESSION_INDEX_PREFIX + userIndexName;
    HoodieTableVersion tableVersion = metaClient.getTableConfig().getTableVersion();
    HoodieIndexVersion indexVersion = indexType.equals(PARTITION_NAME_SECONDARY_INDEX)
        ? HoodieIndexVersion.getCurrentVersion(tableVersion, MetadataPartitionType.SECONDARY_INDEX)
        : HoodieIndexVersion.getCurrentVersion(tableVersion, MetadataPartitionType.EXPRESSION_INDEX);
    if (indexExists(metaClient, fullIndexName)) {
      throw new HoodieMetadataIndexException("Index already exists: " + userIndexName);
    }
    checkArgument(columns.size() == 1, "Only one column can be indexed for functional or secondary index.");

    // This will throw an exception if not eligible
    validateEligibilityForSecondaryOrExpressionIndex(metaClient, indexType, tableProperties, columns, userIndexName);

    return HoodieIndexDefinition.newBuilder()
        .withIndexName(fullIndexName)
        .withIndexType(indexType)
        .withIndexFunction(options.getOrDefault(EXPRESSION_OPTION, IDENTITY_TRANSFORM))
        .withSourceFields(new ArrayList<>(columns.keySet()))
        .withIndexOptions(options)
        .withVersion(indexVersion)
        .build();
  }

  static boolean indexExists(HoodieTableMetaClient metaClient, String indexName) {
    return metaClient.getTableConfig().getMetadataPartitions().stream().anyMatch(partition -> partition.equals(indexName));
  }

  static void validateEligibilityForSecondaryOrExpressionIndex(HoodieTableMetaClient metaClient,
                                                               String indexType,
                                                               Map<String, String> options,
                                                               Map<String, Map<String, String>> columns,
                                                               String userIndexName) throws Exception {
    HoodieSchema tableSchema = new TableSchemaResolver(metaClient).getTableSchema();
    List<String> sourceFields = new ArrayList<>(columns.keySet());
    String columnName = sourceFields.get(0); // We know there's only one column from the check above

    // First check if the field exists
    Pair<String, HoodieSchemaField> fieldSchemaPair = HoodieSchemaUtils.getNestedField(tableSchema, columnName)
        .orElseThrow(() -> new HoodieMetadataIndexException(String.format(
            "Cannot create %s index '%s': Column '%s' does not exist in the table schema. "
                + "Please verify the column name and ensure it exists in the table.",
            indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? "secondary" : "expression",
            userIndexName, columnName)));

    HoodieSchema fieldSchema = fieldSchemaPair.getRight().schema();

    if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX)) {
      // Secondary Index Validation: Strict Allow-List/Whitelist
      if (!isSecondaryIndexSupportedType(fieldSchema)) {
        String actualType = fieldSchema.getType().toString();
        throw new HoodieMetadataIndexException(String.format(
            "Cannot create secondary index '%s': Column '%s' has unsupported data type '%s'. "
            + "Secondary indexes only support: STRING, CHAR, INT, BIGINT/LONG, SMALLINT, TINYINT, "
            + "FLOAT, DOUBLE, TIMESTAMP (including logical types timestampMillis, timestampMicros), "
            + "and DATE types. Please choose a column with one of these supported types.",
            userIndexName, columnName, actualType));
      }

      // Check if record index is enabled for secondary index
      boolean hasRecordIndex = metaClient.getTableConfig().getMetadataPartitions().stream()
          .anyMatch(partition -> partition.equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));
      boolean recordIndexEnabled = Boolean.parseBoolean(
          options.getOrDefault(GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), options.getOrDefault("hoodie.metadata.record.index.enable",
              GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.defaultValue().toString())));

      if (!hasRecordIndex && !recordIndexEnabled) {
        throw new HoodieMetadataIndexException(String.format(
            "Cannot create secondary index '%s': Record index is required for secondary indexes but is not enabled. "
            + "Please enable the record index by setting '%s' to 'true' in the index creation options, "
            + "or create a record index first using: CREATE INDEX record_index ON %s USING record_index",
            userIndexName, GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), metaClient.getTableConfig().getTableName()));
      }
    } else {
      // Expression Index Validation: Loose Deny-List/Blacklist
      if (fieldSchema.getNonNullType().getType().isComplex()) {
        throw new HoodieMetadataIndexException(String.format(
            "Cannot create expression index '%s': Column '%s' has unsupported data type '%s'. "
                + "Complex types (RECORD, ARRAY, MAP, VARIANT, BLOB, VECTOR) are not supported for indexing. "
                + "Please choose a column with a primitive data type.",
            userIndexName, columnName, fieldSchema.getType()));
      }
    }
  }
}
