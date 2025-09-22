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

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.SerializableSchema;
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
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataIndexException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldSchemaFromWriteSchema;
import static org.apache.hudi.common.config.HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
import static org.apache.hudi.common.util.HoodieRecordUtils.getOrderingFieldNames;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.IDENTITY_TRANSFORM;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.table.action.commit.HoodieDeleteHelper.createDeleteRecord;

/**
 * Hoodie Index Utilities.
 */
public class HoodieIndexUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIndexUtils.class);

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
   * Given table schema and fields to index, checks if each field's data types are supported for secondary index.
   * Secondary index has stricter requirements than expression index.
   *
   * @param sourceFields fields to index
   * @param tableSchema  table schema
   * @return true if each field's data type are supported for secondary index, false otherwise
   */
  static boolean validateDataTypeForSecondaryIndex(List<String> sourceFields, Schema tableSchema) {
    return sourceFields.stream().allMatch(fieldToIndex -> {
      Schema schema = getNestedFieldSchemaFromWriteSchema(tableSchema, fieldToIndex);
      return isSecondaryIndexSupportedType(schema);
    });
  }

  /**
   * Given table schema and fields to index, checks if each field's data types are supported.
   *
   * @param sourceFields fields to index
   * @param tableSchema  table schema
   * @return true if each field's data types are supported, false otherwise
   */
  public static boolean validateDataTypeForSecondaryOrExpressionIndex(List<String> sourceFields, Schema tableSchema) {
    return sourceFields.stream().anyMatch(fieldToIndex -> {
      Schema schema = getNestedFieldSchemaFromWriteSchema(tableSchema, fieldToIndex);
      return schema.getType() != Schema.Type.RECORD && schema.getType() != Schema.Type.ARRAY && schema.getType() != Schema.Type.MAP;
    });
  }

  /**
   * Check if the given schema type is supported for secondary index.
   * Supported types are: String (including CHAR), Integer types (Int, BigInt, Long, Short), and timestamp
   */
  private static boolean isSecondaryIndexSupportedType(Schema schema) {
    // Handle union types (nullable fields)
    if (schema.getType() == Schema.Type.UNION) {
      // For union types, check if any of the types is supported
      return schema.getTypes().stream()
          .anyMatch(s -> s.getType() != Schema.Type.NULL && isSecondaryIndexSupportedType(s));
    }

    // Check basic types
    switch (schema.getType()) {
      case STRING:
        // STRING type can have UUID logical type which we don't support
        return schema.getLogicalType() == null; // UUID and other string-based logical types are not supported
      // Regular STRING (includes CHAR)
      case INT:
        // INT type can represent regular integers or dates/times with logical types
        if (schema.getLogicalType() != null) {
          // Support date and time-millis logical types
          return schema.getLogicalType() == LogicalTypes.date()
              || schema.getLogicalType() == LogicalTypes.timeMillis();
        }
        return true; // Regular INT
      case LONG:
        // LONG type can represent regular longs or timestamps with logical types
        if (schema.getLogicalType() != null) {
          // Support timestamp logical types
          return schema.getLogicalType() == LogicalTypes.timestampMillis()
              || schema.getLogicalType() == LogicalTypes.timestampMicros()
              || schema.getLogicalType() == LogicalTypes.timeMicros();
        }
        return true; // Regular LONG
      case DOUBLE:
        return true; // Support DOUBLE type
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
  public static List<Pair<String, Long>> filterKeysFromFile(StoragePath filePath,
                                                            List<String> candidateRecordKeys,
                                                            HoodieStorage storage) throws HoodieIndexException {
    checkArgument(FSUtils.isBaseFile(filePath));
    List<Pair<String, Long>> foundRecordKeys = new ArrayList<>();
    LOG.info(String.format("Going to filter %d keys from file %s", candidateRecordKeys.size(), filePath));
    try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, filePath)) {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = HoodieTimer.start();
        Set<Pair<String, Long>> fileRowKeys = fileReader.filterRowKeys(candidateRecordKeys.stream().collect(Collectors.toSet()));
        foundRecordKeys.addAll(fileRowKeys);
        LOG.info("Checked keys against file {}, in {} ms. #candidates ({}) #found ({})", filePath,
            timer.endTimer(), candidateRecordKeys.size(), foundRecordKeys.size());
        LOG.debug("Keys matching for file {} => {}", filePath, foundRecordKeys);
      }
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
    return foundRecordKeys;
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
   * @return {@link HoodieRecord}s that have the current location being set.
   */
  private static <R> HoodieData<HoodieRecord<R>> getExistingRecords(
      HoodieData<Pair<String, String>> partitionLocations, HoodieWriteConfig config, HoodieTable hoodieTable, ReaderContextFactory<R> readerContextFactory, Schema dataSchema) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    final Option<String> instantTime = metaClient
        .getActiveTimeline() // we need to include all actions and completed
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);
    if (instantTime.isEmpty()) {
      return hoodieTable.getContext().emptyHoodieData();
    }
    return partitionLocations.flatMap(p -> {
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
      HoodieFileGroupReader<R> fileGroupReader = HoodieFileGroupReader.<R>newBuilder()
          .withReaderContext(readerContext)
          .withHoodieTableMetaClient(metaClient)
          .withLatestCommitTime(instantTime.get())
          .withFileSlice(fileSlice)
          .withDataSchema(dataSchema)
          .withRequestedSchema(dataSchema)
          .withInternalSchema(internalSchemaOption)
          .withProps(metaClient.getTableConfig().getProps())
          .withEnableOptimizedLogBlockScan(config.enableOptimizedLogBlocksScan())
          .build();
      try {
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
      Schema writeSchema,
      Schema writeSchemaWithMetaFields,
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

  private static <R> String inferPartitionPath(HoodieRecord<R> incoming, HoodieRecord<R> existing, Schema recordSchema, BaseKeyGenerator keyGenerator,
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
      Schema writeSchema,
      Schema writeSchemaWithMetaFields,
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
      if (config.isFileGroupReaderBasedMergeHandle() && HoodieRecordUtils.isPayloadClassDeprecated(ConfigUtils.getPayloadClass(properties))) {
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
   * Merge tagged incoming records with existing records in case of partition path updated.
   */
  public static <R> HoodieData<HoodieRecord<R>> mergeForPartitionUpdatesAndDeletionsIfNeeded(HoodieData<Pair<HoodieRecord<R>,
                                                                                             Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations,
                                                                                             boolean shouldUpdatePartitionPath,
                                                                                             HoodieWriteConfig config,
                                                                                             HoodieTable hoodieTable,
                                                                                             HoodieReaderContext<R> readerContext,
                                                                                             SerializableSchema writerSchema) {
    boolean isExpressionPayload = config.getPayloadClass().equals("org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    Pair<HoodieWriteConfig, BaseKeyGenerator> keyGeneratorWriteConfigOpt =
        getKeygenAndUpdatedWriteConfig(config, hoodieTable.getMetaClient().getTableConfig(), isExpressionPayload);
    HoodieWriteConfig updatedConfig = keyGeneratorWriteConfigOpt.getLeft();
    BaseKeyGenerator keyGenerator = keyGeneratorWriteConfigOpt.getRight();
    // completely new records
    HoodieData<HoodieRecord<R>> taggedNewRecords = incomingRecordsAndLocations.filter(p -> !p.getRight().isPresent()).map(Pair::getLeft);
    // the records found in existing base files
    HoodieData<HoodieRecord<R>> untaggedUpdatingRecords = incomingRecordsAndLocations.filter(p -> p.getRight().isPresent()).map(Pair::getLeft)
        .distinctWithKey(HoodieRecord::getRecordKey, updatedConfig.getGlobalIndexReconcileParallelism());
    // the tagging partitions and locations
    // NOTE: The incoming records may only differ in record position, however, for the purpose of
    //       merging in case of partition updates, it is safe to ignore the record positions.
    HoodieData<Pair<String, String>> globalLocations = incomingRecordsAndLocations
        .filter(p -> p.getRight().isPresent())
        .map(p -> Pair.of(p.getRight().get().getPartitionPath(), p.getRight().get().getFileId()))
        .distinct(updatedConfig.getGlobalIndexReconcileParallelism());
    // define the buffered record merger.
    TypedProperties properties = readerContext.getMergeProps(updatedConfig.getProps());
    RecordContext<R> incomingRecordContext = readerContext.getRecordContext();
    // Create a reader context for the existing records. In the case of merge-into commands, the incoming records
    // can be using an expression payload so here we rely on the table's configured payload class if it is required.
    ReaderContextFactory<R> readerContextFactoryForExistingRecords = (ReaderContextFactory<R>) hoodieTable.getContext()
        .getReaderContextFactoryForWrite(hoodieTable.getMetaClient(), config.getRecordMerger().getRecordType(), hoodieTable.getMetaClient().getTableConfig().getProps());
    RecordContext<R> existingRecordContext = readerContextFactoryForExistingRecords.getContext().getRecordContext();
    // merged existing records with current locations being set
    SerializableSchema writerSchemaWithMetaFields = new SerializableSchema(HoodieAvroUtils.addMetadataFields(writerSchema.get(), updatedConfig.allowOperationMetadataField()));
    AvroSchemaCache.intern(writerSchema.get());
    AvroSchemaCache.intern(writerSchemaWithMetaFields.get());
    // Read the existing records with the meta fields and current writer schema as the output schema
    HoodieData<HoodieRecord<R>> existingRecords =
        getExistingRecords(globalLocations, keyGeneratorWriteConfigOpt.getLeft(), hoodieTable, readerContextFactoryForExistingRecords, writerSchemaWithMetaFields.get());
    List<String> orderingFieldNames = getOrderingFieldNames(
        readerContext.getMergeMode(), hoodieTable.getMetaClient());
    BufferedRecordMerger<R> recordMerger = BufferedRecordMergerFactory.create(
        readerContext,
        readerContext.getMergeMode(),
        false,
        readerContext.getRecordMerger(),
        writerSchema.get(),
        Option.ofNullable(Pair.of(hoodieTable.getMetaClient().getTableConfig().getPayloadClass(), hoodieTable.getConfig().getPayloadClass())),
        properties,
        hoodieTable.getMetaClient().getTableConfig().getPartialUpdateMode());
    String[] orderingFieldsArray = orderingFieldNames.toArray(new String[0]);
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(properties, writerSchema.get());
    HoodieData<HoodieRecord<R>> taggedUpdatingRecords = untaggedUpdatingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r))
        .leftOuterJoin(existingRecords.mapToPair(r -> Pair.of(r.getRecordKey(), r)))
        .values().flatMap(entry -> {
          HoodieRecord<R> incoming = entry.getLeft();
          Option<HoodieRecord<R>> existingOpt = entry.getRight();
          if (!existingOpt.isPresent()) {
            // existing record not found (e.g., due to delete log not merged to base file): tag as a new record
            return Collections.singletonList(incoming).iterator();
          }
          HoodieRecord<R> existing = existingOpt.get();
          Schema writeSchema = writerSchema.get();

          Option<HoodieRecord<R>> mergedOpt = mergeIncomingWithExistingRecord(
              incoming, existing, writeSchema, writerSchemaWithMetaFields.get(), updatedConfig,
              recordMerger, keyGenerator, incomingRecordContext, existingRecordContext, orderingFieldsArray, properties, isExpressionPayload, deleteContext);
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
    return taggedUpdatingRecords.union(taggedNewRecords);
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
    SerializableSchema writerSchema = new SerializableSchema(config.getWriteSchema());
    boolean isCommitTimeOrdered = readerContext.getMergeMode() == RecordMergeMode.COMMIT_TIME_ORDERING;
    // if the index is not updating the partition of the record, and the table is COW, then we do not need to do merging at
    // this phase since the writer path will merge when rewriting the files as part of the upsert operation.
    boolean requiresMergingWithOlderRecordVersion = shouldUpdatePartitionPath || table.getMetaClient().getTableConfig().getTableType() == HoodieTableType.MERGE_ON_READ;
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(properties, writerSchema.get());

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
    LOG.info("Registering index {} of using {}", indexDefinition.getIndexName(), indexDefinition.getIndexType());
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
    Schema tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    List<String> sourceFields = new ArrayList<>(columns.keySet());
    String columnName = sourceFields.get(0); // We know there's only one column from the check above

    // First check if the field exists
    try {
      getNestedFieldSchemaFromWriteSchema(tableSchema, columnName);
    } catch (Exception e) {
      throw new HoodieMetadataIndexException(String.format(
          "Cannot create %s index '%s': Column '%s' does not exist in the table schema. "
          + "Please verify the column name and ensure it exists in the table.",
          indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? "secondary" : "expression",
          userIndexName, columnName));
    }

    // Check for complex types (RECORD, ARRAY, MAP) - not supported for any index type
    if (!validateDataTypeForSecondaryOrExpressionIndex(sourceFields, tableSchema)) {
      Schema fieldSchema = getNestedFieldSchemaFromWriteSchema(tableSchema, columnName);
      throw new HoodieMetadataIndexException(String.format(
          "Cannot create %s index '%s': Column '%s' has unsupported data type '%s'. "
          + "Complex types (RECORD, ARRAY, MAP) are not supported for indexing. "
          + "Please choose a column with a primitive data type.",
          indexType.equals(PARTITION_NAME_SECONDARY_INDEX) ? "secondary" : "expression",
          userIndexName, columnName, fieldSchema.getType()));
    }

    // For secondary index, apply stricter data type validation
    if (indexType.equals(PARTITION_NAME_SECONDARY_INDEX)) {
      if (!validateDataTypeForSecondaryIndex(sourceFields, tableSchema)) {
        Schema fieldSchema = getNestedFieldSchemaFromWriteSchema(tableSchema, columnName);
        String actualType = fieldSchema.getType().toString();
        if (fieldSchema.getLogicalType() != null) {
          actualType += " with logical type " + fieldSchema.getLogicalType();
        }

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
          options.getOrDefault(RECORD_INDEX_ENABLE_PROP.key(), RECORD_INDEX_ENABLE_PROP.defaultValue().toString()));

      if (!hasRecordIndex && !recordIndexEnabled) {
        throw new HoodieMetadataIndexException(String.format(
            "Cannot create secondary index '%s': Record index is required for secondary indexes but is not enabled. "
            + "Please enable the record index by setting '%s' to 'true' in the index creation options, "
            + "or create a record index first using: CREATE INDEX record_index ON %s USING record_index",
            userIndexName, RECORD_INDEX_ENABLE_PROP.key(), metaClient.getTableConfig().getTableName()));
      }
    }
  }
}
