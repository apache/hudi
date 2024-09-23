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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.io.HoodieMergedReadHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.util.ConfigUtils.DEFAULT_HUDI_CONFIG_FOR_READER;
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
          .getLatestBaseFilesBeforeOrOn(partition, latestCommitTime.get().getTimestamp())
          .collect(toList());
    }
    return Collections.emptyList();
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
              .getLatestFileSlicesBeforeOrOn(partition, latestCommitTime.get().getTimestamp(), true)
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
   * @param storage             - {@link HoodieStorage} instance
   * @return List of candidate keys that are available in the file
   */
  public static List<String> filterKeysFromFile(StoragePath filePath, List<String> candidateRecordKeys,
                                                HoodieStorage storage) throws HoodieIndexException {
    ValidationUtils.checkArgument(FSUtils.isBaseFile(filePath));
    List<String> foundRecordKeys = new ArrayList<>();
    try (HoodieFileReader fileReader = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecordType.AVRO)
        .getFileReader(DEFAULT_HUDI_CONFIG_FOR_READER, filePath)) {
      // Load all rowKeys from the file, to double-confirm
      if (!candidateRecordKeys.isEmpty()) {
        HoodieTimer timer = HoodieTimer.start();
        Set<String> fileRowKeys = fileReader.filterRowKeys(new TreeSet<>(candidateRecordKeys));
        foundRecordKeys.addAll(fileRowKeys);
        LOG.info(String.format("Checked keys against file %s, in %d ms. #candidates (%d) #found (%d)", filePath,
            timer.endTimer(), candidateRecordKeys.size(), foundRecordKeys.size()));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Keys matching for file " + filePath + " => " + foundRecordKeys);
        }
      }
    } catch (Exception e) {
      throw new HoodieIndexException("Error checking candidate keys against file.", e);
    }
    return foundRecordKeys;
  }

  /**
   * Check if the given commit timestamp is valid for the timeline.
   *
   * The commit timestamp is considered to be valid if:
   *   1. the commit timestamp is present in the timeline, or
   *   2. the commit timestamp is less than the first commit timestamp in the timeline
   *
   * @param commitTimeline  The timeline
   * @param commitTs        The commit timestamp to check
   * @return                true if the commit timestamp is valid for the timeline
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
      HoodieData<Pair<String, String>> partitionLocations, HoodieWriteConfig config, HoodieTable hoodieTable) {
    final Option<String> instantTime = hoodieTable
        .getMetaClient()
        .getActiveTimeline() // we need to include all actions and completed
        .filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::getTimestamp);
    return partitionLocations.flatMap(p
        -> new HoodieMergedReadHandle(config, instantTime, hoodieTable, Pair.of(p.getKey(), p.getValue()))
        .getMergedRecords().iterator());
  }

  /**
   * getExistingRecords will create records with expression payload so we overwrite the config.
   * Additionally, we don't want to restore this value because the write will fail later on.
   * We also need the keygenerator so we can figure out the partition path after expression payload
   * evaluates the merge.
   */
  private static Option<Pair<BaseKeyGenerator, HoodieWriteConfig>> maybeGetKeygenAndUpdatedWriteConfig(HoodieWriteConfig config, HoodieTableConfig tableConfig) {
    if (config.getPayloadClass().equals("org.apache.spark.sql.hudi.command.payload.ExpressionPayload")) {
      TypedProperties typedProperties = new TypedProperties(config.getProps());
      // set the payload class to table's payload class and not expresison payload. this will be used to read the existing records
      typedProperties.setProperty(HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key(), tableConfig.getPayloadClass());
      typedProperties.setProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), tableConfig.getPayloadClass());
      HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withProperties(typedProperties).build();
      try {
        return Option.of(Pair.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(writeConfig.getProps()), writeConfig));
      } catch (IOException e) {
        throw new RuntimeException("KeyGenerator must inherit from BaseKeyGenerator to update a records partition path using spark sql merge into", e);
      }
    }
    return Option.empty();
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
      Schema existingSchema,
      Schema writeSchemaWithMetaFields,
      HoodieWriteConfig config,
      HoodieRecordMerger recordMerger,
      BaseKeyGenerator keyGenerator) throws IOException {
    Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger.merge(existing, existingSchema,
        incoming, writeSchemaWithMetaFields, config.getProps());
    if (!mergeResult.isPresent()) {
      return Option.empty();
    }
    HoodieRecord<R> result = mergeResult.get().getLeft();
    if (result.getData().equals(HoodieRecord.SENTINEL)) {
      return Option.of(result);
    }
    String partitionPath = keyGenerator.getPartitionPath((GenericRecord) result.getData());
    HoodieRecord<R> withMeta = result.prependMetaFields(writeSchema, writeSchemaWithMetaFields,
            new MetadataValues().setRecordKey(incoming.getRecordKey()).setPartitionPath(partitionPath), config.getProps());
    return Option.of(withMeta.wrapIntoHoodieRecordPayloadWithParams(writeSchemaWithMetaFields, config.getProps(), Option.empty(),
        config.allowOperationMetadataField(), Option.empty(), false, Option.of(writeSchema)));

  }


  /**
   * Merge the incoming record with the matching existing record loaded via {@link HoodieMergedReadHandle}. The existing record is the latest version in the table.
   */
  private static <R> Option<HoodieRecord<R>> mergeIncomingWithExistingRecord(
      HoodieRecord<R> incoming,
      HoodieRecord<R> existing,
      Schema writeSchema,
      HoodieWriteConfig config,
      HoodieRecordMerger recordMerger,
      Option<Pair<BaseKeyGenerator, HoodieWriteConfig>> keyGeneratorWriteConfigOpt) throws IOException {
    Schema existingSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()), config.allowOperationMetadataField());
    Schema writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
    if (keyGeneratorWriteConfigOpt.isPresent()) {
      return mergeIncomingWithExistingRecordWithExpressionPayload(incoming, existing, writeSchema,
          existingSchema, writeSchemaWithMetaFields, keyGeneratorWriteConfigOpt.get().getRight(), recordMerger, keyGeneratorWriteConfigOpt.get().getKey());
    } else {
      // prepend the hoodie meta fields as the incoming record does not have them
      HoodieRecord incomingPrepended = incoming
          .prependMetaFields(writeSchema, writeSchemaWithMetaFields, new MetadataValues().setRecordKey(incoming.getRecordKey()).setPartitionPath(incoming.getPartitionPath()), config.getProps());
      // after prepend the meta fields, convert the record back to the original payload
      HoodieRecord incomingWithMetaFields = incomingPrepended
          .wrapIntoHoodieRecordPayloadWithParams(writeSchema, config.getProps(), Option.empty(), config.allowOperationMetadataField(), Option.empty(), false, Option.empty());
      Option<Pair<HoodieRecord, Schema>> mergeResult = recordMerger
          .merge(existing, existingSchema, incomingWithMetaFields, writeSchemaWithMetaFields, config.getProps());
      if (mergeResult.isPresent()) {
        // the merged record needs to be converted back to the original payload
        HoodieRecord<R> merged = mergeResult.get().getLeft().wrapIntoHoodieRecordPayloadWithParams(
            writeSchemaWithMetaFields, config.getProps(), Option.empty(),
            config.allowOperationMetadataField(), Option.empty(), false, Option.of(writeSchema));
        return Option.of(merged);
      } else {
        return Option.empty();
      }
    }
  }

  /**
   * Merge tagged incoming records with existing records in case of partition path updated.
   */
  public static <R> HoodieData<HoodieRecord<R>> mergeForPartitionUpdatesIfNeeded(
      HoodieData<Pair<HoodieRecord<R>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations, HoodieWriteConfig config, HoodieTable hoodieTable) {
    Option<Pair<BaseKeyGenerator, HoodieWriteConfig>> keyGeneratorWriteConfigOpt = maybeGetKeygenAndUpdatedWriteConfig(config, hoodieTable.getMetaClient().getTableConfig());
    // completely new records
    HoodieData<HoodieRecord<R>> taggedNewRecords = incomingRecordsAndLocations.filter(p -> !p.getRight().isPresent()).map(Pair::getLeft);
    // the records found in existing base files
    HoodieData<HoodieRecord<R>> untaggedUpdatingRecords = incomingRecordsAndLocations.filter(p -> p.getRight().isPresent()).map(Pair::getLeft)
        .distinctWithKey(HoodieRecord::getRecordKey, config.getGlobalIndexReconcileParallelism());
    // the tagging partitions and locations
    HoodieData<Pair<String, String>> globalLocations = incomingRecordsAndLocations
        .filter(p -> p.getRight().isPresent())
        .map(p -> Pair.of(p.getRight().get().getPartitionPath(), p.getRight().get().getFileId()))
        .distinct(config.getGlobalIndexReconcileParallelism());
    // merged existing records with current locations being set
    HoodieData<HoodieRecord<R>> existingRecords = getExistingRecords(globalLocations,
        keyGeneratorWriteConfigOpt.isPresent() ? keyGeneratorWriteConfigOpt.get().getRight() : config, hoodieTable);

    final HoodieRecordMerger recordMerger = config.getRecordMerger();
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
          Schema writeSchema = new Schema.Parser().parse(config.getWriteSchema());
          if (incoming.isDelete(writeSchema, config.getProps())) {
            // incoming is a delete: force tag the incoming to the old partition
            return Collections.singletonList(tagRecord(incoming.newInstance(existing.getKey()), existing.getCurrentLocation())).iterator();
          }

          Option<HoodieRecord<R>> mergedOpt = mergeIncomingWithExistingRecord(incoming, existing, writeSchema, config, recordMerger, keyGeneratorWriteConfigOpt);
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
          } else {
            // merged record has a different partition: issue a delete to the old partition and insert the merged record to the new partition
            HoodieRecord<R> deleteRecord = createDeleteRecord(config, existing.getKey());
            deleteRecord.setIgnoreIndexUpdate(true);
            return Arrays.asList(tagRecord(deleteRecord, existing.getCurrentLocation()), merged).iterator();
          }
        });
    return taggedUpdatingRecords.union(taggedNewRecords);
  }

  public static <R> HoodieData<HoodieRecord<R>> tagGlobalLocationBackToRecords(
      HoodieData<HoodieRecord<R>> incomingRecords,
      HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations,
      boolean mayContainDuplicateLookup,
      boolean shouldUpdatePartitionPath,
      HoodieWriteConfig config,
      HoodieTable table) {
    final HoodieRecordMerger merger = config.getRecordMerger();

    HoodiePairData<String, HoodieRecord<R>> keyAndIncomingRecords =
        incomingRecords.mapToPair(record -> Pair.of(record.getRecordKey(), record));

    // Pair of incoming record and the global location if meant for merged lookup in later stage
    HoodieData<Pair<HoodieRecord<R>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations
        = keyAndIncomingRecords.leftOuterJoin(keyAndExistingLocations).values()
        .map(v -> {
          final HoodieRecord<R> incomingRecord = v.getLeft();
          Option<HoodieRecordGlobalLocation> currentLocOpt = Option.ofNullable(v.getRight().orElse(null));
          if (currentLocOpt.isPresent()) {
            HoodieRecordGlobalLocation currentLoc = currentLocOpt.get();
            boolean shouldDoMergedLookUpThenTag = mayContainDuplicateLookup
                || !Objects.equals(incomingRecord.getPartitionPath(), currentLoc.getPartitionPath());
            if (shouldUpdatePartitionPath && shouldDoMergedLookUpThenTag) {
              // the pair's right side is a non-empty Option, which indicates that a merged lookup will be performed
              // at a later stage.
              return Pair.of(incomingRecord, currentLocOpt);
            } else {
              // - When update partition path is set to false,
              //   the incoming record will be tagged to the existing record's partition regardless of being equal or not.
              // - When update partition path is set to true,
              //   the incoming record will be tagged to the existing record's partition
              //   when partition is not updated and the look-up won't have duplicates (e.g. COW, or using RLI).
              return Pair.of(createNewTaggedHoodieRecord(incomingRecord, currentLoc, merger.getRecordType()), Option.empty());
            }
          } else {
            return Pair.of(incomingRecord, Option.empty());
          }
        });
    return shouldUpdatePartitionPath
        ? mergeForPartitionUpdatesIfNeeded(incomingRecordsAndLocations, config, table)
        : incomingRecordsAndLocations.map(Pair::getLeft);
  }

  public static <R> HoodieRecord<R> createNewTaggedHoodieRecord(HoodieRecord<R> oldRecord, HoodieRecordGlobalLocation location, HoodieRecordType recordType) {
    switch (recordType) {
      case AVRO:
        HoodieKey recordKey = new HoodieKey(oldRecord.getRecordKey(), location.getPartitionPath());
        return tagRecord(new HoodieAvroRecord(recordKey, (HoodieRecordPayload) oldRecord.getData()), location);
      case SPARK:
        return tagRecord(oldRecord.newInstance(), location);
      default:
        throw new HoodieIndexException("Unsupported record type: " + recordType);
    }
  }
}
