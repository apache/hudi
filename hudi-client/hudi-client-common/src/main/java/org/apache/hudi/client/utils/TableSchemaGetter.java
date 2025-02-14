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

package org.apache.hudi.client.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.HoodieSchemaNotFoundException;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.containsFieldInSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Helper class to read schema from data files and log files and to convert it between different formats.
 */
@ThreadSafe
public class TableSchemaGetter {

  private static final Logger LOG = LoggerFactory.getLogger(TableSchemaGetter.class);

  protected final HoodieTableMetaClient metaClient;

  /**
   * Signals whether suite of the meta-fields should have additional field designating
   * operation particular record was added by. Note, that determining whether this meta-field
   * should be appended to the schema requires reading out the actual schema of some data file,
   * since it's ultimately the source of truth whether this field has to be represented in
   * the schema
   */
  private final Lazy<Boolean> hasOperationField;

  /**
   * NOTE: {@link HoodieCommitMetadata} could be of non-trivial size for large tables (in 100s of Mbs)
   *       and therefore we'd want to limit amount of throw-away work being performed while fetching
   *       commits' metadata
   *
   *       Please check out corresponding methods to fetch commonly used instances of {@link HoodieCommitMetadata}:
   *       {@link #getCachedCommitMetadata(HoodieInstant)}
   */
  private final Lazy<ConcurrentHashMap<HoodieInstant, HoodieCommitMetadata>> commitMetadataCache;
  private final Lazy<ConcurrentHashMap<HoodieInstant, Schema>> tableSchemaCache;

  @GuardedBy("this")
  private Option<HoodieInstant> latestCommitWithValidSchema = Option.empty();
  @GuardedBy("this")
  private volatile HoodieInstant latestCommitWithValidData = null;

  // If we want to compute based on the latest state of metaClient, purge all cached state so no cached result
  // would be returned.
  public synchronized void purgeAllCachedStates() {
    commitMetadataCache.get().clear();
    tableSchemaCache.get().clear();
    latestCommitWithValidSchema = Option.empty();
    latestCommitWithValidData = null;
  }

  @VisibleForTesting
  public ConcurrentHashMap<HoodieInstant, Schema> getTableSchemaCache() {
    return tableSchemaCache.get();
  }

  public TableSchemaGetter(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    // Unbounded sized map. Should replace with some caching library.
    this.commitMetadataCache = Lazy.lazily(() -> new ConcurrentHashMap<>(2));
    this.tableSchemaCache = Lazy.lazily(ConcurrentHashMap::new);
    this.hasOperationField = Lazy.lazily(this::hasOperationField);
  }

  public Schema getTableAvroSchemaFromDataFile() throws Exception {
    return getTableParquetSchemaFromDataFile().orElseThrow(schemaNotFoundError());
  }

  /**
   * Gets full schema (user + metadata) for a hoodie table in Avro format.
   *
   * @return Avro schema for this table
   * @throws Exception
   */
  public Schema getTableAvroSchema() throws Exception {
    return getTableAvroSchemaIfPresent(metaClient.getTableConfig().populateMetaFields(), Option.empty()).orElseThrow(schemaNotFoundError());
  }

  /**
   * Gets schema for a hoodie table in Avro format, can choose if include metadata fields.
   *
   * @param includeMetadataFields choice if include metadata fields
   * @return Avro schema for this table
   * @throws Exception
   */
  public Schema getTableAvroSchema(boolean includeMetadataFields) throws Exception {
    return getTableAvroSchemaIfPresent(includeMetadataFields, Option.empty()).orElseThrow(schemaNotFoundError());
  }

  /**
   * Fetches tables schema in Avro format as of the given instant
   *
   * @param timestamp as of which table's schema will be fetched
   */
  public Schema getTableAvroSchema(String timestamp) throws Exception {
    Option<HoodieInstant> instant = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .findInstantsBeforeOrEquals(timestamp)
        .lastInstant();
    return getTableAvroSchemaIfPresent(metaClient.getTableConfig().populateMetaFields(), instant).orElseThrow(schemaNotFoundError());
  }

  /**
   * Fetches tables schema in Avro format as of the given instant
   *
   * @param instant as of which table's schema will be fetched
   */
  public Schema getTableAvroSchema(HoodieInstant instant, boolean includeMetadataFields) throws Exception {
    return getTableAvroSchemaIfPresent(includeMetadataFields, Option.of(instant)).orElseThrow(schemaNotFoundError());
  }

  public Option<Schema> getTableAvroSchemaIfPresent(boolean includeMetadataFields) {
    return getTableAvroSchemaIfPresent(includeMetadataFields, Option.empty());
  }

  /**
   * Handles partition column logic for a given schema.
   *
   * @param schema the input schema to process
   * @return the processed schema with partition columns handled appropriately
   */
  private Schema handlePartitionColumnsIfNeeded(Schema schema) {
    if (metaClient.getTableConfig().shouldDropPartitionColumns()) {
      return metaClient.getTableConfig().getPartitionFields()
          .map(partitionFields -> appendPartitionColumns(schema, Option.ofNullable(partitionFields)))
          .or(() -> Option.of(schema))
          .get();
    }
    return schema;
  }

  public Option<Schema> getTableAvroSchemaIfPresent(boolean includeMetadataFields, Option<HoodieInstant> instant) {
    return getTableAvroSchemaFromTimelineWithCache(instant) // Get table schema from schema evolution timeline.
        .or(this::getTableCreateSchemaWithoutMetaField) // Fall back 1: read create schema from table config.
        .or(getTableAvroSchemaFromTimelineWithCache(computeSchemaCommitTimelineInReverseOrder(), instant)) // Fall back 2: read any writer schema available in commit timeline.
        .or(this::getTableParquetSchemaFromDataFile) // Fall back 3: try parsing data file.
        .map(tableSchema -> includeMetadataFields ? HoodieAvroUtils.addMetadataFields(tableSchema, hasOperationField.get()) : HoodieAvroUtils.removeMetadataFields(tableSchema))
        .map(this::handlePartitionColumnsIfNeeded);
  }

  private Option<Schema> getTableCreateSchemaWithoutMetaField() {
    return metaClient.getTableConfig().getTableCreateSchema();
  }

  private synchronized void setCachedLatestCommitWithValidSchema(Option<HoodieInstant> instantOption) {
    latestCommitWithValidSchema = instantOption;
  }

  private synchronized Option<HoodieInstant> getCachedLatestCommitWithValidSchema() {
    return latestCommitWithValidSchema;
  }

  @VisibleForTesting
  Option<Schema> getTableAvroSchemaFromTimelineWithCache(Option<HoodieInstant> instantTime) {
    return getTableAvroSchemaFromTimelineWithCache(computeSchemaEvolutionTimelineInReverseOrder(), instantTime);
  }

  Option<Schema> getTableAvroSchemaFromTimelineWithCache(Stream<HoodieInstant> reversedTimelineStream, Option<HoodieInstant> instantTime) {
    // If instantTime is empty it means read the latest one. In that case, get the cached instant if there is one.
    boolean fetchFromLastValidCommit = instantTime.isEmpty();
    Option<HoodieInstant> targetInstant = instantTime.or(getCachedLatestCommitWithValidSchema());
    Schema cachedTableSchema = null;

    // Try cache first if there is a target instant to fetch for.
    if (!targetInstant.isEmpty()) {
      cachedTableSchema = tableSchemaCache.get().getOrDefault(targetInstant.get(), null);
    }

    // Cache miss on either latestCommitWithValidSchema or commitMetadataCache. Compute the result.
    if (cachedTableSchema == null) {
      Option<Pair<HoodieInstant, Schema>> instantWithSchema = getLastCommitMetadataWithValidSchemaFromTimeline(reversedTimelineStream, targetInstant);
      if (instantWithSchema.isPresent()) {
        targetInstant = Option.of(instantWithSchema.get().getLeft());
        cachedTableSchema = instantWithSchema.get().getRight();
      }
    }

    // After computation, update the cache for the instant and commit metadata.
    if (fetchFromLastValidCommit) {
      setCachedLatestCommitWithValidSchema(targetInstant);
    }
    if (cachedTableSchema != null) {
      // We save cache for 2 cases
      // - input specifies a specific instant, we update the cache for that instant, which is instantTime.
      // - input is empty implying fetch the latest schema, update the cache for the
      //   latest valid commit which is targetInstant
      if (instantTime.isPresent()) {
        tableSchemaCache.get().putIfAbsent(instantTime.get(), cachedTableSchema);
      }
      if (targetInstant.isPresent()) {
        tableSchemaCache.get().putIfAbsent(targetInstant.get(), cachedTableSchema);
      }
    }

    // Finally process the computation results and return.
    if (cachedTableSchema == null) {
      return Option.empty();
    }

    return Option.of(cachedTableSchema);
  }

  @VisibleForTesting
  Option<Pair<HoodieInstant, Schema>> getLastCommitMetadataWithValidSchemaFromTimeline(Stream<HoodieInstant> reversedTimelineStream, Option<HoodieInstant> instant) {
    // To find the table schema given an instant time, need to walk backwards from the latest instant in
    // the timeline finding a completed instant containing a valid schema.
    ConcurrentHashMap<HoodieInstant, Schema> tableSchemaAtInstant = new ConcurrentHashMap<>();
    Option<HoodieInstant> instantWithTableSchema = Option.fromJavaOptional(reversedTimelineStream
        // If a completion time is specified, find the first eligible instant in the schema evolution timeline.
        // Should switch to completion time based.
        .filter(s -> instant.isEmpty() || compareTimestamps(s.requestedTime(), LESSER_THAN_OR_EQUALS, instant.get().requestedTime()))
        // Make sure the commit metadata has a valid schema inside. Same caching the result for expensive operation.
        .filter(s -> {
          try {
            // If we processed the instant before, do not parse the commit metadata again.
            if (tableSchemaCache.get().containsKey(s)) {
              tableSchemaAtInstant.putIfAbsent(s, tableSchemaCache.get().get(s));
              return true;
            }
            HoodieCommitMetadata metadata = metaClient.getCommitMetadataSerDe().deserialize(
                    s,
                    metaClient.getActiveTimeline().getInstantDetails(s).get(),
                    HoodieCommitMetadata.class);
            String schemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
            boolean validSchemaStr = !StringUtils.isNullOrEmpty(schemaStr);
            if (validSchemaStr) {
              tableSchemaAtInstant.putIfAbsent(s, new Schema.Parser().parse(schemaStr));
            }
            return validSchemaStr;
          } catch (IOException e) {
            LOG.warn("Failed to parse commit metadata for instant {} ", s, e);
          }
          return false;
        })
        .findFirst());

    if (instantWithTableSchema.isEmpty()) {
      return Option.empty();
    }
    return Option.of(Pair.of(instantWithTableSchema.get(), tableSchemaAtInstant.get(instantWithTableSchema.get())));
  }

  /**
   * Fetches the schema for a table from any the table's data files
   */
  private Option<Schema> getTableParquetSchemaFromDataFile() {
    Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantAndCommitMetadata = getLatestCommitMetadataWithValidData();
    try {
      switch (metaClient.getTableType()) {
        case COPY_ON_WRITE:
        case MERGE_ON_READ:
          // For COW table, data could be written in either Parquet or Orc format currently;
          // For MOR table, data could be written in either Parquet, Orc, Hfile or Delta-log format currently;
          //
          // Determine the file format based on the file name, and then extract schema from it.
          if (instantAndCommitMetadata.isPresent()) {
            HoodieCommitMetadata commitMetadata = instantAndCommitMetadata.get().getRight();
            Iterator<String> filePaths = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().iterator();
            return Option.of(fetchSchemaFromFiles(filePaths));
          } else {
            LOG.warn("Could not find any data file written for commit, so could not get schema for table {}", metaClient.getBasePath());
            return Option.empty();
          }
        default:
          LOG.error("Unknown table type {}", metaClient.getTableType());
          throw new InvalidTableException(metaClient.getBasePath().toString());
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read data schema", e);
    }
  }

  /**
   * Returns table's latest Avro {@link Schema} iff table is non-empty (ie there's at least
   * a single commit)
   *
   * This method differs from {@link #getTableAvroSchema(boolean)} in that it won't fallback
   * to use table's schema used at creation
   */
  public Option<Schema> getTableAvroSchemaFromLatestCommit(boolean includeMetadataFields) {
    return getTableAvroSchemaFromTimelineWithCache(computeSchemaEvolutionTimelineInReverseOrder(), Option.empty())
        .map(tableSchema -> includeMetadataFields ? HoodieAvroUtils.addMetadataFields(tableSchema, hasOperationField.get()) : HoodieAvroUtils.removeMetadataFields(tableSchema))
        .map(this::handlePartitionColumnsIfNeeded);
  }

  private Schema readSchemaFromLogFile(StoragePath path) throws IOException {
    return readSchemaFromLogFile(metaClient.getRawStorage(), path);
  }

  /**
   * Read the schema from the log file on path.
   *
   * @return
   */
  public static Schema readSchemaFromLogFile(HoodieStorage storage, StoragePath path) throws IOException {
    // We only need to read the schema from the log block header,
    // so we read the block lazily to avoid reading block content
    // containing the records
    try (Reader reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(path), null, false)) {
      HoodieDataBlock lastBlock = null;
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (block instanceof HoodieDataBlock) {
          lastBlock = (HoodieDataBlock) block;
        }
      }
      return lastBlock != null ? lastBlock.getSchema() : null;
    }
  }

  /**
   * Need to check and deprecate this.
   * Gets the InternalSchema for a hoodie table from the HoodieCommitMetadata of the instant.
   *
   * @return InternalSchema for this table
   */
  public Option<InternalSchema> getTableInternalSchemaFromCommitMetadata() {
    HoodieTimeline completedInstants = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTimeline timeline = completedInstants
        .filter(instant -> { // consider only instants that can update/change schema.
          try {
            HoodieCommitMetadata commitMetadata = metaClient.getCommitMetadataSerDe().deserialize(
                instant, completedInstants.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
            return WriteOperationType.canUpdateSchema(commitMetadata.getOperationType());
          } catch (IOException e) {
            throw new HoodieIOException(String.format("Failed to fetch HoodieCommitMetadata for instant (%s)", instant), e);
          }
        });
    return timeline.lastInstant().flatMap(this::getTableInternalSchemaFromCommitMetadata);
  }

  /**
   * Need to check and deprecate this.
   * Gets the InternalSchema for a hoodie table from the HoodieCommitMetadata of the instant.
   *
   * @return InternalSchema for this table
   */
  public Option<InternalSchema> getTableInternalSchemaFromCommitMetadata(String timestamp) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants()
        .findInstantsBeforeOrEquals(timestamp);
    return timeline.lastInstant().flatMap(this::getTableInternalSchemaFromCommitMetadata);
  }

  /**
   * Gets the InternalSchema for a hoodie table from the HoodieCommitMetadata of the instant.
   *
   * @return InternalSchema for this table
   */
  private Option<InternalSchema> getTableInternalSchemaFromCommitMetadata(HoodieInstant instant) {
    try {
      HoodieCommitMetadata metadata = getCachedCommitMetadata(instant);
      String latestInternalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
      if (latestInternalSchemaStr != null) {
        return SerDeHelper.fromJson(latestInternalSchemaStr);
      } else {
        return Option.empty();
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from commit metadata", e);
    }
  }

  /**
   * Need to check and deprecate this.
   * Gets the history schemas as String for a hoodie table from the HoodieCommitMetadata of the instant.
   *
   * @return history schemas string for this table
   */
  public Option<String> getTableHistorySchemaStrFromCommitMetadata() {
    // now we only support FileBaseInternalSchemaManager
    FileBasedInternalSchemaStorageManager manager = new FileBasedInternalSchemaStorageManager(metaClient);
    String result = manager.getHistorySchemaStr();
    return result.isEmpty() ? Option.empty() : Option.of(result);
  }

  /**
   * NOTE: This method could only be used in tests
   *
   * @VisibleForTesting
   */
  public boolean hasOperationField() {
    try {
      Schema tableAvroSchema = getTableAvroSchemaFromDataFile();
      return tableAvroSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD) != null;
    } catch (Exception e) {
      LOG.info("Failed to read operation field from avro schema ({})", e.getMessage());
      return false;
    }
  }

  private Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLatestCommitMetadataWithValidData() {
    if (latestCommitWithValidData == null) {
      Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantAndCommitMetadata =
          metaClient.getActiveTimeline().getLastCommitMetadataWithValidData();
      if (instantAndCommitMetadata.isPresent()) {
        HoodieInstant instant = instantAndCommitMetadata.get().getLeft();
        HoodieCommitMetadata metadata = instantAndCommitMetadata.get().getRight();
        synchronized (this) {
          if (latestCommitWithValidData == null) {
            latestCommitWithValidData = instant;
          }
          commitMetadataCache.get().putIfAbsent(instant, metadata);
        }
      }
    }

    return Option.ofNullable(latestCommitWithValidData)
        .map(instant -> Pair.of(instant, commitMetadataCache.get().get(instant)));
  }

  private HoodieCommitMetadata getCachedCommitMetadata(HoodieInstant instant) {
    return commitMetadataCache.get()
        .computeIfAbsent(instant, (missingInstant) -> {
          HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
          byte[] data = timeline.getInstantDetails(missingInstant).get();
          try {
            return metaClient.getCommitMetadataSerDe().deserialize(missingInstant, data, HoodieCommitMetadata.class);
          } catch (IOException e) {
            throw new HoodieIOException(String.format("Failed to fetch HoodieCommitMetadata for instant (%s)", missingInstant), e);
          }
        });
  }

  private Schema fetchSchemaFromFiles(Iterator<String> filePaths) throws IOException {
    Schema schema = null;
    while (filePaths.hasNext() && schema == null) {
      StoragePath filePath = new StoragePath(filePaths.next());
      if (FSUtils.isLogFile(filePath)) {
        // this is a log file
        schema = readSchemaFromLogFile(filePath);
      } else {
        schema = HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(filePath).readAvroSchema(metaClient.getStorage(), filePath);
      }
    }
    return schema;
  }

  public static Schema appendPartitionColumns(Schema dataSchema, Option<String[]> partitionFields) {
    // In cases when {@link DROP_PARTITION_COLUMNS} config is set true, partition columns
    // won't be persisted w/in the data files, and therefore we need to append such columns
    // when schema is parsed from data files
    //
    // Here we append partition columns with {@code StringType} as the data type
    if (!partitionFields.isPresent() || partitionFields.get().length == 0) {
      return dataSchema;
    }

    boolean hasPartitionColNotInSchema = Arrays.stream(partitionFields.get()).anyMatch(pf -> !containsFieldInSchema(dataSchema, pf));
    boolean hasPartitionColInSchema = Arrays.stream(partitionFields.get()).anyMatch(pf -> containsFieldInSchema(dataSchema, pf));
    if (hasPartitionColNotInSchema && hasPartitionColInSchema) {
      throw new HoodieSchemaException("Partition columns could not be partially contained w/in the data schema");
    }

    if (hasPartitionColNotInSchema) {
      // when hasPartitionColNotInSchema is true and hasPartitionColInSchema is false, all partition columns
      // are not in originSchema. So we create and add them.
      List<Field> newFields = new ArrayList<>();
      for (String partitionField: partitionFields.get()) {
        newFields.add(new Schema.Field(
            partitionField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE));
      }
      return appendFieldsToSchema(dataSchema, newFields);
    }

    return dataSchema;
  }

  private Supplier<Exception> schemaNotFoundError() {
    return () -> new HoodieSchemaNotFoundException("No schema found for table at " + metaClient.getBasePath());
  }

  Stream<HoodieInstant> computeSchemaCommitTimelineInReverseOrder() {
    return metaClient.getCommitsTimeline().filterCompletedInstants()
        .getInstantsAsStream()
        .sorted(Comparator.comparing(HoodieInstant::requestedTime).reversed());
  }

  /**
   * Get timeline in REVERSE order that only contains completed instants which POTENTIALLY evolve the table schema.
   * For types of instants that are included and not reflecting table schema at their instant completion time please refer
   * comments inside the code.
   * */
  public Stream<HoodieInstant> computeSchemaEvolutionTimelineInReverseOrder() {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Stream<HoodieInstant> timelineStream = timeline.getInstantsAsStream();
    final Set<String> actions;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE: {
        actions = new HashSet<>(Arrays.asList(COMMIT_ACTION, REPLACE_COMMIT_ACTION));
        break;
      }
      case MERGE_ON_READ: {
        actions = new HashSet<>(Arrays.asList(DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION));
        break;
      }
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }

    // We only care committed instant when it comes to table schema.
    TimelineLayout timelineLayout = metaClient.getTimelineLayout();
    // Should we use completion time based?
    Comparator<HoodieInstant> reversedComparator = timelineLayout.getInstantComparator().requestedTimeOrderedComparator().reversed();

    // The timeline still contains DELTA_COMMIT_ACTION/COMMIT_ACTION which might not contain a valid schema
    // field in their commit metadata.
    // Since the operations of filtering them out are expensive, we should do on-demand stream based
    // filtering when we actually need the table schema.
    Stream<HoodieInstant> reversedTimelineWithTableSchema = timelineStream
        // Only focuses on those who could potentially evolve the table schema.
        .filter(instant -> actions.contains(instant.getAction()))
        // Further filtering out clustering operations as it does not evolve table schema.
        .filter(instant -> !ClusteringUtils.isClusteringInstant(timeline, instant, metaClient.getInstantGenerator()))
        .filter(HoodieInstant::isCompleted)
        // We reverse the order as the operation against this timeline would be very efficient if
        // we always start from the tail.
        .sorted(reversedComparator);
    return reversedTimelineWithTableSchema;
  }
}
