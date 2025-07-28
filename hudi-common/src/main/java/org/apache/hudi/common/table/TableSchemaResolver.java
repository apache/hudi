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

package org.apache.hudi.common.table;

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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
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

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.containsFieldInSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;

/**
 * Helper class to read schema from data files and log files and to convert it between different formats.
 */
@ThreadSafe
public class TableSchemaResolver {

  private static final Logger LOG = LoggerFactory.getLogger(TableSchemaResolver.class);

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
   *       {@link #getLatestCommitMetadataWithValidSchema()},
   *       {@link #getLatestCommitMetadataWithValidSchema()},
   *       {@link #getCachedCommitMetadata(HoodieInstant)}
   */
  private final Lazy<ConcurrentHashMap<HoodieInstant, HoodieCommitMetadata>> commitMetadataCache;

  private volatile HoodieInstant latestCommitWithValidSchema = null;
  private volatile HoodieInstant latestCommitWithInsertOrUpdate = null;

  public TableSchemaResolver(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    this.commitMetadataCache = Lazy.lazily(() -> new ConcurrentHashMap<>(2));
    this.hasOperationField = Lazy.lazily(this::hasOperationField);
  }

  public Schema getTableAvroSchemaFromDataFile() throws Exception {
    return getTableAvroSchemaFromDataFileInternal().orElseThrow(schemaNotFoundError());
  }

  private Option<Schema> getTableAvroSchemaFromDataFileInternal() {
    return getTableParquetSchemaFromDataFile();
  }

  /**
   * Gets full schema (user + metadata) for a hoodie table in Avro format.
   *
   * @return Avro schema for this table
   * @throws Exception
   */
  public Schema getTableAvroSchema() throws Exception {
    return getTableAvroSchema(metaClient.getTableConfig().populateMetaFields());
  }

  /**
   * Gets schema for a hoodie table in Avro format, can choice if include metadata fields.
   *
   * @param includeMetadataFields choice if include metadata fields
   * @return Avro schema for this table
   * @throws Exception
   */
  public Schema getTableAvroSchema(boolean includeMetadataFields) throws Exception {
    return getTableAvroSchemaInternal(includeMetadataFields, Option.empty()).orElseThrow(schemaNotFoundError());
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
    return getTableAvroSchemaInternal(metaClient.getTableConfig().populateMetaFields(), instant)
        .orElseThrow(schemaNotFoundError());
  }

  /**
   * Fetches tables schema in Avro format as of the given instant
   *
   * @param instant as of which table's schema will be fetched
   */
  public Schema getTableAvroSchema(HoodieInstant instant, boolean includeMetadataFields) throws Exception {
    return getTableAvroSchemaInternal(includeMetadataFields, Option.of(instant)).orElseThrow(schemaNotFoundError());
  }

  /**
   * Gets users data schema for a hoodie table in Avro format.
   *
   * @return  Avro user data schema
   * @throws Exception
   *
   * @deprecated use {@link #getTableAvroSchema(boolean)} instead
   */
  @Deprecated
  public Schema getTableAvroSchemaWithoutMetadataFields() throws Exception {
    return getTableAvroSchemaInternal(false, Option.empty()).orElseThrow(schemaNotFoundError());
  }

  public Option<Schema> getTableAvroSchemaIfPresent(boolean includeMetadataFields) {
    return getTableAvroSchemaInternal(includeMetadataFields, Option.empty());
  }

  private Option<Schema> getTableAvroSchemaInternal(boolean includeMetadataFields, Option<HoodieInstant> instantOpt) {
    Option<Schema> schema =
        (instantOpt.isPresent()
            ? getTableSchemaFromCommitMetadata(instantOpt.get(), includeMetadataFields)
            : getTableSchemaFromLatestCommitMetadata(includeMetadataFields))
            .or(() ->
                metaClient.getTableConfig().getTableCreateSchema()
                    .map(tableSchema ->
                        includeMetadataFields
                            ? HoodieAvroUtils.addMetadataFields(tableSchema, hasOperationField.get())
                            : tableSchema)
            )
            .or(() -> {
              Option<Schema> schemaFromDataFile = getTableAvroSchemaFromDataFileInternal();
              return includeMetadataFields
                  ? schemaFromDataFile
                  : schemaFromDataFile.map(HoodieAvroUtils::removeMetadataFields);
            });

    // TODO partition columns have to be appended in all read-paths
    if (metaClient.getTableConfig().shouldDropPartitionColumns() && schema.isPresent()) {
      return metaClient.getTableConfig().getPartitionFields()
          .map(partitionFields -> appendPartitionColumns(schema.get(), Option.ofNullable(partitionFields)))
          .or(() -> schema);
    }

    return schema;
  }

  private Option<Schema> getTableSchemaFromLatestCommitMetadata(boolean includeMetadataFields) {
    Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantAndCommitMetadata = getLatestCommitMetadataWithValidSchema();
    if (instantAndCommitMetadata.isPresent()) {
      HoodieCommitMetadata commitMetadata = instantAndCommitMetadata.get().getRight();
      String schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
      Schema schema = new Schema.Parser().parse(schemaStr);
      if (includeMetadataFields) {
        schema = HoodieAvroUtils.addMetadataFields(schema, hasOperationField.get());
      } else {
        schema = HoodieAvroUtils.removeMetadataFields(schema);
      }
      return Option.of(schema);
    } else {
      return Option.empty();
    }
  }

  private Option<Schema> getTableSchemaFromCommitMetadata(HoodieInstant instant, boolean includeMetadataFields) {
    try {
      HoodieCommitMetadata metadata = getCachedCommitMetadata(instant);
      String existingSchemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);

      if (StringUtils.isNullOrEmpty(existingSchemaStr)) {
        return Option.empty();
      }

      Schema schema = new Schema.Parser().parse(existingSchemaStr);
      if (includeMetadataFields) {
        schema = HoodieAvroUtils.addMetadataFields(schema, hasOperationField.get());
      } else {
        schema = HoodieAvroUtils.removeMetadataFields(schema);
      }
      return Option.of(schema);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from commit metadata", e);
    }
  }

  /**
   * Fetches the schema for a table from any the table's data files
   */
  private Option<Schema> getTableParquetSchemaFromDataFile() {
    Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantAndCommitMetadata = getLatestCommitMetadataWithInsertOrUpdate();
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
      case MERGE_ON_READ:
        // For COW table, data could be written in either Parquet or Orc format currently;
        // For MOR table, data could be written in either Parquet, Orc, Hfile or Delta-log format currently;
        //
        // Determine the file format based on the file name, and then extract schema from it.
        if (instantAndCommitMetadata.isPresent()) {
          HoodieCommitMetadata commitMetadata = instantAndCommitMetadata.get().getRight();
          // inspect non-empty files for schema
          Stream<StoragePath> filePaths = commitMetadata.getPartitionToWriteStats().values().stream().flatMap(Collection::stream)
              .filter(writeStat -> writeStat.getNumInserts() > 0 || writeStat.getNumUpdateWrites() > 0)
              .map(writeStat -> new StoragePath(metaClient.getBasePath(), writeStat.getPath()));
          return Option.of(fetchSchemaFromFiles(filePaths));
        } else {
          LOG.warn("Could not find any data file written for commit, so could not get schema for table {}", metaClient.getBasePath());
          return Option.empty();
        }
      default:
        LOG.error("Unknown table type {}", metaClient.getTableType());
        throw new InvalidTableException(metaClient.getBasePath().toString());
    }
  }

  /**
   * Returns table's latest Avro {@link Schema} iff table is non-empty (ie there's at least
   * a single commit)
   *
   * This method differs from {@link #getTableAvroSchema(boolean)} in that it won't fallback
   * to use table's schema used at creation
   */
  public Option<Schema> getTableAvroSchemaFromLatestCommit(boolean includeMetadataFields) throws Exception {
    if (metaClient.isTimelineNonEmpty()) {
      return getTableAvroSchemaInternal(includeMetadataFields, Option.empty());
    }

    return Option.empty();
  }

  /**
   * Read schema from a data file from the last compaction commit done.
   *
   * @deprecated please use {@link #getTableAvroSchema(HoodieInstant, boolean)} instead
   */
  public Schema readSchemaFromLastCompaction(Option<HoodieInstant> lastCompactionCommitOpt) throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant lastCompactionCommit = lastCompactionCommitOpt.orElseThrow(() -> new Exception(
        "Could not read schema from last compaction, no compaction commits found on path " + metaClient));

    // Read from the compacted file wrote
    HoodieCommitMetadata compactionMetadata =
        activeTimeline.readCommitMetadata(lastCompactionCommit);
    String filePath = compactionMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
        .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for compaction "
            + lastCompactionCommit + ", could not get schema for table " + metaClient.getBasePath()));
    StoragePath path = new StoragePath(filePath);
    return HoodieIOFactory.getIOFactory(metaClient.getStorage())
        .getFileFormatUtils(path).readAvroSchema(metaClient.getStorage(), path);
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
   * Gets the InternalSchema for a hoodie table from the HoodieCommitMetadata of the instant.
   *
   * @return InternalSchema for this table
   */
  public Option<InternalSchema> getTableInternalSchemaFromCommitMetadata() {
    HoodieTimeline completedInstants = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieTimeline timeline = completedInstants
        .filter(instant -> { // consider only instants that can update/change schema.
          return WriteOperationType.canUpdateSchema(getCachedCommitMetadata(instant).getOperationType());
        });
    return timeline.lastInstant().flatMap(this::getTableInternalSchemaFromCommitMetadata);
  }

  /**
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

  private Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLatestCommitMetadataWithValidSchema() {
    if (latestCommitWithValidSchema == null) {
      Option<Pair<HoodieInstant, HoodieCommitMetadata>> instantAndCommitMetadata =
          metaClient.getActiveTimeline().getLastCommitMetadataWithValidSchema();
      if (instantAndCommitMetadata.isPresent()) {
        HoodieInstant instant = instantAndCommitMetadata.get().getLeft();
        HoodieCommitMetadata metadata = instantAndCommitMetadata.get().getRight();
        synchronized (this) {
          if (latestCommitWithValidSchema == null) {
            latestCommitWithValidSchema = instant;
          }
          commitMetadataCache.get().putIfAbsent(instant, metadata);
        }
      }
    }

    return Option.ofNullable(latestCommitWithValidSchema)
        .map(instant -> Pair.of(instant, commitMetadataCache.get().get(instant)));
  }

  private Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLatestCommitMetadataWithInsertOrUpdate() {
    if (latestCommitWithValidSchema != null && commitMetadataCache.get().containsKey(latestCommitWithValidSchema)) {
      HoodieCommitMetadata commitMetadata = commitMetadataCache.get().get(latestCommitWithValidSchema);
      if (commitHasInsertOrUpdate(commitMetadata)) {
        latestCommitWithInsertOrUpdate = latestCommitWithValidSchema;
      }
    }
    if (latestCommitWithInsertOrUpdate == null) {
      getLatestCommitWithInsertOrUpdate().ifPresent(instantAndCommitMetadata -> {
        HoodieInstant instant = instantAndCommitMetadata.getLeft();
        HoodieCommitMetadata metadata = instantAndCommitMetadata.getRight();
        synchronized (this) {
          if (latestCommitWithInsertOrUpdate == null) {
            latestCommitWithInsertOrUpdate = instant;
          }
          commitMetadataCache.get().putIfAbsent(instant, metadata);
        }
      });
    }

    return Option.ofNullable(latestCommitWithInsertOrUpdate)
        .map(instant -> Pair.of(instant, commitMetadataCache.get().get(instant)));
  }

  private Option<Pair<HoodieInstant, HoodieCommitMetadata>> getLatestCommitWithInsertOrUpdate() {
    HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    return Option.fromJavaOptional(commitsTimeline.getReverseOrderedInstants()
        .map(instant -> {
          try {
            HoodieCommitMetadata commitMetadata = commitsTimeline.readCommitMetadata(instant);
            return Pair.of(instant, commitMetadata);
          } catch (IOException e) {
            throw new HoodieIOException(String.format("Failed to fetch HoodieCommitMetadata for instant (%s)", instant), e);
          }
        })
        .filter(pair -> commitHasInsertOrUpdate(pair.getRight()))
        .findFirst());
  }

  private boolean commitHasInsertOrUpdate(HoodieCommitMetadata commitMetadata) {
    return commitMetadata.getPartitionToWriteStats().values().stream().flatMap(Collection::stream)
        .anyMatch(writeStat -> writeStat.getNumInserts() > 0 || writeStat.getNumUpdateWrites() > 0);
  }

  private HoodieCommitMetadata getCachedCommitMetadata(HoodieInstant instant) {
    return commitMetadataCache.get()
        .computeIfAbsent(instant, (missingInstant) -> {
          HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
          try {
            return timeline.readCommitMetadata(missingInstant);
          } catch (IOException e) {
            throw new HoodieIOException(String.format("Failed to fetch HoodieCommitMetadata for instant (%s)", missingInstant), e);
          }
        });
  }

  private Schema fetchSchemaFromFiles(Stream<StoragePath> filePaths) {
    return filePaths.map(filePath -> {
      try {
        if (FSUtils.isLogFile(filePath)) {
          // this is a log file
          return readSchemaFromLogFile(filePath);
        } else {
          return HoodieIOFactory.getIOFactory(metaClient.getStorage())
              .getFileFormatUtils(filePath).readAvroSchema(metaClient.getStorage(), filePath);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read schema from file: " + filePath, e);
      }
    }).filter(Objects::nonNull).findFirst().orElse(null);
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
}
