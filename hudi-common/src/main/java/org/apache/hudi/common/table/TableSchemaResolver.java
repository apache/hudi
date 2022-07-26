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

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Functions.Function1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIncompatibleSchemaException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieHFileReader;
import org.apache.hudi.io.storage.HoodieOrcReader;
import org.apache.hudi.util.Lazy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.containsFieldInSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;

/**
 * Helper class to read schema from data files and log files and to convert it between different formats.
 */
@ThreadSafe
public class TableSchemaResolver {

  private static final Logger LOG = LogManager.getLogger(TableSchemaResolver.class);

  private final HoodieTableMetaClient metaClient;

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
  private volatile HoodieInstant latestCommitWithValidData = null;

  public TableSchemaResolver(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    this.commitMetadataCache = Lazy.lazily(() -> new ConcurrentHashMap<>(2));
    this.hasOperationField = Lazy.lazily(this::hasOperationField);
  }

  public Schema getTableAvroSchemaFromDataFile() {
    return convertParquetSchemaToAvro(getTableParquetSchemaFromDataFile());
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
    return getTableAvroSchemaInternal(includeMetadataFields, Option.empty());
  }

  /**
   * Fetches tables schema in Avro format as of the given instant
   *
   * @param instant as of which table's schema will be fetched
   */
  public Schema getTableAvroSchema(HoodieInstant instant, boolean includeMetadataFields) throws Exception {
    return getTableAvroSchemaInternal(includeMetadataFields, Option.of(instant));
  }

  /**
   * Gets full schema (user + metadata) for a hoodie table in Parquet format.
   *
   * @return Parquet schema for the table
   */
  public MessageType getTableParquetSchema() throws Exception {
    return convertAvroSchemaToParquet(getTableAvroSchema(true));
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
    return getTableAvroSchema(false);
  }

  private Schema getTableAvroSchemaInternal(boolean includeMetadataFields, Option<HoodieInstant> instantOpt) {
    Schema schema =
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
            .orElseGet(() -> {
              Schema schemaFromDataFile = getTableAvroSchemaFromDataFile();
              return includeMetadataFields
                  ? schemaFromDataFile
                  : HoodieAvroUtils.removeMetadataFields(schemaFromDataFile);
            });

    // TODO partition columns have to be appended in all read-paths
    if (metaClient.getTableConfig().shouldDropPartitionColumns()) {
      return metaClient.getTableConfig().getPartitionFields()
          .map(partitionFields -> appendPartitionColumns(schema, partitionFields))
          .orElse(schema);
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
      }
      return Option.of(schema);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from commit metadata", e);
    }
  }

  /**
   * Fetches the schema for a table from any the table's data files
   */
  private MessageType getTableParquetSchemaFromDataFile() {
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
            Iterator<String> filePaths = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePathV2()).values().iterator();
            return fetchSchemaFromFiles(filePaths);
          } else {
            throw new IllegalArgumentException("Could not find any data file written for commit, "
                + "so could not get schema for table " + metaClient.getBasePath());
          }
        default:
          LOG.error("Unknown table type " + metaClient.getTableType());
          throw new InvalidTableException(metaClient.getBasePath());
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read data schema", e);
    }
  }

  public static MessageType convertAvroSchemaToParquet(Schema schema, Configuration hadoopConf) {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(hadoopConf);
    return avroSchemaConverter.convert(schema);
  }

  private Schema convertParquetSchemaToAvro(MessageType parquetSchema) {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(metaClient.getHadoopConf());
    return avroSchemaConverter.convert(parquetSchema);
  }

  private MessageType convertAvroSchemaToParquet(Schema schema) {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(metaClient.getHadoopConf());
    return avroSchemaConverter.convert(schema);
  }

  /**
   * HUDI specific validation of schema evolution. Ensures that a newer schema can be used for the dataset by
   * checking if the data written using the old schema can be read using the new schema.
   *
   * HUDI requires a Schema to be specified in HoodieWriteConfig and is used by the HoodieWriteClient to
   * create the records. The schema is also saved in the data files (parquet format) and log files (avro format).
   * Since a schema is required each time new data is ingested into a HUDI dataset, schema can be evolved over time.
   *
   * New Schema is compatible only if:
   * A1. There is no change in schema
   * A2. A field has been added and it has a default value specified
   *
   * New Schema is incompatible if:
   * B1. A field has been deleted
   * B2. A field has been renamed (treated as delete + add)
   * B3. A field's type has changed to be incompatible with the older type
   *
   * Issue with org.apache.avro.SchemaCompatibility:
   *  org.apache.avro.SchemaCompatibility checks schema compatibility between a writer schema (which originally wrote
   *  the AVRO record) and a readerSchema (with which we are reading the record). It ONLY guarantees that that each
   *  field in the reader record can be populated from the writer record. Hence, if the reader schema is missing a
   *  field, it is still compatible with the writer schema.
   *
   *  In other words, org.apache.avro.SchemaCompatibility was written to guarantee that we can read the data written
   *  earlier. It does not guarantee schema evolution for HUDI (B1 above).
   *
   * Implementation: This function implements specific HUDI specific checks (listed below) and defers the remaining
   * checks to the org.apache.avro.SchemaCompatibility code.
   *
   * Checks:
   * C1. If there is no change in schema: success
   * C2. If a field has been deleted in new schema: failure
   * C3. If a field has been added in new schema: it should have default value specified
   * C4. If a field has been renamed(treated as delete + add): failure
   * C5. If a field type has changed: failure
   *
   * @param oldSchema Older schema to check.
   * @param newSchema Newer schema to check.
   * @return True if the schema validation is successful
   */
  public static boolean isSchemaCompatible(Schema oldSchema, Schema newSchema) {
    if (oldSchema.getType() == newSchema.getType() && newSchema.getType() == Schema.Type.RECORD) {
      // record names must match:
      if (!SchemaCompatibility.schemaNameEquals(newSchema, oldSchema)) {
        return false;
      }

      // Check that each field in the oldSchema can populated the newSchema
      for (final Field oldSchemaField : oldSchema.getFields()) {
        final Field newSchemaField = SchemaCompatibility.lookupWriterField(newSchema, oldSchemaField);
        if (newSchemaField == null) {
          // C4 or C2: newSchema does not correspond to any field in the oldSchema
          return false;
        } else {
          if (!isSchemaCompatible(oldSchemaField.schema(), newSchemaField.schema())) {
            // C5: The fields do not have a compatible type
            return false;
          }
        }
      }

      // Check that new fields added in newSchema have default values as they will not be
      // present in oldSchema and hence cannot be populated on reading records from existing data.
      for (final Field newSchemaField : newSchema.getFields()) {
        final Field oldSchemaField = SchemaCompatibility.lookupWriterField(oldSchema, newSchemaField);
        if (oldSchemaField == null) {
          if (newSchemaField.defaultVal() == null) {
            // C3: newly added field in newSchema does not have a default value
            return false;
          }
        }
      }

      // All fields in the newSchema record can be populated from the oldSchema record
      return true;
    } else {
      // Use the checks implemented by Avro
      // newSchema is the schema which will be used to read the records written earlier using oldSchema. Hence, in the
      // check below, use newSchema as the reader schema and oldSchema as the writer schema.
      org.apache.avro.SchemaCompatibility.SchemaPairCompatibility compatResult =
          org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility(newSchema, oldSchema);
      return compatResult.getType() == org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE;
    }
  }

  public static boolean isSchemaCompatible(String oldSchema, String newSchema) {
    return isSchemaCompatible(new Schema.Parser().parse(oldSchema), new Schema.Parser().parse(newSchema));
  }

  /**
   * Get latest schema either from incoming schema or table schema.
   * @param writeSchema incoming batch's write schema.
   * @param convertTableSchemaToAddNamespace {@code true} if table schema needs to be converted. {@code false} otherwise.
   * @param converterFn converter function to be called over table schema (to add namespace may be). Each caller can decide if any conversion is required.
   * @return the latest schema.
   *
   * @deprecated will be removed (HUDI-4472)
   */
  @Deprecated
  public Schema getLatestSchema(Schema writeSchema, boolean convertTableSchemaToAddNamespace,
      Function1<Schema, Schema> converterFn) {
    Schema latestSchema = writeSchema;
    try {
      if (metaClient.isTimelineNonEmpty()) {
        Schema tableSchema = getTableAvroSchemaWithoutMetadataFields();
        if (convertTableSchemaToAddNamespace && converterFn != null) {
          tableSchema = converterFn.apply(tableSchema);
        }
        if (writeSchema.getFields().size() < tableSchema.getFields().size() && isSchemaCompatible(writeSchema, tableSchema)) {
          // if incoming schema is a subset (old schema) compared to table schema. For eg, one of the
          // ingestion pipeline is still producing events in old schema
          latestSchema = tableSchema;
          LOG.debug("Using latest table schema to rewrite incoming records " + tableSchema.toString());
        }
      }
    } catch (IllegalArgumentException | InvalidTableException e) {
      LOG.warn("Could not find any commits, falling back to using incoming batch's write schema");
    } catch (Exception e) {
      LOG.warn("Unknown exception thrown " + e.getMessage() + ", Falling back to using incoming batch's write schema");
    }
    return latestSchema;
  }

  private MessageType readSchemaFromParquetBaseFile(Path parquetFilePath) throws IOException {
    LOG.info("Reading schema from " + parquetFilePath);

    FileSystem fs = metaClient.getRawFs();
    ParquetMetadata fileFooter =
        ParquetFileReader.readFooter(fs.getConf(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    return fileFooter.getFileMetaData().getSchema();
  }

  private MessageType readSchemaFromHFileBaseFile(Path hFilePath) throws IOException {
    LOG.info("Reading schema from " + hFilePath);

    FileSystem fs = metaClient.getRawFs();
    CacheConfig cacheConfig = new CacheConfig(fs.getConf());
    HoodieHFileReader<IndexedRecord> hFileReader = new HoodieHFileReader<>(fs.getConf(), hFilePath, cacheConfig);
    return convertAvroSchemaToParquet(hFileReader.getSchema());
  }

  private MessageType readSchemaFromORCBaseFile(Path orcFilePath) throws IOException {
    LOG.info("Reading schema from " + orcFilePath);

    FileSystem fs = metaClient.getRawFs();
    HoodieOrcReader<IndexedRecord> orcReader = new HoodieOrcReader<>(fs.getConf(), orcFilePath);
    return convertAvroSchemaToParquet(orcReader.getSchema());
  }

  /**
   * Read schema from a data file from the last compaction commit done.
   *
   * @deprecated please use {@link #getTableAvroSchema(HoodieInstant, boolean)} instead
   */
  public MessageType readSchemaFromLastCompaction(Option<HoodieInstant> lastCompactionCommitOpt) throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant lastCompactionCommit = lastCompactionCommitOpt.orElseThrow(() -> new Exception(
        "Could not read schema from last compaction, no compaction commits found on path " + metaClient));

    // Read from the compacted file wrote
    HoodieCommitMetadata compactionMetadata = HoodieCommitMetadata
        .fromBytes(activeTimeline.getInstantDetails(lastCompactionCommit).get(), HoodieCommitMetadata.class);
    String filePath = compactionMetadata.getFileIdAndFullPaths(metaClient.getBasePathV2()).values().stream().findAny()
        .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for compaction "
            + lastCompactionCommit + ", could not get schema for table " + metaClient.getBasePath()));
    return readSchemaFromBaseFile(filePath);
  }

  private MessageType readSchemaFromLogFile(Path path) throws IOException {
    return readSchemaFromLogFile(metaClient.getRawFs(), path);
  }

  /**
   * Read the schema from the log file on path.
   *
   * @return
   */
  public static MessageType readSchemaFromLogFile(FileSystem fs, Path path) throws IOException {
    try (Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(path), null)) {
      HoodieDataBlock lastBlock = null;
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (block instanceof HoodieDataBlock) {
          lastBlock = (HoodieDataBlock) block;
        }
      }
      return lastBlock != null ? new AvroSchemaConverter().convert(lastBlock.getSchema()) : null;
    }
  }

  /**
   * Gets the InternalSchema for a hoodie table from the HoodieCommitMetadata of the instant.
   *
   * @return InternalSchema for this table
   */
  public Option<InternalSchema> getTableInternalSchemaFromCommitMetadata() {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
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
      LOG.info(String.format("Failed to read operation field from avro schema (%s)", e.getMessage()));
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
            return HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
          } catch (IOException e) {
            throw new HoodieIOException(String.format("Failed to fetch HoodieCommitMetadata for instant (%s)", missingInstant), e);
          }
        });
  }

  private MessageType fetchSchemaFromFiles(Iterator<String> filePaths) throws IOException {
    MessageType type = null;
    while (filePaths.hasNext() && type == null) {
      String filePath = filePaths.next();
      if (filePath.contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
        // this is a log file
        type = readSchemaFromLogFile(new Path(filePath));
      } else {
        type = readSchemaFromBaseFile(filePath);
      }
    }
    return type;
  }

  private MessageType readSchemaFromBaseFile(String filePath) throws IOException {
    if (filePath.contains(HoodieFileFormat.PARQUET.getFileExtension())) {
      return readSchemaFromParquetBaseFile(new Path(filePath));
    } else if (filePath.contains(HoodieFileFormat.HFILE.getFileExtension())) {
      return readSchemaFromHFileBaseFile(new Path(filePath));
    } else if (filePath.contains(HoodieFileFormat.ORC.getFileExtension())) {
      return readSchemaFromORCBaseFile(new Path(filePath));
    } else {
      throw new IllegalArgumentException("Unknown base file format :" + filePath);
    }
  }

  static Schema appendPartitionColumns(Schema dataSchema, String[] partitionFields) {
    // In cases when {@link DROP_PARTITION_COLUMNS} config is set true, partition columns
    // won't be persisted w/in the data files, and therefore we need to append such columns
    // when schema is parsed from data files
    //
    // Here we append partition columns with {@code StringType} as the data type
    if (partitionFields.length == 0) {
      return dataSchema;
    }

    boolean hasPartitionColNotInSchema = Arrays.stream(partitionFields).anyMatch(pf -> !containsFieldInSchema(dataSchema, pf));
    boolean hasPartitionColInSchema = Arrays.stream(partitionFields).anyMatch(pf -> containsFieldInSchema(dataSchema, pf));
    if (hasPartitionColNotInSchema && hasPartitionColInSchema) {
      throw new HoodieIncompatibleSchemaException("Partition columns could not be partially contained w/in the data schema");
    }

    if (hasPartitionColNotInSchema) {
      // when hasPartitionColNotInSchema is true and hasPartitionColInSchema is false, all partition columns
      // are not in originSchema. So we create and add them.
      List<Field> newFields = new ArrayList<>();
      for (String partitionField: partitionFields) {
        newFields.add(new Schema.Field(
            partitionField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE));
      }
      return appendFieldsToSchema(dataSchema, newFields);
    }

    return dataSchema;
  }
}
