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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaCompatibility;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
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
import org.apache.hudi.exception.InvalidTableException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * Helper class to read schema from data files and log files and to convert it between different formats.
 */
public class TableSchemaResolver {

  private static final Logger LOG = LogManager.getLogger(TableSchemaResolver.class);
  private final HoodieTableMetaClient metaClient;
  private final boolean withOperationField;

  public TableSchemaResolver(HoodieTableMetaClient metaClient) {
    this(metaClient, false);
  }

  public TableSchemaResolver(HoodieTableMetaClient metaClient, boolean withOperationField) {
    this.metaClient = metaClient;
    this.withOperationField = withOperationField;
  }

  /**
   * Gets the schema for a hoodie table. Depending on the type of table, read from any file written in the latest
   * commit. We will assume that the schema has not changed within a single atomic write.
   *
   * @return Parquet schema for this table
   * @throws Exception
   */
  private MessageType getTableParquetSchemaFromDataFile() throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      switch (metaClient.getTableType()) {
        case COPY_ON_WRITE:
          // If this is COW, get the last commit and read the schema from a file written in the
          // last commit
          HoodieInstant lastCommit =
              activeTimeline.getCommitsTimeline().filterCompletedInstantsWithCommitMetadata()
                      .lastInstant().orElseThrow(() -> new InvalidTableException(metaClient.getBasePath()));
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(activeTimeline.getInstantDetails(lastCommit).get(), HoodieCommitMetadata.class);
          String filePath = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
              .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for commit "
                  + lastCommit + ", could not get schema for table " + metaClient.getBasePath() + ", Metadata :"
                  + commitMetadata));
          return readSchemaFromBaseFile(new Path(filePath));
        case MERGE_ON_READ:
          // If this is MOR, depending on whether the latest commit is a delta commit or
          // compaction commit
          // Get a datafile written and get the schema from that file
          Option<HoodieInstant> lastCompactionCommit = metaClient.getActiveTimeline().getCommitTimeline()
                  .filterCompletedInstantsWithCommitMetadata().lastInstant();
          LOG.info("Found the last compaction commit as " + lastCompactionCommit);

          Option<HoodieInstant> lastDeltaCommit;
          if (lastCompactionCommit.isPresent()) {
            lastDeltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants()
                .findInstantsAfter(lastCompactionCommit.get().getTimestamp(), Integer.MAX_VALUE).lastInstant();
          } else {
            lastDeltaCommit =
                metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
          }
          LOG.info("Found the last delta commit " + lastDeltaCommit);

          if (lastDeltaCommit.isPresent()) {
            HoodieInstant lastDeltaInstant = lastDeltaCommit.get();
            // read from the log file wrote
            commitMetadata = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(lastDeltaInstant).get(),
                HoodieCommitMetadata.class);
            Pair<String, HoodieFileFormat> filePathWithFormat =
                commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream()
                    .filter(s -> s.contains(HoodieLogFile.DELTA_EXTENSION)).findAny()
                    .map(f -> Pair.of(f, HoodieFileFormat.HOODIE_LOG)).orElseGet(() -> {
                      // No Log files in Delta-Commit. Check if there are any parquet files
                      return commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream()
                          .filter(s -> s.contains((metaClient.getTableConfig().getBaseFileFormat().getFileExtension())))
                          .findAny().map(f -> Pair.of(f, HoodieFileFormat.PARQUET)).orElseThrow(() ->
                              new IllegalArgumentException("Could not find any data file written for commit "
                              + lastDeltaInstant + ", could not get schema for table " + metaClient.getBasePath()
                              + ", CommitMetadata :" + commitMetadata));
                    });
            switch (filePathWithFormat.getRight()) {
              case HOODIE_LOG:
                return readSchemaFromLogFile(lastCompactionCommit, new Path(filePathWithFormat.getLeft()));
              case PARQUET:
                return readSchemaFromBaseFile(new Path(filePathWithFormat.getLeft()));
              default:
                throw new IllegalArgumentException("Unknown file format :" + filePathWithFormat.getRight()
                    + " for file " + filePathWithFormat.getLeft());
            }
          } else {
            return readSchemaFromLastCompaction(lastCompactionCommit);
          }
        default:
          LOG.error("Unknown table type " + metaClient.getTableType());
          throw new InvalidTableException(metaClient.getBasePath());
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read data schema", e);
    }
  }

  public Schema getTableAvroSchemaFromDataFile() throws Exception {
    return convertParquetSchemaToAvro(getTableParquetSchemaFromDataFile());
  }

  /**
   * Gets full schema (user + metadata) for a hoodie table in Avro format.
   *
   * @return Avro schema for this table
   * @throws Exception
   */
  public Schema getTableAvroSchema() throws Exception {
    return getTableAvroSchema(true);
  }

  /**
   * Gets schema for a hoodie table in Avro format, can choice if include metadata fields.
   *
   * @param includeMetadataFields choice if include metadata fields
   * @return Avro schema for this table
   * @throws Exception
   */
  public Schema getTableAvroSchema(boolean includeMetadataFields) throws Exception {
    Option<Schema> schemaFromCommitMetadata = getTableSchemaFromCommitMetadata(includeMetadataFields);
    if (schemaFromCommitMetadata.isPresent()) {
      return schemaFromCommitMetadata.get();
    }
    Option<Schema> schemaFromTableConfig = metaClient.getTableConfig().getTableCreateSchema();
    if (schemaFromTableConfig.isPresent()) {
      if (includeMetadataFields) {
        return HoodieAvroUtils.addMetadataFields(schemaFromTableConfig.get(), withOperationField);
      } else {
        return schemaFromTableConfig.get();
      }
    }
    if (includeMetadataFields) {
      return getTableAvroSchemaFromDataFile();
    } else {
      return HoodieAvroUtils.removeMetadataFields(getTableAvroSchemaFromDataFile());
    }
  }

  /**
   * Gets full schema (user + metadata) for a hoodie table in Parquet format.
   *
   * @return Parquet schema for the table
   * @throws Exception
   */
  public MessageType getTableParquetSchema() throws Exception {
    Option<Schema> schemaFromCommitMetadata = getTableSchemaFromCommitMetadata(true);
    if (schemaFromCommitMetadata.isPresent()) {
      return convertAvroSchemaToParquet(schemaFromCommitMetadata.get());
    }
    Option<Schema> schemaFromTableConfig = metaClient.getTableConfig().getTableCreateSchema();
    if (schemaFromTableConfig.isPresent()) {
      Schema schema = HoodieAvroUtils.addMetadataFields(schemaFromTableConfig.get(), withOperationField);
      return convertAvroSchemaToParquet(schema);
    }
    return getTableParquetSchemaFromDataFile();
  }

  /**
   * Gets users data schema for a hoodie table in Avro format.
   *
   * @return  Avro user data schema
   * @throws Exception
   */
  public Schema getTableAvroSchemaWithoutMetadataFields() throws Exception {
    return getTableAvroSchema(false);
  }

  /**
   * Gets users data schema for a hoodie table in Avro format of the instant.
   *
   * @param instant will get the instant data schema
   * @return  Avro user data schema
   * @throws Exception
   */
  public Schema getTableAvroSchemaWithoutMetadataFields(HoodieInstant instant) throws Exception {
    Option<Schema> schemaFromCommitMetadata = getTableSchemaFromCommitMetadata(instant, false);
    if (schemaFromCommitMetadata.isPresent()) {
      return schemaFromCommitMetadata.get();
    }
    Option<Schema> schemaFromTableConfig = metaClient.getTableConfig().getTableCreateSchema();
    if (schemaFromTableConfig.isPresent()) {
      return schemaFromTableConfig.get();
    }
    return HoodieAvroUtils.removeMetadataFields(getTableAvroSchemaFromDataFile());
  }

  /**
   * Gets the schema for a hoodie table in Avro format from the HoodieCommitMetadata of the last commit.
   *
   * @return Avro schema for this table
   */
  private Option<Schema> getTableSchemaFromCommitMetadata(boolean includeMetadataFields) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    if (timeline.lastInstant().isPresent()) {
      return getTableSchemaFromCommitMetadata(timeline.lastInstant().get(), includeMetadataFields);
    } else {
      return Option.empty();
    }
  }


  /**
   * Gets the schema for a hoodie table in Avro format from the HoodieCommitMetadata of the instant.
   *
   * @return Avro schema for this table
   */
  private Option<Schema> getTableSchemaFromCommitMetadata(HoodieInstant instant, boolean includeMetadataFields) {
    try {
      HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      byte[] data = timeline.getInstantDetails(instant).get();
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
      String existingSchemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);

      if (StringUtils.isNullOrEmpty(existingSchemaStr)) {
        return Option.empty();
      }

      Schema schema = new Schema.Parser().parse(existingSchemaStr);
      if (includeMetadataFields) {
        schema = HoodieAvroUtils.addMetadataFields(schema, withOperationField);
      }
      return Option.of(schema);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from commit metadata", e);
    }
  }

  /**
   * Convert a parquet scheme to the avro format.
   *
   * @param parquetSchema The parquet schema to convert
   * @return The converted avro schema
   */
  public Schema convertParquetSchemaToAvro(MessageType parquetSchema) {
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter(metaClient.getHadoopConf());
    return avroSchemaConverter.convert(parquetSchema);
  }

  /**
   * Convert a avro scheme to the parquet format.
   *
   * @param schema The avro schema to convert
   * @return The converted parquet schema
   */
  public MessageType convertAvroSchemaToParquet(Schema schema) {
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
   */
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


  /**
   * Get Last commit's Metadata.
   */
  public Option<HoodieCommitMetadata> getLatestCommitMetadata() {
    try {
      HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
      if (timeline.lastInstant().isPresent()) {
        HoodieInstant instant = timeline.lastInstant().get();
        byte[] data = timeline.getInstantDetails(instant).get();
        return Option.of(HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class));
      } else {
        return Option.empty();
      }
    } catch (Exception e) {
      throw new HoodieException("Failed to get commit metadata", e);
    }
  }

  /**
   * Read the parquet schema from a parquet File.
   */
  public MessageType readSchemaFromBaseFile(Path parquetFilePath) throws IOException {
    LOG.info("Reading schema from " + parquetFilePath);

    FileSystem fs = metaClient.getRawFs();
    if (!fs.exists(parquetFilePath)) {
      throw new IllegalArgumentException(
          "Failed to read schema from data file " + parquetFilePath + ". File does not exist.");
    }
    ParquetMetadata fileFooter =
        ParquetFileReader.readFooter(fs.getConf(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    return fileFooter.getFileMetaData().getSchema();
  }

  /**
   * Read schema from a data file from the last compaction commit done.
   * @throws Exception
   */
  public MessageType readSchemaFromLastCompaction(Option<HoodieInstant> lastCompactionCommitOpt) throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    HoodieInstant lastCompactionCommit = lastCompactionCommitOpt.orElseThrow(() -> new Exception(
        "Could not read schema from last compaction, no compaction commits found on path " + metaClient));

    // Read from the compacted file wrote
    HoodieCommitMetadata compactionMetadata = HoodieCommitMetadata
        .fromBytes(activeTimeline.getInstantDetails(lastCompactionCommit).get(), HoodieCommitMetadata.class);
    String filePath = compactionMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
        .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for compaction "
            + lastCompactionCommit + ", could not get schema for table " + metaClient.getBasePath()));
    return readSchemaFromBaseFile(new Path(filePath));
  }

  /**
   * Read the schema from the log file on path.
   *
   * @return
   */
  public MessageType readSchemaFromLogFile(Path path) throws IOException {
    return readSchemaFromLogFile(metaClient.getRawFs(), path);
  }

  /**
   * Read the schema from the log file on path.
   * @throws Exception
   */
  public MessageType readSchemaFromLogFile(Option<HoodieInstant> lastCompactionCommitOpt, Path path)
      throws Exception {
    MessageType messageType = readSchemaFromLogFile(path);
    // Fall back to read the schema from last compaction
    if (messageType == null) {
      LOG.info("Falling back to read the schema from last compaction " + lastCompactionCommitOpt);
      return readSchemaFromLastCompaction(lastCompactionCommitOpt);
    }
    return messageType;
  }

  /**
   * Read the schema from the log file on path.
   *
   * @return
   */
  public static MessageType readSchemaFromLogFile(FileSystem fs, Path path) throws IOException {
    Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(path), null);
    HoodieDataBlock lastBlock = null;
    while (reader.hasNext()) {
      HoodieLogBlock block = reader.next();
      if (block instanceof HoodieDataBlock) {
        lastBlock = (HoodieDataBlock) block;
      }
    }
    reader.close();
    if (lastBlock != null) {
      return new AvroSchemaConverter().convert(lastBlock.getSchema());
    }
    return null;
  }
}
