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

package org.apache.hudi.io;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.TransactionConflictResolutionStrategy;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * Base class for all write operations logically performed at the file group level.
 */
public abstract class HoodieWriteHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieIOHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteHandle.class);

  /**
   * A special record returned by {@link HoodieRecordPayload}, which means
   * {@link HoodieWriteHandle} should just skip this record.
   * This record is only used for {@link HoodieRecordPayload} currently, so it should not
   * shuffle though network, we can compare the record locally by the equal method.
   * The HoodieRecordPayload#combineAndGetUpdateValue and HoodieRecordPayload#getInsertValue
   * have 3 kind of return:
   * 1、Option.empty
   * This means we should delete this record.
   * 2、IGNORE_RECORD
   * This means we should not process this record,just skip.
   * 3、Other non-empty record
   * This means we should process this record.
   *
   * We can see the usage of IGNORE_RECORD in
   * org.apache.spark.sql.hudi.command.payload.ExpressionPayload
   */
  public static IgnoreRecord IGNORE_RECORD = new IgnoreRecord();

  /**
   * The specified schema of the table. ("specified" denotes that this is configured by the client,
   * as opposed to being implicitly fetched out of the commit metadata)
   */
  protected final Schema tableSchema;
  protected final Schema tableSchemaWithMetaFields;

  /**
   * The write schema. In most case the write schema is the same to the
   * input schema. But if HoodieWriteConfig#WRITE_SCHEMA is specified,
   * we use the WRITE_SCHEMA as the write schema.
   *
   * This is useful for the case of custom HoodieRecordPayload which do some conversion
   * to the incoming record in it. e.g. the ExpressionPayload do the sql expression conversion
   * to the input.
   */
  protected final Schema writeSchema;
  protected final Schema writeSchemaWithMetaFields;

  protected HoodieTimer timer;
  protected WriteStatus writeStatus;
  protected final String partitionPath;
  protected final String fileId;
  protected final String writeToken;
  protected final TaskContextSupplier taskContextSupplier;
  // For full schema evolution
  protected final boolean schemaOnReadEnabled;

  public HoodieWriteHandle(HoodieWriteConfig config, String instantTime, String partitionPath,
                           String fileId, HoodieTable<T, I, K, O> hoodieTable, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, partitionPath, fileId, hoodieTable,
        Option.empty(), taskContextSupplier);
  }

  protected HoodieWriteHandle(HoodieWriteConfig config, String instantTime, String partitionPath, String fileId,
                              HoodieTable<T, I, K, O> hoodieTable, Option<Schema> overriddenSchema,
                              TaskContextSupplier taskContextSupplier) {
    super(config, Option.of(instantTime), hoodieTable);
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    this.tableSchema = overriddenSchema.orElseGet(() -> getSpecifiedTableSchema(config));
    this.tableSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(tableSchema, config.allowOperationMetadataField());
    this.writeSchema = overriddenSchema.orElseGet(() -> getWriteSchema(config));
    this.writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
    this.timer = new HoodieTimer().startTimer();
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        !hoodieTable.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
    this.taskContextSupplier = taskContextSupplier;
    this.writeToken = makeWriteToken();
    schemaOnReadEnabled = !isNullOrEmpty(hoodieTable.getConfig().getInternalSchema());
  }

  /**
   * Get the specified table schema.
   * @param config
   * @return
   */
  private static Schema getSpecifiedTableSchema(HoodieWriteConfig config) {
    return new Schema.Parser().parse(config.getSchema());
  }

  /**
   * Get the schema, of the actual write.
   *
   * @param config
   * @return
   */
  private static Schema getWriteSchema(HoodieWriteConfig config) {
    return new Schema.Parser().parse(config.getWriteSchema());
  }

  /**
   * Generate a write token based on the currently running spark task and its place in the spark dag.
   */
  private String makeWriteToken() {
    return FSUtils.makeWriteToken(getPartitionId(), getStageId(), getAttemptId());
  }

  public Path makeNewPath(String partitionPath) {
    Path path = FSUtils.getPartitionPath(config.getBasePath(), partitionPath);
    try {
      if (!fs.exists(path)) {
        fs.mkdirs(path); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }

    return new Path(path.toString(), FSUtils.makeBaseFileName(instantTime, writeToken, fileId,
        hoodieTable.getMetaClient().getTableConfig().getBaseFileFormat().getFileExtension()));
  }

  /**
   * Make new file path with given file name.
   */
  protected Path makeNewFilePath(String partitionPath, String fileName) {
    String relativePath = new Path((partitionPath.isEmpty() ? "" : partitionPath + "/")
        + fileName).toString();
    return new Path(config.getBasePath(), relativePath);
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  protected Option<Path> createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);

    // do early conflict detection before create markers.
    if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()
        && config.isEarlyConflictDetectionEnable()) {

      ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();
      if (resolutionStrategy instanceof TransactionConflictResolutionStrategy) {
        return createMarkerFileWithTransaction(resolutionStrategy, writeMarkers, partitionPath, dataFileName);
      } else {
        return createMarkerFileWithEarlyConflictDetection(resolutionStrategy, writeMarkers, partitionPath, dataFileName);
      }
    }

    return writeMarkers.create(partitionPath, dataFileName, getIOType());
  }

  private Option<Path> createMarkerFileWithEarlyConflictDetection(ConflictResolutionStrategy resolutionStrategy, WriteMarkers writeMarkers, String partitionPath, String dataFileName) {
    if (resolutionStrategy.hasMarkerConflict(writeMarkers, config, fs, partitionPath, fileId)) {
      resolutionStrategy.resolveMarkerConflict(writeMarkers, partitionPath, fileId);
    }
    return writeMarkers.create(partitionPath, dataFileName, getIOType());
  }

  private Option<Path> createMarkerFileWithTransaction(ConflictResolutionStrategy resolutionStrategy, WriteMarkers writeMarkers, String partitionPath, String dataFileName) {
    TransactionManager txnManager = new TransactionManager(config, fs, partitionPath, fileId);
    try {
      // Need to do transaction before create marker file when using early conflict detection
      txnManager.beginTransaction(partitionPath, fileId);
      return createMarkerFileWithEarlyConflictDetection(resolutionStrategy, writeMarkers, partitionPath, dataFileName);

    } catch (Exception e) {
      LOG.warn("Exception occurs during create marker file in early conflict detection mode.");
      throw e;
    } finally {
      // End transaction after created marker file.
      txnManager.endTransaction(partitionPath + "/" + fileId);
      txnManager.close();
    }
  }

  public Schema getWriterSchemaWithMetaFields() {
    return writeSchemaWithMetaFields;
  }

  /**
   * Determines whether we can accept the incoming records, into the current file. Depending on
   * <p>
   * - Whether it belongs to the same partitionPath as existing records - Whether the current file written bytes lt max
   * file size
   */
  public boolean canWrite(HoodieRecord record) {
    return false;
  }

  boolean layoutControlsNumFiles() {
    return hoodieTable.getStorageLayout().determinesNumFileGroups();
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Option<IndexedRecord> insertValue) {
    // NO_OP
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Option<IndexedRecord> avroRecord, Option<Exception> exception) {
    Option recordMetadata = ((HoodieRecordPayload) record.getData()).getMetadata();
    if (exception.isPresent() && exception.get() instanceof Throwable) {
      // Not throwing exception from here, since we don't want to fail the entire job for a single record
      writeStatus.markFailure(record, exception.get(), recordMetadata);
      LOG.error("Error writing record " + record, exception.get());
    } else {
      write(record, avroRecord);
    }
  }

  /**
   * Rewrite the GenericRecord with the Schema containing the Hoodie Metadata fields.
   */
  protected GenericRecord rewriteRecord(GenericRecord record) {
    return schemaOnReadEnabled ? HoodieAvroUtils.rewriteRecordWithNewSchema(record, writeSchemaWithMetaFields, new HashMap<>())
        : HoodieAvroUtils.rewriteRecord(record, writeSchemaWithMetaFields);
  }

  protected GenericRecord rewriteRecordWithMetadata(GenericRecord record, String fileName) {
    return schemaOnReadEnabled ? HoodieAvroUtils.rewriteEvolutionRecordWithMetadata(record, writeSchemaWithMetaFields, fileName)
        : HoodieAvroUtils.rewriteRecordWithMetadata(record, writeSchemaWithMetaFields, fileName);
  }

  public abstract List<WriteStatus> close();

  public List<WriteStatus> writeStatuses() {
    return Collections.singletonList(writeStatus);
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public abstract IOType getIOType();

  @Override
  protected FileSystem getFileSystem() {
    return hoodieTable.getMetaClient().getFs();
  }

  protected int getPartitionId() {
    return taskContextSupplier.getPartitionIdSupplier().get();
  }

  protected int getStageId() {
    return taskContextSupplier.getStageIdSupplier().get();
  }

  protected long getAttemptId() {
    return taskContextSupplier.getAttemptIdSupplier().get();
  }

  protected HoodieFileWriter createNewFileWriter(String instantTime, Path path, HoodieTable<T, I, K, O> hoodieTable,
      HoodieWriteConfig config, Schema schema, TaskContextSupplier taskContextSupplier) throws IOException {
    return HoodieFileWriterFactory.getFileWriter(instantTime, path, hoodieTable, config, schema, taskContextSupplier);
  }

  private static class IgnoreRecord implements GenericRecord {

    @Override
    public void put(int i, Object v) {

    }

    @Override
    public Object get(int i) {
      return null;
    }

    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public void put(String key, Object v) {

    }

    @Override
    public Object get(String key) {
      return null;
    }
  }
}
