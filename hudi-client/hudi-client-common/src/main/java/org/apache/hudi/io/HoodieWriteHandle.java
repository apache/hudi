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
import org.apache.hudi.table.MarkerFiles;

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

/**
 * Base class for all write operations logically performed at the file group level.
 */
public abstract class HoodieWriteHandle<T extends HoodieRecordPayload, I, K, O> extends HoodieIOHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteHandle.class);

  /**
   * The table schema is the schema of the table which used to read/write record from table.
   */
  protected final Schema tableSchema;
  /**
   * The table schema with meta fields.
   */
  protected final Schema tableSchemaWithMetaFields;
  /**
   * The input schema is the input data schema which used to parse data from incoming record.
   */
  protected final Schema inputSchema;
  /**
   * The input schema with meta fields.
   */
  protected final Schema inputSchemaWithMetaFields;

  protected HoodieTimer timer;
  protected WriteStatus writeStatus;
  protected final String partitionPath;
  protected final String fileId;
  protected final String writeToken;
  protected final TaskContextSupplier taskContextSupplier;

  /**
   *
   * @param config the write config
   * @param instantTime the instance time
   * @param partitionPath the partition path
   * @param fileId the file id
   * @param hoodieTable the hoodie table
   * @param schemaOption the option schema specified for HoodieBootstrapHandle
   * @param taskContextSupplier
   */
  protected HoodieWriteHandle(HoodieWriteConfig config, String instantTime, String partitionPath,
                           String fileId, HoodieTable<T, I, K, O> hoodieTable,
                           Option<Schema> schemaOption,
                           TaskContextSupplier taskContextSupplier) {

    super(config, instantTime, hoodieTable);
    this.partitionPath = partitionPath;
    this.fileId = fileId;
    // If the schemaOption has specified,use it,otherwise load the schema from the table.
    // The schemaOption will be specified only for HoodieBootstrapHandle.
    this.tableSchema = schemaOption.orElseGet(() -> hoodieTable.getTableSchema(config, false));
    this.tableSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(tableSchema);
    this.inputSchema = schemaOption.orElseGet(() -> getInputSchema(config));
    this.inputSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(inputSchema);
    this.timer = new HoodieTimer().startTimer();
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
      !hoodieTable.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
    this.taskContextSupplier = taskContextSupplier;
    this.writeToken = makeWriteToken();
  }

  public HoodieWriteHandle(HoodieWriteConfig config, String instantTime, String partitionPath,
                              String fileId, HoodieTable<T, I, K, O> hoodieTable,
                              TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, partitionPath, fileId, hoodieTable,
        Option.empty(), taskContextSupplier);
  }

  private static Schema getInputSchema(HoodieWriteConfig config) {
    return new Schema.Parser().parse(config.getSchema());
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
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }

    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, writeToken, fileId,
        hoodieTable.getMetaClient().getTableConfig().getBaseFileFormat().getFileExtension()));
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  protected void createMarkerFile(String partitionPath, String dataFileName) {
    MarkerFiles markerFiles = new MarkerFiles(hoodieTable, instantTime);
    markerFiles.create(partitionPath, dataFileName, getIOType());
  }

  public Schema getTableSchemaWithMetaFields() {
    return tableSchemaWithMetaFields;
  }

  public Schema getInputSchemaWithMetaFields() {
    return inputSchemaWithMetaFields;
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
    Option recordMetadata = record.getData().getMetadata();
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
    return HoodieAvroUtils.rewriteRecord(record, inputSchemaWithMetaFields);
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
}
