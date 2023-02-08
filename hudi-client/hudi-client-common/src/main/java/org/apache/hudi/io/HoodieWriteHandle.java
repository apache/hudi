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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

/**
 * Base class for all write operations logically performed at the file group level.
 */
public abstract class HoodieWriteHandle<T, I, K, O> extends HoodieIOHandle<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteHandle.class);

  /**
   * Schema used to write records into data files
   */
  protected final Schema writeSchema;
  protected final Schema writeSchemaWithMetaFields;
  protected final HoodieRecordMerger recordMerger;

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
    this.writeSchema = overriddenSchema.orElseGet(() -> getWriteSchema(config));
    this.writeSchemaWithMetaFields = HoodieAvroUtils.addMetadataFields(writeSchema, config.allowOperationMetadataField());
    this.timer = HoodieTimer.start();
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        !hoodieTable.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
    this.taskContextSupplier = taskContextSupplier;
    this.writeToken = makeWriteToken();
    this.schemaOnReadEnabled = !isNullOrEmpty(hoodieTable.getConfig().getInternalSchema());
    this.recordMerger = config.getRecordMerger();
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
  protected void createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime)
        .create(partitionPath, dataFileName, getIOType(), config, fileId, hoodieTable.getMetaClient().getActiveTimeline());
  }

  public Schema getWriterSchemaWithMetaFields() {
    return writeSchemaWithMetaFields;
  }

  public Schema getWriterSchema() {
    return writeSchema;
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
  protected void doWrite(HoodieRecord record, Schema schema, TypedProperties props) {
    // NO_OP
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Schema schema, TypedProperties props) {
    doWrite(record, schema, props);
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
  public FileSystem getFileSystem() {
    return hoodieTable.getMetaClient().getFs();
  }

  public HoodieWriteConfig getConfig() {
    return this.config;
  }

  public HoodieTableMetaClient getHoodieTableMetaClient() {
    return hoodieTable.getMetaClient();
  }
  
  public String getFileId() {
    return this.fileId;
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

  private static Schema getWriteSchema(HoodieWriteConfig config) {
    return new Schema.Parser().parse(config.getWriteSchema());
  }

  protected HoodieLogFormat.Writer createLogWriter(
      Option<FileSlice> fileSlice, String baseCommitTime) throws IOException {
    return createLogWriter(fileSlice, baseCommitTime, null);
  }

  protected HoodieLogFormat.Writer createLogWriter(
      Option<FileSlice> fileSlice, String baseCommitTime, String suffix) throws IOException {
    Option<HoodieLogFile> latestLogFile = fileSlice.isPresent()
        ? fileSlice.get().getLatestLogFile()
        : Option.empty();

    return HoodieLogFormat.newWriterBuilder()
        .onParentPath(FSUtils.getPartitionPath(hoodieTable.getMetaClient().getBasePath(), partitionPath))
        .withFileId(fileId)
        .overBaseCommit(baseCommitTime)
        .withLogVersion(latestLogFile.map(HoodieLogFile::getLogVersion).orElse(HoodieLogFile.LOGFILE_BASE_VERSION))
        .withFileSize(latestLogFile.map(HoodieLogFile::getFileSize).orElse(0L))
        .withSizeThreshold(config.getLogFileMaxSize())
        .withFs(fs)
        .withRolloverLogWriteToken(writeToken)
        .withLogWriteToken(latestLogFile.map(x -> FSUtils.getWriteTokenFromLogPath(x.getPath())).orElse(writeToken))
        .withSuffix(suffix)
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
  }

  protected HoodieLogFormat.Writer createLogWriter(String baseCommitTime, String fileSuffix) {
    try {
      return createLogWriter(Option.empty(),baseCommitTime, fileSuffix);
    } catch (IOException e) {
      throw new HoodieException("Creating logger writer with fileId: " + fileId + ", "
          + "base commit time: " + baseCommitTime + ", "
          + "file suffix: " + fileSuffix + " error");
    }
  }

  protected static Option<IndexedRecord> toAvroRecord(HoodieRecord record, Schema writerSchema, TypedProperties props) {
    try {
      return record.toIndexedRecord(writerSchema, props).map(HoodieAvroIndexedRecord::getData);
    } catch (IOException e) {
      LOG.error("Fail to get indexRecord from " + record, e);
      return Option.empty();
    }
  }
}
