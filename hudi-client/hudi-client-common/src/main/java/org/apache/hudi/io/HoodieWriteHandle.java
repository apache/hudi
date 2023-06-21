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
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFileWriteCallback;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCorruptedDataException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.table.marker.WriteMarkers.FINALIZE_WRITE_COMPLETED;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for all write operations logically performed at the file group level.
 */
public abstract class HoodieWriteHandle<T, I, K, O> extends HoodieIOHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieWriteHandle.class);

  /**
   * Schema used to write records into data files
   */
  protected final Schema writeSchema;
  protected final Schema writeSchemaWithMetaFields;
  protected final HoodieRecordMerger recordMerger;

  protected HoodieTimer timer;
  protected WriteStatus writeStatus;
  protected HoodieRecordLocation newRecordLocation;
  protected List<List<WriteStatus>> recoveredWriteStatuses = new ArrayList<>();
  protected final String partitionPath;
  protected final String fileId;
  protected final String writeToken;
  protected final TaskContextSupplier taskContextSupplier;
  // For full schema evolution
  protected final boolean schemaOnReadEnabled;

  private boolean closed = false;

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
    this.newRecordLocation = new HoodieRecordLocation(instantTime, fileId);
    this.taskContextSupplier = taskContextSupplier;
    this.writeToken = makeWriteToken();
    this.schemaOnReadEnabled = !isNullOrEmpty(hoodieTable.getConfig().getInternalSchema());
    this.recordMerger = config.getRecordMerger();
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        hoodieTable.shouldTrackSuccessRecords(), config.getWriteStatusFailureFraction());
  }

  /**
   * Generate a write token based on the currently running spark task and its place in the spark dag.
   */
  private String makeWriteToken() {
    return FSUtils.makeWriteToken(getPartitionId(), getStageId(), getAttemptId());
  }

  public boolean hasRecoveredWriteStatuses() {
    return !recoveredWriteStatuses.isEmpty();
  }

  public List<List<WriteStatus>> getRecoveredWriteStatuses() {
    return recoveredWriteStatuses;
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
   * @param dataFileName data file for which inprogress marker creation is requested
   * @param markerInstantTime - instantTime associated with the request
   * returns true - inprogress marker successfully created,
   *         false - inprogress marker was not created.
   */
  protected void createInProgressMarkerFile(String partitionPath, String dataFileName, String markerInstantTime) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
    if (!writeMarkers.doesMarkerDirExist()) {
      throw new HoodieIOException(String.format("Marker root directory absent : %s/%s (%s)",
          partitionPath, dataFileName, markerInstantTime));
    }
    writeMarkers.create(partitionPath, dataFileName, getIOType());
  }

  protected boolean recoverWriteStatusIfAvailable(String partitionPath, String dataFileName,
                                                  String markerInstantTime) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
    if (config.isFailRetriesAfterFinalizeWrite()
        && writeMarkers.markerExists(writeMarkers.getCompletionMarkerPath(StringUtils.EMPTY_STRING,
        FINALIZE_WRITE_COMPLETED, markerInstantTime, IOType.CREATE))) {
      throw new HoodieCorruptedDataException(" Failing retry attempt for instant " + instantTime
          + " as the job is trying to re-write the data files, after writes have been finalized.");
    }
    if (config.optimizeTaskRetriesWithMarkers()
        && writeMarkers.markerExists(writeMarkers.getCompletionMarkerPath(partitionPath, fileId, markerInstantTime, getIOType()))) {
      // read the writeStatuses for the previously completed successful attempt(s) from the completed marker file(s) and
      // return false to indicate inprogress marker was not created (for the new snapshot version).
      try {
        if (recoverWriteStatuses(writeMarkers, dataFileName)) {
          return true;
        }
      } catch (HoodieException | IOException e) {
        // failed to read the contents of an existing completed marker. (one or more files)
        // fall through to recreate the files.
      }
    }
    return false;
  }

  /**
   * If a single writer/executor wrote to multiple files (records split into multiple parts) then all the files would have
   * the same write token identifying the task and fileId prefix. When recovering the writestatus for the requested file,
   * we are trying to identify all the files created by the same executor by looking for file with the same fileId prefix
   * and returning write statuses for all of them.
   *
   * @param writeMarkers -  to get marker related information
   * @param dataFileName - first of the multiple files created by the writeHandle.
   * @return true if one or more write statuses were recovered.  false, otherwise.
   * @throws IOException
   */
  private boolean recoverWriteStatuses(WriteMarkers writeMarkers, String dataFileName) throws IOException {
    String fileIdPrefix = fileId.substring(0, fileId.lastIndexOf('-'));
    List<String> markersWithMatchingFileIdPrefix = writeMarkers.allMarkerFilePaths().stream()
        .filter(p -> p.contains(partitionPath) && p.contains(instantTime) && p.contains(fileIdPrefix))
        .collect(Collectors.toList());
    Set<String> candidateFileIDs = markersWithMatchingFileIdPrefix.stream()
        .map(p -> FSUtils.getFileId(new Path(p).getName())).collect(Collectors.toSet());
    boolean recoveredWS = false;
    for (String candidateFileID : candidateFileIDs) {
      // read the writeStatus for the previously completed successful attempt from the completed marker file and
      // return false to indicate in progress marker was not created (for the new snapshot version).
      try {
        Option<byte[]> content = writeMarkers.getContentsOfCompletionMarker(partitionPath, candidateFileID, instantTime, getIOType());
        if (content.isPresent()) {
          recoveredWriteStatuses.addAll(SerializationUtils.deserialize(content.get()));
          recoveredWS = true;
        }
      } catch (HoodieException | IOException e) {
        // failed to read the contents of an existing completed marker.
        throw new HoodieIOException("Failed to read contents from marker file "
            + writeMarkers.getCompletionMarkerPath(partitionPath, candidateFileID, instantTime, getIOType()));
      }
    }
    return recoveredWS;
  }

  // visible for testing
  public void createCompletedMarkerFileIfRequired(String partition, String markerInstantTime, List<WriteStatus> writeStatuses)
      throws IOException {
    if (!config.optimizeTaskRetriesWithMarkers()) {
      return;
    }
    try {
      WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime)
          .createCompletionMarker(partition, fileId, markerInstantTime, getIOType(), true,
              Option.of(SerializationUtils.serialize(writeStatuses)));
    } catch (HoodieException e) {
      // Clean up the data file, if the marker is already present or marker directories don't exist.
      Path partitionPath = FSUtils.getPartitionPath(hoodieTable.getMetaClient().getBasePath(), partition);
      Path dataFilePath = new Path(partitionPath, FSUtils.makeBaseFileName(markerInstantTime, writeToken, fileId, hoodieTable.getBaseFileExtension()));
      FSUtils.cleanupDataFile(dataFilePath, fs);
      throw new HoodieIOException("Cleaned up the data file " + dataFilePath + ", as marker directory is absent.");
    }
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

  protected boolean isClosed() {
    return closed;
  }

  protected void markClosed() {
    this.closed = true;
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
        .withLogWriteCallback(getLogWriteCallback())
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
  }

  protected HoodieLogFileWriteCallback getLogWriteCallback() {
    return new AppendLogWriteCallback();
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

  protected class AppendLogWriteCallback implements HoodieLogFileWriteCallback {
    // here we distinguish log files created from log files being appended. Considering following scenario:
    // An appending task write to log file.
    // (1) append to existing file file_instant_writetoken1.log.1
    // (2) rollover and create file file_instant_writetoken2.log.2
    // Then this task failed and retry by a new task.
    // (3) append to existing file file_instant_writetoken1.log.1
    // (4) rollover and create file file_instant_writetoken3.log.2
    // finally file_instant_writetoken2.log.2 should not be committed to hudi, we use marker file to delete it.
    // keep in mind that log file is not always fail-safe unless it never roll over

    @Override
    public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
      WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
      return writeMarkers.createIfNotExists(partitionPath, logFileToAppend.getFileName(), IOType.APPEND,
          config, fileId, hoodieTable.getMetaClient().getActiveTimeline()).isPresent();
    }

    @Override
    public boolean preLogFileCreate(HoodieLogFile logFileToCreate) {
      WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
      return writeMarkers.create(partitionPath, logFileToCreate.getFileName(), IOType.CREATE,
          config, fileId, hoodieTable.getMetaClient().getActiveTimeline()).isPresent();
    }
  }
}
