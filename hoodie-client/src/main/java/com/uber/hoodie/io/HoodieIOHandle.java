/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.io.storage.HoodieWrapperFileSystem;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.FailSafeConsistencyGuard;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.HoodieTimer;
import com.uber.hoodie.common.util.NoOpConsistencyGuard;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;


public abstract class HoodieIOHandle<T extends HoodieRecordPayload> {

  private static Logger logger = LogManager.getLogger(HoodieIOHandle.class);
  protected final String commitTime;
  protected final String fileId;
  protected final String writeToken;
  protected final HoodieWriteConfig config;
  protected final FileSystem fs;
  protected final HoodieTable<T> hoodieTable;
  protected final Schema originalSchema;
  protected final Schema writerSchema;
  protected HoodieTimer timer;
  protected final WriteStatus writeStatus;

  public HoodieIOHandle(HoodieWriteConfig config, String commitTime, String fileId,
      HoodieTable<T> hoodieTable) {
    this.commitTime = commitTime;
    this.fileId = fileId;
    this.writeToken = makeSparkWriteToken();
    this.config = config;
    this.fs = getFileSystem(hoodieTable, config);
    this.hoodieTable = hoodieTable;
    this.originalSchema = new Schema.Parser().parse(config.getSchema());
    this.writerSchema = createHoodieWriteSchema(originalSchema);
    this.timer = new HoodieTimer().startTimer();
    this.writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        !hoodieTable.getIndex().isImplicitWithStorage(),
        config.getWriteStatusFailureFraction());
  }

  private static FileSystem getFileSystem(HoodieTable hoodieTable, HoodieWriteConfig config) {
    return new HoodieWrapperFileSystem(hoodieTable.getMetaClient().getFs(), config.isConsistencyCheckEnabled()
        ? new FailSafeConsistencyGuard(hoodieTable.getMetaClient().getFs(),
        config.getMaxConsistencyChecks(), config.getInitialConsistencyCheckIntervalMs(),
        config.getMaxConsistencyCheckIntervalMs()) : new NoOpConsistencyGuard());
  }

  /**
   * Generate a write token based on the currently running spark task and its place in the spark dag.
   */
  private static String makeSparkWriteToken() {
    return FSUtils.makeWriteToken(TaskContext.getPartitionId(), TaskContext.get().stageId(),
        TaskContext.get().taskAttemptId());
  }

  public static Schema createHoodieWriteSchema(Schema originalSchema) {
    return HoodieAvroUtils.addMetadataFields(originalSchema);
  }

  public Path makeNewPath(String partitionPath) {
    Path path = FSUtils.getPartitionPath(config.getBasePath(), partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }

    return new Path(path.toString(), FSUtils.makeDataFileName(commitTime, writeToken, fileId));
  }

  /**
   * Creates an empty marker file corresponding to storage writer path
   * @param partitionPath Partition path
   */
  protected void createMarkerFile(String partitionPath) {
    Path markerPath = makeNewMarkerPath(partitionPath);
    try {
      logger.info("Creating Marker Path=" + markerPath);
      fs.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
  }

  /**
   * THe marker path will be  <base-path>/.hoodie/.temp/<instant_ts>/2019/04/25/filename
   * @param partitionPath
   * @return
   */
  private Path makeNewMarkerPath(String partitionPath) {
    Path markerRootPath = new Path(hoodieTable.getMetaClient().getMarkerFolderPath(commitTime));
    Path path = FSUtils.getPartitionPath(markerRootPath, partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    return new Path(path.toString(), FSUtils.makeMarkerFile(commitTime, writeToken, fileId));
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  /**
   * Determines whether we can accept the incoming records, into the current file, depending on
   * <p>
   * - Whether it belongs to the same partitionPath as existing records - Whether the current file
   * written bytes lt max file size
   */
  public boolean canWrite(HoodieRecord record) {
    return false;
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Optional<IndexedRecord> insertValue) {
    // NO_OP
  }

  /**
   * Perform the actual writing of the given record into the backing file.
   */
  public void write(HoodieRecord record, Optional<IndexedRecord> avroRecord, Optional<Exception> exception) {
    Optional recordMetadata = record.getData().getMetadata();
    if (exception.isPresent() && exception.get() instanceof Throwable) {
      // Not throwing exception from here, since we don't want to fail the entire job for a single record
      writeStatus.markFailure(record, exception.get(), recordMetadata);
      logger.error("Error writing record " + record, exception.get());
    } else {
      write(record, avroRecord);
    }
  }

  /**
   * Rewrite the GenericRecord with the Schema containing the Hoodie Metadata fields
   * @param record
   * @return
   */
  protected GenericRecord rewriteRecord(GenericRecord record) {
    return HoodieAvroUtils.rewriteRecord(record, writerSchema);
  }

  public abstract WriteStatus close();

  public abstract WriteStatus getWriteStatus();
}
