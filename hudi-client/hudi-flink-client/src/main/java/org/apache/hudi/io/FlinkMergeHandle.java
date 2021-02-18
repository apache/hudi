/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link HoodieMergeHandle} that supports merge write incrementally(small data buffers).
 *
 * <p>For a new data buffer, it initialize and set up the next file path to write,
 * and closes the file path when the data buffer write finish. When next data buffer
 * write starts, it rolls over to another new file. If all the data buffers write finish
 * for a checkpoint round, it renames the last new file path as the desired file name
 * (name with the expected file ID).
 *
 * @param <T> Payload type
 * @param <I> Input type
 * @param <K> Key type
 * @param <O> Output type
 */
public class FlinkMergeHandle<T extends HoodieRecordPayload, I, K, O>
    extends HoodieMergeHandle<T, I, K, O>
    implements MiniBatchHandle {

  private static final Logger LOG = LogManager.getLogger(FlinkMergeHandle.class);

  /**
   * Records the current file handles number that rolls over.
   */
  private int rollNumber = 0;
  /**
   * Records the rolled over file paths.
   */
  private List<Path> rolloverPaths;
  /**
   * Whether it is the first time to generate file handle, E.G. the handle has not rolled over yet.
   */
  private boolean needBootStrap = true;

  public FlinkMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                          Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                          TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier);
    rolloverPaths = new ArrayList<>();
  }

  /**
   * Called by compactor code path.
   */
  public FlinkMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                          Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                          HoodieBaseFile dataFileToBeMerged, TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, keyToNewRecords, partitionPath, fileId,
        dataFileToBeMerged, taskContextSupplier);
  }

  /**
   * Use the fileId + "-" + rollNumber as the new fileId of a mini-batch write.
   */
  protected String generatesDataFileName() {
    return FSUtils.makeDataFileName(instantTime, writeToken, fileId + "-" + rollNumber, hoodieTable.getBaseFileExtension());
  }

  public boolean isNeedBootStrap() {
    return needBootStrap;
  }

  @Override
  public List<WriteStatus> close() {
    List<WriteStatus> writeStatus = super.close();
    this.needBootStrap = false;
    return writeStatus;
  }

  /**
   * The difference with the parent method is that there is no need to set up
   * locations for the records.
   *
   * @param fileId        The file ID
   * @param newRecordsItr The incremental records iterator
   */
  @Override
  protected void init(String fileId, Iterator<HoodieRecord<T>> newRecordsItr) {
    initializeIncomingRecordsMap();
    while (newRecordsItr.hasNext()) {
      HoodieRecord<T> record = newRecordsItr.next();
      // NOTE: Once Records are added to map (spillable-map), DO NOT change it as they won't persist
      keyToNewRecords.put(record.getRecordKey(), record);
    }
    LOG.info(String.format("Number of entries in MemoryBasedMap => %d\n"
            + "Total size in bytes of MemoryBasedMap => %d\n"
            + "Number of entries in DiskBasedMap => %d\n"
            + "Size of file spilled to disk => %d",
            ((ExternalSpillableMap) keyToNewRecords).getInMemoryMapNumEntries(),
            ((ExternalSpillableMap) keyToNewRecords).getCurrentInMemoryMapSize(),
            ((ExternalSpillableMap) keyToNewRecords).getDiskBasedMapNumEntries(),
            ((ExternalSpillableMap) keyToNewRecords).getSizeOfFileOnDiskInBytes()));
  }

  /**
   *
   * Rolls over the write handle to prepare for the next batch write.
   *
   * <p>It tweaks the handle state as following:
   *
   * <ul>
   *   <li>Increment the {@code rollNumber}</li>
   *   <li>Book-keep the last file path, these files (except the last one) are temporary that need to be cleaned</li>
   *   <li>Make the last new file path as old</li>
   *   <li>Initialize the new file path and file writer</li>
   * </ul>
   *
   * @param newRecordsItr The records iterator to update
   */
  public void rollOver(Iterator<HoodieRecord<T>> newRecordsItr) {
    init(this.fileId, newRecordsItr);
    this.recordsWritten = 0;
    this.recordsDeleted = 0;
    this.updatedRecordsWritten = 0;
    this.insertRecordsWritten = 0;
    this.writeStatus.setTotalErrorRecords(0);
    this.timer = new HoodieTimer().startTimer();

    rollNumber++;

    rolloverPaths.add(newFilePath);
    oldFilePath = newFilePath;
    // Use the fileId + "-" + rollNumber as the new fileId of a mini-batch write.
    String newFileName = generatesDataFileName();
    String relativePath = new Path((partitionPath.isEmpty() ? "" : partitionPath + "/")
        + newFileName).toString();
    newFilePath = new Path(config.getBasePath(), relativePath);

    try {
      fileWriter = createNewFileWriter(instantTime, newFilePath, hoodieTable, config, writerSchemaWithMetafields, taskContextSupplier);
    } catch (IOException e) {
      throw new HoodieIOException("Error when creating file writer for path " + newFilePath, e);
    }

    LOG.info(String.format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(),
        newFilePath.toString()));
  }

  public void finishWrite() {
    for (int i = 0; i < rolloverPaths.size() - 1; i++) {
      Path path = rolloverPaths.get(i);
      try {
        fs.delete(path, false);
      } catch (IOException e) {
        throw new HoodieIOException("Error when clean the temporary roll file: " + path, e);
      }
    }
    Path lastPath = rolloverPaths.size() > 0
        ? rolloverPaths.get(rolloverPaths.size() - 1)
        : newFilePath;
    String newFileName = FSUtils.makeDataFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
    String relativePath = new Path((partitionPath.isEmpty() ? "" : partitionPath + "/")
        + newFileName).toString();
    final Path desiredPath = new Path(config.getBasePath(), relativePath);
    try {
      fs.rename(lastPath, desiredPath);
    } catch (IOException e) {
      throw new HoodieIOException("Error when rename the temporary roll file: " + lastPath + " to: " + desiredPath, e);
    }
  }
}
