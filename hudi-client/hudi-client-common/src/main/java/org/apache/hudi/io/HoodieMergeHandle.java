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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Abstract class to implement merging records from base file with incoming records or records from log blocks
 * at a file group level.
 */
public abstract class HoodieMergeHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMergeHandle.class);

  protected final HoodieBaseFile baseFileToMerge;
  protected Option<BaseKeyGenerator> keyGeneratorOpt;
  protected Path targetFilePath;
  protected Path oldFilePath;
  protected Option<String[]> partitionFields = Option.empty();
  protected Object[] partitionValues = new Object[0];

  /**
   * Used by writer code path, to upsert new records by providing an iterator to the new records.
   * @param config Hoodie writer configs.
   * @param instantTime current instant time.
   * @param hoodieTable an instance of {@link HoodieTable}
   * @param recordItr Iterator to the incoming upserts and insert records.
   * @param partitionPath Partition path of the upsert and insert records.
   * @param fileId New file id of the target base file.
   * @param taskContextSupplier Base task context supplier
   * @param keyGeneratorOpt Optional instance of the {@link org.apache.hudi.keygen.KeyGenerator} used.
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                           TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    this(config, instantTime, hoodieTable, recordItr, partitionPath, fileId, taskContextSupplier,
        getLatestBaseFile(hoodieTable, partitionPath, fileId), keyGeneratorOpt);
  }

  /**
   * Used by writer code path, to upsert new records by providing an iterator to the new records.
   * @param config Hoodie writer configs.
   * @param instantTime current instant time.
   * @param hoodieTable an instance of {@link HoodieTable}
   * @param recordItr Iterator to the incoming upserts and insert records.
   * @param partitionPath Partition path of the upsert and insert records.
   * @param fileId New file id of the target base file.
   * @param taskContextSupplier Base task context supplier
   * @param baseFile current base file that needs to be read for the records in storage.
   * @param keyGeneratorOpt Optional instance of the {@link org.apache.hudi.keygen.KeyGenerator} used.
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId,
                           TaskContextSupplier taskContextSupplier, HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    this.baseFileToMerge = baseFile;
    this.keyGeneratorOpt = keyGeneratorOpt;
    init(fileId, partitionPath);
  }

  /**
   * Used by compactor code path, since compactor uses {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner}
   * that returns a Map of the records to be merged.
   * @param config Hoodie writer configs.
   * @param instantTime current instant time.
   * @param hoodieTable an instance of {@link HoodieTable}
   * @param keyToNewRecords Map (with record key as the key) to the incoming upserts and insert records.
   * @param partitionPath Partition path of the upsert and insert records.
   * @param fileId New file id of the target base file.
   * @param taskContextSupplier Base task context supplier
   * @param baseFile current base file that needs to be read for the records in storage.
   * @param keyGeneratorOpt Optional instance of the {@link org.apache.hudi.keygen.KeyGenerator} used.
   */
  public HoodieMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                           Map<String, HoodieRecord<T>> keyToNewRecords, String partitionPath, String fileId,
                           HoodieBaseFile baseFile, TaskContextSupplier taskContextSupplier, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
    this.baseFileToMerge = baseFile;
    this.keyGeneratorOpt = keyGeneratorOpt;
    init(fileId, partitionPath);
  }

  /**
   * Called to read the base file, the incoming records, merge the records and write the final base file.
   * @throws IOException
   */
  public abstract void doMerge() throws IOException;

  public Path getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public IOType getIOType() {
    return IOType.MERGE;
  }

  public HoodieBaseFile baseFileForMerge() {
    return baseFileToMerge;
  }

  public void setPartitionFields(Option<String[]> partitionFields) {
    this.partitionFields = partitionFields;
  }

  public Option<String[]> getPartitionFields() {
    return this.partitionFields;
  }

  public void setPartitionValues(Object[] partitionValues) {
    this.partitionValues = partitionValues;
  }

  public Object[] getPartitionValues() {
    return this.partitionValues;
  }

  /**
   * Extract old file path, initialize StorageWriter and WriteStatus.
   */
  private void init(String targetFileId, String partitionPath) {
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
    validateInstantTime(baseFileToMerge, instantTime);

    LOG.info("partitionPath:" + partitionPath + ", targetFileId to be merged:" + targetFileId);
    String latestValidFilePath = baseFileToMerge.getFileName();
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
        new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath),
        hoodieTable.getPartitionMetafileFormat());
    partitionMetadata.trySave(getPartitionId());

    String newFileName = FSUtils.makeBaseFileName(instantTime, writeToken, targetFileId, hoodieTable.getBaseFileExtension());
    makeOldAndNewFilePaths(partitionPath, latestValidFilePath, newFileName);

    LOG.info(String.format("Merging new data into oldPath %s, as newPath %s", oldFilePath.toString(), targetFilePath.toString()));
  }

  private void validateAndSetAndKeyGenProps(Option<BaseKeyGenerator> keyGeneratorOpt, boolean populateMetaFields) {
    ValidationUtils.checkArgument(populateMetaFields == !keyGeneratorOpt.isPresent());
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  protected void makeOldAndNewFilePaths(String partitionPath, String oldFileName, String newFileName) {
    oldFilePath = makeNewFilePath(partitionPath, oldFileName);
    targetFilePath = makeNewFilePath(partitionPath, newFileName);
  }

  public static HoodieBaseFile getLatestBaseFile(HoodieTable<?, ?, ?, ?> hoodieTable, String partitionPath, String fileId) {
    Option<HoodieBaseFile> baseFileOp = hoodieTable.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId);
    if (!baseFileOp.isPresent()) {
      throw new NoSuchElementException(String.format("FileID %s of partition path %s does not exist.", fileId, partitionPath));
    }
    return baseFileOp.get();
  }

  private static void validateInstantTime(HoodieBaseFile baseFileToMerge, String instantTime) {
    if (baseFileToMerge.getCommitTime().compareTo(instantTime) >= 0) {
      LOG.error("The new instant: " + instantTime + " should be strictly higher than the instant of the latest base file: " + baseFileToMerge.getCommitTime()
          + " for fileId: " + baseFileToMerge.getFileId());
      throw new HoodieValidationException("The new instant: " + instantTime + " should be strictly higher than the instant of the latest base file: " + baseFileToMerge.getCommitTime()
          + " for fileId: " + baseFileToMerge.getFileId());
    }
  }
}
