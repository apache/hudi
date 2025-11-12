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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Abstract class to implement merging records from base file with incoming records or records from log blocks
 * at a file group level.
 */
public abstract class HoodieAbstractMergeHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> implements HoodieMergeHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAbstractMergeHandle.class);

  protected Map<String, HoodieRecord<T>> keyToNewRecords;
  protected StoragePath newFilePath;
  protected StoragePath oldFilePath;
  protected Option<BaseKeyGenerator> keyGeneratorOpt;
  protected HoodieBaseFile baseFileToMerge;
  protected Option<String[]> partitionFields = Option.empty();
  protected Object[] partitionValues = new Object[0];

  /**
   * Used by writer code path, to upsert new records by providing an iterator to the new records.
   * @param config Hoodie writer configs.
   * @param instantTime current instant time.
   * @param hoodieTable an instance of {@link HoodieTable}
   * @param partitionPath Partition path of the upsert and insert records.
   * @param fileId New file id of the target base file.
   * @param taskContextSupplier Base task context supplier
   * @param baseFile current base file that needs to be read for the records in storage.
   * @param keyGeneratorOpt Optional instance of the {@link org.apache.hudi.keygen.KeyGenerator} used.
   */
  public HoodieAbstractMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                   String partitionPath, String fileId, TaskContextSupplier taskContextSupplier,
                                   HoodieBaseFile baseFile, Option<BaseKeyGenerator> keyGeneratorOpt, boolean preserveMetadata) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier, preserveMetadata);
    this.baseFileToMerge = baseFile;
    this.keyGeneratorOpt = keyGeneratorOpt;
    initPartitionMetadataAndFilePaths(partitionPath);
    initWriteStatus(fileId, partitionPath);
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  /**
   * Used by fg reader merge handle
   */
  protected HoodieAbstractMergeHandle(HoodieWriteConfig config, String instantTime, String partitionPath,
                                      String fileId, HoodieTable<T, I, K, O> hoodieTable, TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier, preserveMetadata);
  }

  @Override
  public StoragePath getOldFilePath() {
    return oldFilePath;
  }

  @Override
  public IOType getIOType() {
    return IOType.MERGE;
  }

  @Override
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
  private void initPartitionMetadataAndFilePaths(String partitionPath) {
    LOG.info("partitionPath:{}, targetFileId to be merged: {}", partitionPath, fileId);
    String latestValidFilePath = baseFileToMerge == null ? null : baseFileToMerge.getFileName();
    HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(
        storage,
        instantTime,
        new StoragePath(config.getBasePath()),
        FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
        hoodieTable.getPartitionMetafileFormat());
    partitionMetadata.trySave();

    String newFileName = createNewFileName(latestValidFilePath);
    oldFilePath = makeNewFilePath(partitionPath, latestValidFilePath);
    newFilePath = makeNewFilePath(partitionPath, newFileName);

    LOG.info(
        "Merging new data into oldPath: {}, as newPath: {}",
        oldFilePath.toString(), newFilePath.toString());

  }

  private void initWriteStatus(String fileId, String partitionPath) {
    writeStatus.setStat(new HoodieWriteStat());
    if (baseFileToMerge != null) {
      writeStatus.getStat().setPrevCommit(baseFileToMerge.getCommitTime());
      // At the moment, we only support SI for overwrite with latest payload. So, we don't need to embed entire file slice here.
      // HUDI-8518 will be taken up to fix it for any payload during which we might require entire file slice to be set here.
      // Already AppendHandle adds all logs file from current file slice to HoodieDeltaWriteStat.
      writeStatus.getStat().setPrevBaseFile(baseFileToMerge.getFileName());
    } else {
      writeStatus.getStat().setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    }
    // file name is same for all records, in this bunch
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    writeStatus.getStat().setPartitionPath(partitionPath);
    writeStatus.getStat().setFileId(fileId);
    LOG.debug("Initializing Write status with fileId {} partitionPath {}", fileId, partitionPath);
    setWriteStatusPath();
  }

  private void validateAndSetAndKeyGenProps(Option<BaseKeyGenerator> keyGeneratorOpt, boolean populateMetaFields) {
    ValidationUtils.checkArgument(populateMetaFields == !keyGeneratorOpt.isPresent());
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  protected String createNewFileName(String oldFileName) {
    return FSUtils.makeBaseFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
  }

  protected void makeOldAndNewFilePaths(String partitionPath, String oldFileName, String newFileName) {
    oldFilePath = makeNewFilePath(partitionPath, oldFileName);
    newFilePath = makeNewFilePath(partitionPath, newFileName);
  }

  public static HoodieBaseFile getLatestBaseFile(HoodieTable<?, ?, ?, ?> hoodieTable, String partitionPath, String fileId) {
    Option<HoodieBaseFile> baseFileOp = hoodieTable.getBaseFileOnlyView().getLatestBaseFile(partitionPath, fileId);
    if (!baseFileOp.isPresent()) {
      throw new NoSuchElementException(String.format("FileID %s of partition path %s does not exist.", fileId, partitionPath));
    }
    return baseFileOp.get();
  }

  protected void setWriteStatusPath() {
    writeStatus.getStat().setPath(new StoragePath(config.getBasePath()), newFilePath);
  }
}
