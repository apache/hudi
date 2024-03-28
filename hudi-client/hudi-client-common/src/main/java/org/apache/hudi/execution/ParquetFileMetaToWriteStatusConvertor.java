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

package org.apache.hudi.execution;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This class is mainly used by the ParquetToolsExecutionStrategy to generate WriteStatus classes.
 */
public class ParquetFileMetaToWriteStatusConvertor<T extends HoodieRecordPayload, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetFileMetaToWriteStatusConvertor.class);
  private final HoodieTable<T, I, K, O> hoodieTable;
  private final HoodieWriteConfig writeConfig;
  private final FileSystem fs;
  public static final String PREV_COMMIT = "prevCommit";
  public static final String TIME_TAKEN = "timeTaken";

  public ParquetFileMetaToWriteStatusConvertor(HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig writeConfig) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.fs = this.hoodieTable.getMetaClient().getFs();
  }

  /**
   * This method generates writeStatus object from parquet file.
   */
  public WriteStatus convert(String parquetFile, String partitionPath,
                             Map<String, Object> executionConfigs) throws IOException {
    LOG.info("Creating write status for parquet file " + parquetFile);
    WriteStatus writeStatus = (WriteStatus) ReflectionUtils.loadClass(this.writeConfig.getWriteStatusClassName(),
        !this.hoodieTable.getIndex().isImplicitWithStorage(), this.writeConfig.getWriteStatusFailureFraction());
    Path parquetFilePath = new Path(parquetFile);
    writeStatus.setFileId(FSUtils.getFileId(parquetFilePath.getName()));
    writeStatus.setPartitionPath(partitionPath);
    generateHoodieWriteStat(writeStatus, parquetFilePath, executionConfigs);
    return writeStatus;
  }

  /**
   * This method generates HoodieWriteStat object and set it as part of WriteStatus object.
   */
  private void generateHoodieWriteStat(
      WriteStatus writeStatus, Path parquetFilePath, Map<String, Object> executionConfigs) throws IOException {
    HoodieWriteStat stat = new HoodieWriteStat();

    // Set row count
    ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(this.fs.getConf(), parquetFilePath,
        ParquetMetadataConverter.NO_FILTER);
    List<BlockMetaData> blockMetaDataList = parquetMetadata.getBlocks();
    long rowCount = blockMetaDataList.stream().mapToLong(BlockMetaData::getRowCount).sum();
    stat.setNumWrites(rowCount);
    stat.setNumInserts(rowCount);

    // Set runtime stats
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime((long) executionConfigs.get(TIME_TAKEN));
    stat.setRuntimeStats(runtimeStats);

    // File size
    FileStatus parquetFileStatus = this.fs.getFileStatus(parquetFilePath);
    long fileSize = parquetFileStatus.getLen();
    stat.setFileSizeInBytes(fileSize);
    stat.setTotalWriteBytes(fileSize);

    stat.setFileId(writeStatus.getFileId());
    stat.setPartitionPath(writeStatus.getPartitionPath());
    stat.setPath(new Path(writeConfig.getBasePath()), parquetFilePath);
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
    stat.setPrevCommit(String.valueOf(executionConfigs.get(PREV_COMMIT)));

    writeStatus.setStat(stat);
  }

}