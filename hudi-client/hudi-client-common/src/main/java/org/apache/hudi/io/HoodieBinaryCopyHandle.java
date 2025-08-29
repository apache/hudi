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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.util.HoodieFileMetadataMerger;
import org.apache.hudi.io.storage.HoodieFileBinaryCopier;
import org.apache.hudi.parquet.io.HoodieParquetFileBinaryCopier;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.HoodieAvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Compared to other Write Handles, HoodieBinaryCopyHandle merge multiple inputFiles into a single outputFile without performing
 * extra operations like data serialization/deserialization or compression/decompression.
 *
 * Instead, it directly merges file blocks (e.g., Parquet row groups) at the binary stream level and merge the metadata,
 * enabling highly efficient data consolidation.
 *
 */
public class HoodieBinaryCopyHandle<T, I, K, O> extends HoodieWriteHandle<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBinaryCopyHandle.class);
  protected final HoodieFileBinaryCopier writer;
  private final List<StoragePath> inputFiles;
  private final StoragePath path;
  private final Configuration conf;
  private final MessageType writeScheMessageType;
  protected long recordsWritten = 0;
  protected long insertRecordsWritten = 0;

  private MessageType getWriteSchema(HoodieWriteConfig config, List<StoragePath> inputFiles, Configuration conf, HoodieTable<?, ?, ?, ?> table) {
    if (!config.isBinaryCopySchemaEvolutionEnabled() && !inputFiles.isEmpty()) {
      // When schema evolution is disabled, use the schema from the first input file
      // All files should have the same schema in this case
      try {
        ParquetUtils parquetUtils = new ParquetUtils();
        MessageType fileSchema = parquetUtils.readSchema(table.getStorage(), inputFiles.get(0));
        LOG.info("Binary copy schema evolution disabled. Using schema from input file: " + inputFiles.get(0));
        return fileSchema;
      } catch (Exception e) {
        LOG.error("Failed to read schema from input file", e);
        throw new HoodieIOException("Failed to read schema from input file when schema evolution is disabled: " + inputFiles.get(0),
            e instanceof IOException ? (IOException) e : new IOException(e));
      }
    } else {
      // Default behavior: use the table's write schema for evolution
      return new HoodieAvroSchemaConverter(conf).convert(writeSchemaWithMetaFields);
    }
  }

  public HoodieBinaryCopyHandle(
      HoodieWriteConfig config,
      String instantTime,
      String partitionPath,
      String fileId,
      HoodieTable<T, I, K, O> hoodieTable,
      TaskContextSupplier taskContextSupplier,
      List<StoragePath> inputFilePaths) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier, true);
    this.inputFiles = inputFilePaths;
    this.conf = hoodieTable.getStorageConf().unwrapAs(Configuration.class);
    // When schema evolution is disabled, use the schema from one of the input files
    // Otherwise, use the table's write schema
    this.writeScheMessageType = getWriteSchema(config, inputFilePaths, conf, hoodieTable);
    HoodieFileMetadataMerger fileMetadataMerger = new HoodieFileMetadataMerger();
    this.path = makeNewPath(partitionPath);
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setStat(new HoodieWriteStat());
    this.writer = new HoodieParquetFileBinaryCopier(
        conf,
        CompressionCodecName.fromConf(config.getStringOrDefault(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME)),
        fileMetadataMerger);
  }

  public void write() {
    LOG.info("Start to merge source files " + this.inputFiles + " into target file: " + this.path
        + ". Please pay attention that we will not rolling files based on max-file-size config during binary copy.");
    HoodieTimer timer = HoodieTimer.start();
    long records = 0;
    try {
      boolean schemaEvolutionEnabled = config.isBinaryCopySchemaEvolutionEnabled();
      LOG.info("Schema evolution enabled for binary copy: {}", schemaEvolutionEnabled);
      records = this.writer.binaryCopy(inputFiles, Collections.singletonList(path), writeScheMessageType, schemaEvolutionEnabled);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    } finally {
      this.recordsWritten = records;
      this.insertRecordsWritten = records;
    }
    LOG.info("Finish rewriting " + this.path + ". Using " + timer.endTimer() + " mills");
  }

  @Override
  public List<WriteStatus> close() {
    LOG.info("Closing the file " + writeStatus.getFileId() + " as we are done with all the records " + recordsWritten);
    try {
      this.writer.close();

      HoodieWriteStat stat = writeStatus.getStat();
      stat.setPartitionPath(writeStatus.getPartitionPath());
      stat.setNumWrites(recordsWritten);
      stat.setNumInserts(insertRecordsWritten);
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(writeStatus.getFileId());
      stat.setPath(new StoragePath(config.getBasePath()), path);
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());

      long fileSize = storage.getPathInfo(path).getLength();
      stat.setTotalWriteBytes(fileSize);
      stat.setFileSizeInBytes(fileSize);

      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalCreateTime(timer.endTimer());
      stat.setRuntimeStats(runtimeStats);

      LOG.info(String.format("HoodieBinaryCopyHandle for partitionPath %s fileID %s, took %d ms.",
          writeStatus.getStat().getPartitionPath(), writeStatus.getStat().getFileId(),
          writeStatus.getStat().getRuntimeStats().getTotalCreateTime()));

      return Collections.singletonList(writeStatus);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to close the Binary Copy Handle for path " + path, e);
    }
  }

  @Override
  public IOType getIOType() {
    return IOType.CREATE;
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }
}
