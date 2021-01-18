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

package org.apache.hudi.table.action.rollback;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Performs rollback using marker files generated during the write..
 */
public abstract class AbstractMarkerBasedRollbackStrategy<T extends HoodieRecordPayload, I, K, O> implements BaseRollbackActionExecutor.RollbackStrategy {

  private static final Logger LOG = LogManager.getLogger(AbstractMarkerBasedRollbackStrategy.class);

  protected final HoodieTable<T, I, K, O> table;

  protected final transient HoodieEngineContext context;

  protected final HoodieWriteConfig config;

  protected final String basePath;

  protected final String instantTime;

  public AbstractMarkerBasedRollbackStrategy(HoodieTable<T, I, K, O> table, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    this.table = table;
    this.context = context;
    this.basePath = table.getMetaClient().getBasePath();
    this.config = config;
    this.instantTime = instantTime;
  }

  protected HoodieRollbackStat undoMerge(String mergedBaseFilePath) throws IOException {
    LOG.info("Rolling back by deleting the merged base file:" + mergedBaseFilePath);
    return deleteBaseFile(mergedBaseFilePath);
  }

  protected HoodieRollbackStat undoCreate(String createdBaseFilePath) throws IOException {
    LOG.info("Rolling back by deleting the created base file:" + createdBaseFilePath);
    return deleteBaseFile(createdBaseFilePath);
  }

  private HoodieRollbackStat deleteBaseFile(String baseFilePath) throws IOException {
    Path fullDeletePath = new Path(basePath, baseFilePath);
    String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), fullDeletePath.getParent());
    boolean isDeleted = table.getMetaClient().getFs().delete(fullDeletePath);
    return HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath)
        .withDeletedFileResult(baseFilePath, isDeleted)
        .build();
  }

  protected HoodieRollbackStat undoAppend(String appendBaseFilePath, HoodieInstant instantToRollback) throws IOException, InterruptedException {
    Path baseFilePathForAppend = new Path(basePath, appendBaseFilePath);
    String fileId = FSUtils.getFileIdFromFilePath(baseFilePathForAppend);
    String baseCommitTime = FSUtils.getCommitTime(baseFilePathForAppend.getName());
    String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), new Path(basePath, appendBaseFilePath).getParent());
    final Map<FileStatus, Long> writtenLogFileSizeMap = getWrittenLogFileSizeMap(partitionPath, baseCommitTime, fileId);

    HoodieLogFormat.Writer writer = null;
    try {
      Path partitionFullPath = FSUtils.getPartitionPath(basePath, partitionPath);

      if (!table.getMetaClient().getFs().exists(partitionFullPath)) {
        return HoodieRollbackStat.newBuilder()
            .withPartitionPath(partitionPath)
            .build();
      }
      writer = HoodieLogFormat.newWriterBuilder()
          .onParentPath(partitionFullPath)
          .withFileId(fileId)
          .overBaseCommit(baseCommitTime)
          .withFs(table.getMetaClient().getFs())
          .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

      // generate metadata
      Map<HoodieLogBlock.HeaderMetadataType, String> header = RollbackUtils.generateHeader(instantToRollback.getTimestamp(), instantTime);
      // if update belongs to an existing log file
      writer.appendBlock(new HoodieCommandBlock(header));
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException io) {
        throw new HoodieIOException("Error closing append of rollback block..", io);
      }
    }

    // the information of files appended to is required for metadata sync
    Map<FileStatus, Long> filesToNumBlocksRollback = Collections.singletonMap(
          table.getMetaClient().getFs().getFileStatus(Objects.requireNonNull(writer).getLogFile().getPath()),
          1L);

    return HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath)
        .withRollbackBlockAppendResults(filesToNumBlocksRollback)
        .withWrittenLogFileSizeMap(writtenLogFileSizeMap).build();
  }

  /**
   * Returns written log file size map for the respective baseCommitTime to assist in metadata table syncing.
   * @param partitionPath partition path of interest
   * @param baseCommitTime base commit time of interest
   * @param fileId fileId of interest
   * @return Map<FileStatus, File size>
   * @throws IOException
   */
  protected Map<FileStatus, Long> getWrittenLogFileSizeMap(String partitionPath, String baseCommitTime, String fileId) throws IOException {
    return Collections.EMPTY_MAP;
  }
}
