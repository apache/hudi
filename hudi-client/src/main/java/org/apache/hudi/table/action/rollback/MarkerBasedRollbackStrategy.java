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

import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Performs rollback using marker files generated during the write..
 */
public class MarkerBasedRollbackStrategy implements BaseRollbackActionExecutor.RollbackStrategy {

  private static final Logger LOG = LogManager.getLogger(MarkerBasedRollbackStrategy.class);

  private final HoodieTable<?> table;

  private final transient JavaSparkContext jsc;

  private final HoodieWriteConfig config;

  private final String basePath;

  private final String instantTime;

  public MarkerBasedRollbackStrategy(HoodieTable<?> table, JavaSparkContext jsc, HoodieWriteConfig config, String instantTime) {
    this.table = table;
    this.jsc = jsc;
    this.basePath = table.getMetaClient().getBasePath();
    this.config = config;
    this.instantTime = instantTime;
  }

  private HoodieRollbackStat undoMerge(String mergedBaseFilePath) throws IOException {
    LOG.info("Rolling back by deleting the merged base file:" + mergedBaseFilePath);
    return deleteBaseFile(mergedBaseFilePath);
  }

  private HoodieRollbackStat undoCreate(String createdBaseFilePath) throws IOException {
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

  private HoodieRollbackStat undoAppend(String appendBaseFilePath, HoodieInstant instantToRollback) throws IOException, InterruptedException {
    Path baseFilePathForAppend = new Path(basePath, appendBaseFilePath);
    String fileId = FSUtils.getFileIdFromFilePath(baseFilePathForAppend);
    String baseCommitTime = FSUtils.getCommitTime(baseFilePathForAppend.getName());
    String partitionPath = FSUtils.getRelativePartitionPath(new Path(basePath), new Path(basePath, appendBaseFilePath).getParent());

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
      writer = writer.appendBlock(new HoodieCommandBlock(header));
    } finally {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException io) {
        throw new HoodieIOException("Error closing append of rollback block..", io);
      }
    }

    return HoodieRollbackStat.newBuilder()
        .withPartitionPath(partitionPath)
        // we don't use this field per se. Avoiding the extra file status call.
        .withRollbackBlockAppendResults(Collections.emptyMap())
        .build();
  }

  @Override
  public List<HoodieRollbackStat> execute(HoodieInstant instantToRollback) {
    try {
      MarkerFiles markerFiles = new MarkerFiles(table, instantToRollback.getTimestamp());
      List<String> markerFilePaths = markerFiles.allMarkerFilePaths();
      int parallelism = Math.max(Math.min(markerFilePaths.size(), config.getRollbackParallelism()), 1);
      return jsc.parallelize(markerFilePaths, parallelism)
          .map(markerFilePath -> {
            String typeStr = markerFilePath.substring(markerFilePath.lastIndexOf(".") + 1);
            IOType type = IOType.valueOf(typeStr);
            switch (type) {
              case MERGE:
                return undoMerge(MarkerFiles.stripMarkerSuffix(markerFilePath));
              case APPEND:
                return undoAppend(MarkerFiles.stripMarkerSuffix(markerFilePath), instantToRollback);
              case CREATE:
                return undoCreate(MarkerFiles.stripMarkerSuffix(markerFilePath));
              default:
                throw new HoodieRollbackException("Unknown marker type, during rollback of " + instantToRollback);
            }
          })
          .mapToPair(rollbackStat -> new Tuple2<>(rollbackStat.getPartitionPath(), rollbackStat))
          .reduceByKey(RollbackUtils::mergeRollbackStat)
          .map(Tuple2::_2).collect();
    } catch (Exception e) {
      throw new HoodieRollbackException("Error rolling back using marker files written for " + instantToRollback, e);
    }
  }
}
