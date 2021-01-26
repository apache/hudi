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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

@SuppressWarnings("checkstyle:LineLength")
public class SparkMarkerBasedRollbackStrategy<T extends HoodieRecordPayload> extends AbstractMarkerBasedRollbackStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  public SparkMarkerBasedRollbackStrategy(HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table, HoodieEngineContext context, HoodieWriteConfig config, String instantTime) {
    super(table, context, config, instantTime);
  }

  @Override
  public List<HoodieRollbackStat> execute(HoodieInstant instantToRollback) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    try {
      MarkerFiles markerFiles = new MarkerFiles(table, instantToRollback.getTimestamp());
      List<String> markerFilePaths = markerFiles.allMarkerFilePaths();
      int parallelism = Math.max(Math.min(markerFilePaths.size(), config.getRollbackParallelism()), 1);
      jsc.setJobGroup(this.getClass().getSimpleName(), "Rolling back using marker files");
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

  protected Map<FileStatus, Long> getWrittenLogFileSizeMap(String partitionPathStr, String baseCommitTime, String fileId) throws IOException {
    // collect all log files that is supposed to be deleted with this rollback
    return FSUtils.getAllLogFiles(table.getMetaClient().getFs(),
        FSUtils.getPartitionPath(config.getBasePath(), partitionPathStr), fileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseCommitTime)
        .collect(Collectors.toMap(HoodieLogFile::getFileStatus, value -> value.getFileStatus().getLen()));
  }
}
