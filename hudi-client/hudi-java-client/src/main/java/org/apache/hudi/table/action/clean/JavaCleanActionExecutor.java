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

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaCleanActionExecutor<T extends HoodieRecordPayload> extends
    BaseCleanActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(JavaCleanActionExecutor.class);

  public JavaCleanActionExecutor(HoodieEngineContext context,
                                  HoodieWriteConfig config,
                                  HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                  String instantTime) {
    super(context, config, table, instantTime);
  }

  @Override
  List<HoodieCleanStat> clean(HoodieEngineContext context, HoodieCleanerPlan cleanerPlan) {

    Iterator<ImmutablePair<String, CleanFileInfo>> filesToBeDeletedPerPartition = cleanerPlan.getFilePathsToBeDeletedPerPartition().entrySet().stream()
        .flatMap(x -> x.getValue().stream().map(y -> new ImmutablePair<>(x.getKey(), new CleanFileInfo(y.getFilePath(), y.getIsBootstrapBaseFile())))).iterator();

    Stream<Pair<String, PartitionCleanStat>> partitionCleanStats =
        deleteFilesFunc(filesToBeDeletedPerPartition, table)
            .collect(Collectors.groupingBy(Pair::getLeft))
            .entrySet().stream()
            .map(x -> new ImmutablePair(x.getKey(), x.getValue().stream().map(y -> y.getRight()).reduce(PartitionCleanStat::merge).get()));

    Map<String, PartitionCleanStat> partitionCleanStatsMap = partitionCleanStats
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    // Return PartitionCleanStat for each partition passed.
    return cleanerPlan.getFilePathsToBeDeletedPerPartition().keySet().stream().map(partitionPath -> {
      PartitionCleanStat partitionCleanStat = partitionCleanStatsMap.containsKey(partitionPath)
          ? partitionCleanStatsMap.get(partitionPath)
          : new PartitionCleanStat(partitionPath);
      HoodieActionInstant actionInstant = cleanerPlan.getEarliestInstantToRetain();
      return HoodieCleanStat.newBuilder().withPolicy(config.getCleanerPolicy()).withPartitionPath(partitionPath)
          .withEarliestCommitRetained(Option.ofNullable(
              actionInstant != null
                  ? new HoodieInstant(HoodieInstant.State.valueOf(actionInstant.getState()),
                  actionInstant.getAction(), actionInstant.getTimestamp())
                  : null))
          .withDeletePathPattern(partitionCleanStat.deletePathPatterns())
          .withSuccessfulDeletes(partitionCleanStat.successDeleteFiles())
          .withFailedDeletes(partitionCleanStat.failedDeleteFiles())
          .withDeleteBootstrapBasePathPatterns(partitionCleanStat.getDeleteBootstrapBasePathPatterns())
          .withSuccessfulDeleteBootstrapBaseFiles(partitionCleanStat.getSuccessfulDeleteBootstrapBaseFiles())
          .withFailedDeleteBootstrapBaseFiles(partitionCleanStat.getFailedDeleteBootstrapBaseFiles())
          .build();
    }).collect(Collectors.toList());
  }

  private static Stream<Pair<String, PartitionCleanStat>> deleteFilesFunc(Iterator<ImmutablePair<String, CleanFileInfo>> iter, HoodieTable table) {
    Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();
    FileSystem fs = table.getMetaClient().getFs();

    while (iter.hasNext()) {
      Pair<String, CleanFileInfo> partitionDelFileTuple = iter.next();
      String partitionPath = partitionDelFileTuple.getLeft();
      Path deletePath = new Path(partitionDelFileTuple.getRight().getFilePath());
      String deletePathStr = deletePath.toString();
      Boolean deletedFileResult = null;
      try {
        deletedFileResult = deleteFileAndGetResult(fs, deletePathStr);
      } catch (IOException e) {
        LOG.error("Delete file failed");
      }
      if (!partitionCleanStatMap.containsKey(partitionPath)) {
        partitionCleanStatMap.put(partitionPath, new PartitionCleanStat(partitionPath));
      }
      boolean isBootstrapBasePathFile = partitionDelFileTuple.getRight().isBootstrapBaseFile();
      PartitionCleanStat partitionCleanStat = partitionCleanStatMap.get(partitionPath);
      if (isBootstrapBasePathFile) {
        // For Bootstrap Base file deletions, store the full file path.
        partitionCleanStat.addDeleteFilePatterns(deletePath.toString(), true);
        partitionCleanStat.addDeletedFileResult(deletePath.toString(), deletedFileResult, true);
      } else {
        partitionCleanStat.addDeleteFilePatterns(deletePath.getName(), false);
        partitionCleanStat.addDeletedFileResult(deletePath.getName(), deletedFileResult, false);
      }
    }
    return partitionCleanStatMap.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue()));
  }
}
