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

package org.apache.hudi.table.action.deltacommit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.io.AppendHandleFactory;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseSparkDeltaCommitActionExecutor<T>
    extends BaseSparkCommitActionExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSparkDeltaCommitActionExecutor.class);

  // UpsertPartitioner for MergeOnRead table type
  private SparkUpsertDeltaCommitPartitioner<T> mergeOnReadUpsertPartitioner;

  public BaseSparkDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                String instantTime, WriteOperationType operationType) {
    this(context, config, table, instantTime, operationType, Option.empty());
  }

  public BaseSparkDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                String instantTime, WriteOperationType operationType,
                                                Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
  }

  @Override
  protected HoodieCommitMetadata appendMetadataForMissingFiles(HoodieCommitMetadata commitMetadata) throws IOException {
    return appendMetadataForMissingFiles(table, getCommitActionType(), instantTime, commitMetadata, config, context, hadoopConf, this.getClass().getSimpleName());
  }

  /* In spark mor table, any failed spark task may generate log files which are not included in write status.
   * We need to add these to CommitMetadata so that it will be synced to MDT and make MDT has correct file info.
   */
  public static HoodieCommitMetadata appendMetadataForMissingFiles(HoodieTable table, String commitActionType, String instantTime,
                                                                   HoodieCommitMetadata commitMetadata, HoodieWriteConfig config,
                                                                   HoodieEngineContext context, Configuration hadoopConf, String classNameForContext) throws IOException {
    if (!table.getMetaClient().getTableConfig().getTableType().equals(HoodieTableType.MERGE_ON_READ)
        || !commitActionType.equals(HoodieActiveTimeline.DELTA_COMMIT_ACTION)) {
      return commitMetadata;
    }

    HoodieCommitMetadata metadata = commitMetadata;
    WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), table, instantTime);
    // if there is log files in this delta commit, we search any invalid log files generated by failed spark task
    boolean hasLogFileInDeltaCommit = metadata.getPartitionToWriteStats()
        .values().stream().flatMap(List::stream)
        .anyMatch(writeStat -> FSUtils.isLogFile(new Path(config.getBasePath(), writeStat.getPath()).getName()));
    if (hasLogFileInDeltaCommit) {
      // get all log files generated by log mark file
      Set<String> logFilesMarkerPath = new HashSet<>(markers.getAppendedLogPaths(context, config.getFinalizeWriteParallelism()));

      // remove valid log files
      for (Map.Entry<String, List<HoodieWriteStat>> partitionAndWriteStats : metadata.getPartitionToWriteStats().entrySet()) {
        for (HoodieWriteStat hoodieWriteStat : partitionAndWriteStats.getValue()) {
          logFilesMarkerPath.remove(hoodieWriteStat.getPath());
        }
      }

      // remaining are log files generated by failed spark task, let's generate write stat for them
      if (logFilesMarkerPath.size() > 0) {
        // populate partition -> map (fileId -> HoodieWriteStat) // we just need one write stat per fileID to fetch some info about the file slice of interest when we want to add a new WriteStat.
        List<Pair<String, Map<String, HoodieWriteStat>>> partitionToFileIdAndWriteStatList = new ArrayList<>();
        for (Map.Entry<String, List<HoodieWriteStat>> partitionAndWriteStats : metadata.getPartitionToWriteStats().entrySet()) {
          String partition = partitionAndWriteStats.getKey();
          Map<String, HoodieWriteStat> fileIdToWriteStat = new HashMap<>();
          partitionAndWriteStats.getValue().forEach(writeStat -> {
            String fileId = writeStat.getFileId();
            if (!fileIdToWriteStat.containsKey(fileId)) {
              fileIdToWriteStat.put(fileId, writeStat);
            }
          });
          partitionToFileIdAndWriteStatList.add(Pair.of(partition, fileIdToWriteStat));
        }

        final Path basePath = new Path(config.getBasePath());
        Map<String, Map<String, List<String>>> partitionToFileIdAndMissingLogFiles = new HashMap<>();
        logFilesMarkerPath
            .stream()
            .forEach(logFilePathStr -> {
              Path logFileFullPath = new Path(config.getBasePath(), logFilePathStr);
              String fileID = FSUtils.getFileId(logFileFullPath.getName());
              String partitionPath = FSUtils.getRelativePartitionPath(basePath, logFileFullPath.getParent());
              if (!partitionToFileIdAndMissingLogFiles.containsKey(partitionPath)) {
                partitionToFileIdAndMissingLogFiles.put(partitionPath, new HashMap<>());
              }
              if (!partitionToFileIdAndMissingLogFiles.get(partitionPath).containsKey(fileID)) {
                partitionToFileIdAndMissingLogFiles.get(partitionPath).put(fileID, new ArrayList<>());
              }
              partitionToFileIdAndMissingLogFiles.get(partitionPath).get(fileID).add(logFilePathStr);
            });

        context.setJobStatus(classNameForContext, "generate writeStat for missing log files");

        // populate partition -> map (fileId -> List <missing log file>)
        List<Map.Entry<String, Map<String, List<String>>>> missingFilesInfo = partitionToFileIdAndMissingLogFiles.entrySet().stream().collect(Collectors.toList());
        HoodiePairData<String, Map<String, List<String>>> partitionToMissingLogFilesHoodieData = context.parallelize(missingFilesInfo).mapToPair(
            (SerializablePairFunction<Map.Entry<String, Map<String, List<String>>>, String, Map<String, List<String>>>) t -> Pair.of(t.getKey(), t.getValue()));

        // populate partition -> map (fileId -> HoodieWriteStat) // we just need one write stat per fileId to fetch some info about the file slice of interest.
        HoodiePairData<String, Map<String, HoodieWriteStat>> partitionToWriteStatHoodieData = context.parallelize(partitionToFileIdAndWriteStatList).mapToPair(
            (SerializablePairFunction<Pair<String, Map<String, HoodieWriteStat>>, String, Map<String, HoodieWriteStat>>) t -> t);

        SerializableConfiguration serializableConfiguration = new SerializableConfiguration(hadoopConf);

        // lets do left outer join to add write stats for missing log files
        List<Pair<String, List<HoodieWriteStat>>> additionalLogFileWriteStat = partitionToWriteStatHoodieData
            .join(partitionToMissingLogFilesHoodieData)
            .map((SerializableFunction<Pair<String, Pair<Map<String, HoodieWriteStat>, Map<String, List<String>>>>, Pair<String, List<HoodieWriteStat>>>) v1 -> {
              final Path basePathLocal = new Path(config.getBasePath());
              String partitionPath = v1.getKey();
              Map<String, HoodieWriteStat> fileIdToOriginalWriteStat = v1.getValue().getKey();
              Map<String, List<String>> missingFileIdToLogFilesList = v1.getValue().getValue();

              List<HoodieWriteStat> missingWriteStats = new ArrayList();
              List<String> missingLogFilesForPartition = new ArrayList();
              missingFileIdToLogFilesList.values().forEach(entry -> missingLogFilesForPartition.addAll(entry));

              // fetch file sizes for missing log files
              Path fullPartitionPath = new Path(config.getBasePath(), partitionPath);
              FileSystem fileSystem = fullPartitionPath.getFileSystem(serializableConfiguration.get());
              List<Option<FileStatus>> fileStatues = FSUtils.getFileStatusesUnderPartition(fileSystem, fullPartitionPath, missingLogFilesForPartition, true);
              Map<String, List<FileStatus>> fileIdToFileStatuses = new HashMap<>();
              fileStatues.forEach(entry -> {
                if (entry.isPresent()) {
                  FileStatus fileStatus = entry.get();
                  String fileId = FSUtils.getFileId(fileStatus.getPath().getName());
                  if (!fileIdToFileStatuses.containsKey(fileId)) {
                    fileIdToFileStatuses.put(fileId, new ArrayList<>());
                  }
                  fileIdToFileStatuses.get(fileId).add(fileStatus);
                }
              });

              // for each missing log file/fileStatus, add a new DeltaWriteStat.
              fileIdToFileStatuses.forEach((k, v) -> {
                String fileId = k;
                List<FileStatus> missingLogFileFileStatuses = v;
                HoodieDeltaWriteStat existedWriteStat =
                    (HoodieDeltaWriteStat) fileIdToOriginalWriteStat.get(fileId); // are there chances that there won't be any write stat in original list?
                List<String> logFiles = new ArrayList<>(existedWriteStat.getLogFiles());
                missingLogFileFileStatuses.forEach(fileStatus -> {
                  // for every missing file, add a new HoodieDeltaWriteStat
                  HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
                  HoodieLogFile logFile = new HoodieLogFile(fileStatus);
                  writeStat.setPath(basePathLocal, logFile.getPath());
                  writeStat.setPartitionPath(partitionPath);
                  writeStat.setFileId(fileId);
                  writeStat.setTotalWriteBytes(logFile.getFileSize());
                  writeStat.setFileSizeInBytes(logFile.getFileSize());
                  writeStat.setLogVersion(logFile.getLogVersion());
                  writeStat.setLogFiles(logFiles);
                  writeStat.setBaseFile(existedWriteStat.getBaseFile());
                  writeStat.setPrevCommit(logFile.getBaseCommitTime());
                  missingWriteStats.add(writeStat);
                });
              });
              return Pair.of(partitionPath, missingWriteStats);
            }).collectAsList();

        // add these write stat to commit meta. deltaWriteStat can be empty due to file missing. See code above to address FileNotFoundException
        for (Pair<String, List<HoodieWriteStat>> partitionDeltaStats : additionalLogFileWriteStat) {
          String partitionPath = partitionDeltaStats.getKey();
          partitionDeltaStats.getValue().forEach(ws -> metadata.addWriteStat(partitionPath, ws));
        }
      }
    }
    return metadata;
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    mergeOnReadUpsertPartitioner = new SparkUpsertDeltaCommitPartitioner<>(profile, (HoodieSparkEngineContext) context, table, config);
    return mergeOnReadUpsertPartitioner;
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    LOG.info("Merging updates for commit " + instantTime + " for file " + fileId);
    if (!table.getIndex().canIndexLogFiles() && mergeOnReadUpsertPartitioner != null
        && mergeOnReadUpsertPartitioner.getSmallFileIds().contains(fileId)) {
      LOG.info("Small file corrections for updates for commit " + instantTime + " for file " + fileId);
      return super.handleUpdate(partitionPath, fileId, recordItr);
    } else {
      HoodieAppendHandle<?, ?, ?, ?> appendHandle = new HoodieAppendHandle<>(config, instantTime, table,
          partitionPath, fileId, recordItr, taskContextSupplier);
      appendHandle.doAppend();
      return Collections.singletonList(appendHandle.close()).iterator();
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) {
    // If canIndexLogFiles, write inserts to log files else write inserts to base files
    if (table.getIndex().canIndexLogFiles()) {
      return new SparkLazyInsertIterable<>(recordItr, true, config, instantTime, table,
          idPfx, taskContextSupplier, new AppendHandleFactory<>());
    } else {
      return super.handleInsert(idPfx, recordItr);
    }
  }

}
