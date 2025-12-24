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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.lsm.DataFile;
import org.apache.hudi.table.action.cluster.lsm.LsmClusteringUtils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;

/**
 * Scheduling strategy with restriction that clustering groups can only contain files from same partition.
 */
public class LsmBaseClusteringPlanStrategy<T,I,K,O> extends ClusteringPlanStrategy<T,I,K,O> {
  private static final Logger LOG = LogManager.getLogger(LsmBaseClusteringPlanStrategy.class);
  private HoodieTableMetaClient metaClient;
  public static final BaseFileUtils PARQUET_UTILS = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  protected final transient Configuration conf;
  private final int numRunClusteringTrigger;
  private final int maxSizeAmp;
  protected boolean skipOP;
  protected final long clusteringSmallFileLimit;
  protected final long clusteringMaxBytesInGroup;
  private final boolean numeric;

  protected Option<HoodieClusteringPlan> plan;
  protected transient Schema schema;

  public LsmBaseClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    this.metaClient = table.getMetaClient();
    this.conf = engineContext.getHadoopConf().get();
    this.numRunClusteringTrigger = getWriteConfig().getLsmNunRunClusteringTrigger();
    this.maxSizeAmp = getWriteConfig().getLsmMaxSizeAMP();
    this.skipOP = !getWriteConfig().isLsmReadFooterEnabled();
    this.clusteringSmallFileLimit = getWriteConfig().getLsmClusteringSmallFileLimit();
    this.clusteringMaxBytesInGroup = getWriteConfig().getLsmClusteringMaxBytesInGroup();
    // get schema from write config
    String schemaStr = getWriteConfig().getSchema();
    this.schema = StringUtils.isNullOrEmpty(schemaStr) ? null : HoodieAvroUtils.createHoodieWriteSchema(schemaStr);
    this.numeric = this.isHoodieRecordKeyNumeric();
  }

  public LsmBaseClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig, Schema schema) {
    super(table, engineContext, writeConfig);
    this.metaClient = table.getMetaClient();
    this.conf = engineContext.getHadoopConf().get();
    this.numRunClusteringTrigger = getWriteConfig().getLsmNunRunClusteringTrigger();
    this.maxSizeAmp = getWriteConfig().getLsmMaxSizeAMP();
    this.skipOP = !getWriteConfig().isLsmReadFooterEnabled();
    this.clusteringSmallFileLimit = getWriteConfig().getLsmClusteringSmallFileLimit();
    this.clusteringMaxBytesInGroup = getWriteConfig().getLsmClusteringMaxBytesInGroup();
    this.schema = schema;
    this.numeric = this.isHoodieRecordKeyNumeric();
  }

  /**
   * Create Clustering group based on files eligible for clustering in the partition.
   * @param fileSlices all file slice of the partition
   * @return Pair of missing partition and clustering groups
   */
  public Stream<Pair<String, HoodieClusteringGroup>> buildClusteringGroupsForPartition(String partitionPath,
                                                                         List<FileSlice> fileSlices,
                                                                         Set<String> level1,
                                                                         Set<String> level1InPending,
                                                                         boolean skipOP) {
    Stream<Pair<String,HoodieClusteringGroup>> hoodieClusteringGroupStream = fileSlices.stream().flatMap(fileSlice -> {
      Pair<Boolean, Stream<HoodieClusteringGroup>> partitionClusteringGroupPair =
          buildClusteringGroups(fileSlice.getLogFiles().collect(Collectors.toList()), level1.contains(fileSlice.getFileId()),
              level1InPending.contains(fileSlice.getFileId()), partitionPath, skipOP);
      return partitionClusteringGroupPair.getRight().map(group -> {
        return partitionClusteringGroupPair.getLeft() ? Pair.of(partitionPath, group) : Pair.of("", group);
      });
    });
    return hoodieClusteringGroupStream;
  }

  /**
   * Create Clustering group based on files slice for clustering in the file slice.
   * @return Pair of partial schedule and clustering groups
   */
  public Pair<Boolean, Stream<HoodieClusteringGroup>> buildClusteringGroups(List<HoodieLogFile> logfiles, boolean level1Existed, boolean level1InPending, String partitionPath, boolean skipOP) {
    // Add a Boolean variable to indicate whether all log files on the partition are participating in clustering.
    List<HoodieLogFile> amp = pickForSizeAmp(logfiles, level1Existed, level1InPending);
    if (amp != null) {
      return Pair.of(false, majorClustering(amp, partitionPath));
    }

    List<HoodieLogFile> filesWithoutLevel1 = pickMinorClustering(logfiles);
    if (filesWithoutLevel1 != null) {
      return Pair.of(false, minorClustering(filesWithoutLevel1, partitionPath, skipOP));
    }

    return Pair.of(false, Stream.empty());
  }

  protected Stream<HoodieClusteringGroup> minorClustering(List<HoodieLogFile> filesWithoutLevel1, String partitionPath, boolean skipOP) {
    if (skipOP) {
      return minorClusteringWithoutGroup(filesWithoutLevel1, partitionPath);
    }

    List<DataFile> files = filesWithoutLevel1.stream().map(hoodieLsmLogFile -> {
      return LsmClusteringUtils.buildDataFile(PARQUET_UTILS, (HoodieLSMLogFile) hoodieLsmLogFile, metaClient.getHadoopConf());
    }).collect(Collectors.toList());

    List<List<DataFile>> newSortedRunGroup = groupFiles(files, numeric);

    ArrayList<HoodieClusteringGroup> res = new ArrayList<>();
    Comparator<DataFile> dataFileComparator = new Comparator<DataFile>() {
      private Comparator<HoodieLogFile> internalComparator = HoodieLogFile.getLogFileComparator();

      @Override
      public int compare(DataFile o1, DataFile o2) {
        return internalComparator.compare(o1.getLogFile(), o2.getLogFile());
      }
    };
    TreeSet<DataFile> dataFileGroup = new TreeSet<>(dataFileComparator);
    long totalFileSize = 0L;
    // each data file is a 'slice'
    // clustering at clustering group level
    for (List<DataFile> group : newSortedRunGroup) {
      if (group.size() > 1) {
        List<HoodieSliceInfo> infos = group.stream()
            .sorted(dataFileComparator)
            .map(dataFile -> {
              return buildHoodieSliceInfo(dataFile, partitionPath);
            }).collect(Collectors.toList());
        res.add(buildHoodieClusteringGroup(infos, true, 0));
      } else if (group.size() == 1) {
        DataFile dataFile = group.get(0);
        if (totalFileSize < this.clusteringMaxBytesInGroup) {
          dataFileGroup.add(dataFile);
          totalFileSize = totalFileSize + dataFile.getDataSize();
        } else {
          if (dataFileGroup.size() > 0) {
            List<HoodieSliceInfo> infos = dataFileGroup.stream().map(d -> {
              return buildHoodieSliceInfo(d, partitionPath);
            }).collect(Collectors.toList());
            res.add(buildHoodieClusteringGroup(infos, false, 0));
          }
          totalFileSize = 0L;
          dataFileGroup.clear();

          dataFileGroup.add(dataFile);
          totalFileSize = totalFileSize + dataFile.getDataSize();
        }
      }
    }

    if (!dataFileGroup.isEmpty()) {
      List<HoodieSliceInfo> infos = dataFileGroup.stream().map(d -> {
        return buildHoodieSliceInfo(d, partitionPath);
      }).collect(Collectors.toList());
      res.add(buildHoodieClusteringGroup(infos, false, 0));
      totalFileSize = 0L;
      dataFileGroup.clear();
    }

    return res.stream();
  }

  protected Stream<HoodieClusteringGroup> minorClusteringWithoutGroup(List<HoodieLogFile> filesWithoutLevel1, String partitionPath) {
    if (filesWithoutLevel1.isEmpty()) {
      return null;
    }

    ArrayList<HoodieClusteringGroup> res = new ArrayList<>();
    long totalFileSize = 0L;
    TreeSet<HoodieLSMLogFile> dataFileGroup = new TreeSet<>(HoodieLogFile.getLogFileComparator());
    for (HoodieLogFile hoodieLogFile : filesWithoutLevel1) {
      HoodieLSMLogFile dataFile = (HoodieLSMLogFile) hoodieLogFile;
      if (totalFileSize < this.clusteringMaxBytesInGroup) {
        dataFileGroup.add(dataFile);
        totalFileSize = totalFileSize + dataFile.getFileSize();
      } else {
        if (!dataFileGroup.isEmpty()) {
          List<HoodieSliceInfo> infos = dataFileGroup.stream().map(d -> {
            return buildHoodieSliceInfo(d, partitionPath);
          }).collect(Collectors.toList());
          res.add(buildHoodieClusteringGroup(infos, false, 0));
        }
        totalFileSize = 0L;
        dataFileGroup.clear();

        dataFileGroup.add(dataFile);
        totalFileSize = totalFileSize + dataFile.getFileSize();
      }
    }

    if (!dataFileGroup.isEmpty()) {
      List<HoodieSliceInfo> infos = dataFileGroup.stream().map(d -> {
        return buildHoodieSliceInfo(d, partitionPath);
      }).collect(Collectors.toList());
      res.add(buildHoodieClusteringGroup(infos, false, 0));
      totalFileSize = 0L;
      dataFileGroup.clear();
    }

    return res.stream();
  }

  // Major Clustering : merge all sorted runs into one
  private Stream<HoodieClusteringGroup> majorClustering(List<HoodieLogFile> files, String partitionPath) {
    List<HoodieSliceInfo> infos = files.stream()
        .sorted(HoodieLogFile.getLogFileComparator())
        .map(file -> {
          return buildHoodieSliceInfo(file, partitionPath);
        }).collect(Collectors.toList());

    return Stream.of(buildHoodieClusteringGroup(infos, false, 1));
  }

  protected HoodieSliceInfo buildHoodieSliceInfo(HoodieLogFile logFile, String partitionPath) {
    return HoodieSliceInfo.newBuilder()
        .setPartitionPath(partitionPath)
        .setFileId(logFile.getFileName()) // use full file name as fileID
        .setDataFilePath(logFile.getPath().toString())
        .setDeltaFilePaths(new ArrayList<>())
        .setBootstrapFilePath(StringUtils.EMPTY_STRING)
        .build();
  }

  private HoodieSliceInfo buildHoodieSliceInfo(DataFile dataFile, String partitionPath) {
    return HoodieSliceInfo.newBuilder()
        .setPartitionPath(partitionPath)
        .setFileId(new Path(dataFile.getPath()).getName()) // use full file name as fileID
        .setDataFilePath(dataFile.getPath())
        .setDeltaFilePaths(new ArrayList<>())
        .setBootstrapFilePath(StringUtils.EMPTY_STRING)
        .build();
  }

  protected HoodieClusteringGroup buildHoodieClusteringGroup(List<HoodieSliceInfo> infos, boolean usingStreamingCopy, int outputLevel) {
    HashMap<String, String> extraMeta = new HashMap<>();
    extraMeta.put(HoodieClusteringConfig.LSM_CLUSTERING_USING_STREAMING_COPY, String.valueOf(usingStreamingCopy));
    extraMeta.put(HoodieClusteringConfig.LSM_CLUSTERING_OUT_PUT_LEVEL, String.valueOf(outputLevel));

    return HoodieClusteringGroup.newBuilder()
        .setSlices(infos)
        .setNumOutputFileGroups(1)
        .setExtraMetadata(extraMeta)
        .build();
  }

  // Minor Clustering
  public List<HoodieLogFile> pickMinorClustering(List<HoodieLogFile> logfiles) {
    List<HoodieLogFile> smallLogfiles = logfiles.stream().filter(logfile -> {
      return logfile.getFileSize() <= this.clusteringSmallFileLimit && ((HoodieLSMLogFile) logfile).getLevelNumber() != 1;
    }).collect(Collectors.toList());

    if (smallLogfiles.size() < numRunClusteringTrigger) {
      return null;
    }

    return smallLogfiles;
  }

  // Major Clustering
  protected List<HoodieLogFile> pickForSizeAmp(List<HoodieLogFile> logFiles, boolean level1Existed, boolean level1InPending) {
    if (level1InPending) {
      LOG.info("Already exists Level 1 operation in pending clustering. Try to schedule minor clustering.");
      return null;
    }

    if (!level1Existed) {
      // 控制第一次 Major Clustering的频率
      long totalSize = logFiles.stream().mapToLong(HoodieLogFile::getFileSize).sum();
      if (totalSize > clusteringMaxBytesInGroup) {
        return logFiles;
      }
      return null;
    }

    // level1 existed
    if (logFiles.size() < numRunClusteringTrigger) {
      return null;
    }

    long level1Size = 0;
    long level0SizeSum = 0;
    for (HoodieLogFile file : logFiles) {
      HoodieLSMLogFile logFile = (HoodieLSMLogFile) file;
      int levelNumber = logFile.getLevelNumber();
      if (levelNumber == 1) {
        level1Size = level1Size + logFile.getFileSize();
      } else {
        level0SizeSum = level0SizeSum + logFile.getFileSize();
      }
    }
    // size amplification = percentage of additional size
    if (level0SizeSum * 100 > maxSizeAmp * level1Size) {
      return logFiles;
    }
    return null;
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan() {
    if (plan != null && plan.isPresent()) {
      return plan;
    }

    if (!checkPrecondition()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for " + metaClient.getBasePath());
    Pair<List<String>, Pair<Set<String>, Set<String>>> partitionsAndInstantsPair = getPartitionPathsToCluster();
    List<String> partitionPaths = partitionsAndInstantsPair.getLeft();
    Set<String> missingPartitions = new HashSet<>();

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }
    // Pair<Missing partition, Clustering group>
    List<Pair<String, HoodieClusteringGroup>> partitionClusteringGroupPair = getEngineContext()
        .flatMap(
            partitionPaths,
            partitionPath -> {
              // get and plan at file level
              Pair<List<FileSlice>, Set<String>> res = getFilesEligibleForClustering(partitionPath);
              List<FileSlice> fileSlicesEligible = res.getLeft();
              Set<String> level1 = res.getRight();
              Set<String> level1InPendingClustering = getLevel1InPendingClustering(partitionPath);
              return buildClusteringGroupsForPartition(partitionPath, fileSlicesEligible, level1, level1InPendingClustering, skipOP);
            },
            partitionPaths.size());

    List<HoodieClusteringGroup> clusteringGroups = partitionClusteringGroupPair.stream().map(pair -> {
      String missingPartition = pair.getLeft();
      if (!StringUtils.isNullOrEmpty(missingPartition)) {
        // missingPartition value is not empty, which means it related candidate fileSlices all not all processed.
        // so that we need to mark this kind of partition as missing partition.
        missingPartitions.add(missingPartition);
      }
      return pair.getRight();
    }).collect(Collectors.toList());

    if (clusteringGroups.isEmpty()) {
      LOG.info("No data available to cluster");
      return Option.empty();
    }

    return buildClusteringPlan(metaClient,
        clusteringGroups, getWriteConfig(),
        partitionsAndInstantsPair.getRight().getLeft(),
        partitionsAndInstantsPair.getRight().getRight(),
        missingPartitions,
        getStrategyParams(),
        getExtraMetadata(),
        getPlanVersion());
  }

  public static Option<HoodieClusteringPlan> buildClusteringPlan(HoodieTableMetaClient metaClient,
                                                                 List<HoodieClusteringGroup> clusteringGroups,
                                                                 HoodieWriteConfig config,
                                                                 Set<String> missingInstants,
                                                                 Set<String> completedInstants,
                                                                 Set<String> missingSchedulePartitions,
                                                                 Map<String, String> para,
                                                                 Map<String, String> extraMeta,
                                                                 int version) {
    //Avoid missing inflight instants generated during plan building
    HoodieActiveTimeline hoodieActiveTimeline = metaClient.reloadActiveTimeline();
    Option<HoodieInstant> clusteringInstant = hoodieActiveTimeline.filterCompletedInstants().getCompletedReplaceTimeline().lastInstant();
    hoodieActiveTimeline.getDeltaCommitTimeline().getInstants().forEach(instant -> {
      if (!clusteringInstant.isPresent() || instant.getTimestamp().compareToIgnoreCase(clusteringInstant.get().getTimestamp()) > 0) {
        if (instant.isCompleted() && !completedInstants.contains(instant.getTimestamp())) {
          missingInstants.add(instant.getTimestamp());
        }
        if (!instant.isCompleted() && !missingInstants.contains(instant.getTimestamp())) {
          missingInstants.add(instant.getTimestamp());
        }
      }
    });
    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(config.getLSMClusteringExecutionStrategyClass())
        .setStrategyParams(para)
        .build();

    return Option.of(HoodieClusteringPlan.newBuilder()
        .setStrategy(strategy)
        .setInputGroups(clusteringGroups)
        .setMissingInstants(new ArrayList<>(missingInstants))
        .setMissingSchedulePartitions(new ArrayList<>(missingSchedulePartitions))
        .setExtraMetadata(extraMeta)
        .setVersion(version)
        .setPreserveHoodieMetadata(config.isPreserveHoodieCommitMetadataForClustering())
        .build());
  }

  @Override
  protected Map<String, String> getStrategyParams() {
    return new HashMap<>();
  }

  public Pair<List<FileSlice>, Set<String>> getFilesEligibleForClustering(String partition) {
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) getHoodieTable().getSliceView();

    Set<String> pendingClusteringFiles = fileSystemView.getFileGroupsInPendingClustering()
        .map(Pair::getKey).map(HoodieFileGroupId::getFileId).collect(Collectors.toSet());

    HashSet<String> level1 = new HashSet<>();
    List<FileSlice> fileSlices = hoodieTable.getSliceView().getLatestFileSlices(partition).map(slice -> {
      List<HoodieLogFile> newLogFiles = dropPending(slice.getLogFiles(), level1, pendingClusteringFiles);

      return new FileSlice(slice.getFileGroupId(), slice.getBaseInstantTime(), null, newLogFiles);
    }).collect(Collectors.toList());

    return Pair.of(fileSlices, level1);
  }

  public Set<String> getLevel1InPendingClustering(String partition) {
    return  ((SyncableFileSystemView) getHoodieTable().getSliceView()).getLevel1FileIdInPendingClustering(partition);
  }

  public List<HoodieLogFile> dropPending(Stream<HoodieLogFile> stream, HashSet<String> levelMap, Set<String> pendingClusteringFiles) {
    return stream.filter(logfile -> {
      if (!pendingClusteringFiles.contains(logfile.getFileName())) {
        int levelNumber = ((HoodieLSMLogFile) logfile).getLevelNumber();
        if (levelNumber == 1) {
          levelMap.add(logfile.getFileId());
        }
        return true;
      }
      return false;
    }).collect(Collectors.toList());
  }

  /**
   * Get the partitions that need to be scheduled for clustering
   * @return Pair(List of partitions, Pair(Set of missing instants, Set of completed instants))
   */
  public Pair<List<String>, Pair<Set<String>, Set<String>>> getPartitionPathsToCluster() {
    if (getWriteConfig().getClusteringPlanInstantsLimit() > 0) {
      // When setting the limit, missing partitions and missing instants are not considered.
      return Pair.of(getLimitedInstantsRelatedPartitions(), Pair.of(Collections.emptySet(), Collections.emptySet()));
    }

    List<HoodieInstant> replaceCommitInstant = hoodieTable.getActiveTimeline().filterCompletedInstants().getCompletedReplaceTimeline().getReverseOrderedInstants().collect(Collectors.toList());
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> lastCompetedClusteringPlan = getLastCompetedClusteringPlan(replaceCommitInstant);
    if (lastCompetedClusteringPlan.isPresent()) {
      return getIncrementalPartitions(lastCompetedClusteringPlan.get());
    } else {
      Set<String> currentMissingInstants = new HashSet<>();
      Set<String> completedInstants = new HashSet<>();
      hoodieTable.getActiveTimeline().getDeltaCommitTimeline().getInstants().stream().forEach(instant -> {
        if (instant.isCompleted()) {
          completedInstants.add(instant.getTimestamp());
        } else {
          currentMissingInstants.add(instant.getTimestamp());
        }
      });
      return Pair.of(FSUtils.getAllPartitionPaths(getEngineContext(), getWriteConfig().getMetadataConfig(), metaClient.getBasePath()),
          Pair.of(currentMissingInstants, completedInstants));
    }
  }

  /**
   * take care of missing instants from plan caused by multi-write
   * @return Pair(List of partitions, Pair(Set of missing instants, Set of completed instants))
   */
  protected Pair<List<String>, Pair<Set<String>, Set<String>>> getIncrementalPartitions(Pair<HoodieInstant, HoodieClusteringPlan> pair) {
    HoodieInstant clusteringInstant = pair.getLeft();
    HoodieClusteringPlan clusteringPlan = pair.getRight();
    List<String> missingPartitionsFromPlan = clusteringPlan.getMissingSchedulePartitions() == null
        ? Collections.emptyList() : clusteringPlan.getMissingSchedulePartitions();
    List<String> missingInstantsFromPlan = clusteringPlan.getMissingInstants() == null
        ? Collections.emptyList() : clusteringPlan.getMissingInstants();

    HoodieActiveTimeline activeTimeline = hoodieTable.getActiveTimeline();

    Set<String> currentMissingInstants = new HashSet<>();
    Set<String> completedInstants = new HashSet<>();
    Set<String> partitions = new HashSet<>(missingPartitionsFromPlan);
    ArrayList<String> res = hoodieTable.getActiveTimeline().getDeltaCommitTimeline().getInstants().stream().filter(instant -> {
      boolean isMissing = missingInstantsFromPlan.contains(instant.getTimestamp());
      boolean afterLastClustering = instant.getTimestamp().compareToIgnoreCase(clusteringInstant.getTimestamp()) > 0;
      if (isMissing || afterLastClustering) {
        if (instant.isCompleted()) {
          completedInstants.add(instant.getTimestamp());
          return true;
        } else {
          currentMissingInstants.add(instant.getTimestamp());
          return false;
        }
      }
      return false;
    }).flatMap(instant -> {
      try {
        HoodieCommitMetadata metadata = TimelineUtils.getCommitMetadata(instant, activeTimeline);
        return metadata.getWriteStats().stream().map(HoodieWriteStat::getPartitionPath);
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    }).distinct().collect(Collectors.toCollection(ArrayList::new));
    partitions.addAll(res);

    LOG.info("Get Missing Instants : " + currentMissingInstants);
    return Pair.of(new ArrayList<>(partitions), Pair.of(currentMissingInstants, completedInstants));
  }

  protected Option<Pair<HoodieInstant, HoodieClusteringPlan>> getLastCompetedClusteringPlan(List<HoodieInstant> replaceCommitInstant) {
    for (HoodieInstant hoodieInstant : replaceCommitInstant) {
      Option<Pair<HoodieInstant, HoodieClusteringPlan>> clusteringPlan =
          ClusteringUtils.getClusteringPlan(metaClient, hoodieInstant, false);
      if (clusteringPlan.isPresent()) {
        return clusteringPlan;
      }
    }
    return Option.empty();
  }

  protected List<String> getLimitedInstantsRelatedPartitions() {
    HoodieWriteConfig writeConfig = getWriteConfig();
    int limit = writeConfig.getClusteringPlanInstantsLimit();
    ValidationUtils.checkArgument(limit > 0);
    HashSet<String> partitions = new HashSet<>();
    HoodieTimeline lsmDeltaCommitTimeline = hoodieTable.getActiveTimeline().getTimelineOfActions(CollectionUtils.createSet(DELTA_COMMIT_ACTION)).filterCompletedInstants();
    lsmDeltaCommitTimeline.getInstantsAsStream().sorted(Comparator.comparing(HoodieInstant::getTimestamp).reversed()).limit(limit).forEach(instant -> {
      try {
        HoodieCommitMetadata metadata =
            HoodieCommitMetadata.fromBytes(lsmDeltaCommitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
        partitions.addAll(metadata.getWritePartitionPaths());
      } catch (IOException e) {
        // ignore Exception here
      }
    });

    if (!partitions.isEmpty()) {
      return new ArrayList<>(partitions);
    } else {
      return FSUtils.getAllPartitionPaths(getEngineContext(), getWriteConfig().getMetadataConfig(), metaClient.getBasePath());
    }
  }

  /**
   * 将Files分组 List<List<DataFile>> res，要求统一组内的DataFile：1.min max没有交集 2.小的放在前面
   */
  public List<List<DataFile>> groupFiles(List<DataFile> files, boolean numeric) {
    // 按 min 升序排序
    List<DataFile> sortedFiles = new ArrayList<>(files);
    sortedFiles.sort((file1, file2) -> {
      String min1 = file1.getMin();
      String min2 = file2.getMin();
      String max1 = file1.getMax();
      String max2 = file2.getMax();

      if (numeric) {
        double min1Double = Double.parseDouble(min1);
        double min2Double = Double.parseDouble(min2);
        double max1Double = Double.parseDouble(max1);
        double max2Double = Double.parseDouble(max2);
        int i = Double.compare(min1Double, min2Double);
        if (i == 0) {
          return Double.compare(max1Double, max2Double);
        }
        return i;
      } else {
        int i = min1.compareToIgnoreCase(min2);
        if (i == 0) {
          return max1.compareToIgnoreCase(max2);
        }
        return i;
      }
    });

    List<Group> groups = new ArrayList<>();

    for (DataFile file : sortedFiles) {
      boolean added = false;
      for (Group group : groups) {
        if (group.canAdd(file, clusteringMaxBytesInGroup)) {
          group.addFile(file);
          added = true;
          break;
        }
      }
      if (!added) {
        groups.add(new Group(file));
      }
    }

    // 转换为 List<List<DataFile>>
    List<List<DataFile>> result = new ArrayList<>();
    for (Group group : groups) {
      result.add(group.getFiles());
    }
    return result;
  }

  // 辅助类，用于维护分组信息
  private static class Group {
    private final List<DataFile> files;
    private long totalSize;
    private String lastMax;

    public Group(DataFile file) {
      this.files = new ArrayList<>();
      this.files.add(file);
      this.totalSize = file.getDataSize();
      this.lastMax = file.getMax();
    }

    public boolean canAdd(DataFile file, long maxSize) {
      // 检查区间是否无交集
      String currentMin = file.getMin();
      String groupLastMax = this.lastMax;
      boolean noOverlap = groupLastMax.compareToIgnoreCase(currentMin) < 0;

      // 检查总大小是否超限
      boolean sizeWithinLimit = (this.totalSize + file.getDataSize()) <= maxSize;

      return noOverlap && sizeWithinLimit;
    }

    public void addFile(DataFile file) {
      this.files.add(file);
      this.totalSize += file.getDataSize();
      this.lastMax = file.getMax();
    }

    public List<DataFile> getFiles() {
      return new ArrayList<>(this.files);
    }
  }

  private boolean isHoodieRecordKeyNumeric() {
    if (schema == null) {
      return false;
    }
    Option<String[]> partitionFields = metaClient.getTableConfig().getPartitionFields();
    if (partitionFields.isPresent() && partitionFields.get().length > 1) {
      return false;
    }
    Option<String[]> recordKeyFields = metaClient.getTableConfig().getRecordKeyFields();
    if (recordKeyFields.isPresent() && recordKeyFields.get().length == 1) {
      // 只关心单一主键
      String recordKeyName = recordKeyFields.get()[0];
      Schema.Field field = schema.getField(recordKeyName);
      return LsmClusteringUtils.isNumericType(field);
    }
    return false;
  }

  public boolean getNumeric() {
    return numeric;
  }
}
