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

import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LSMCleanPlanner<T, I, K, O> extends CleanPlanner<T, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(LSMCleanPlanner.class);
  // List<partitionPath + filename>
  private final List<String> pendingReplaceFiles;
  private final HoodieStorageStrategy storageStrategy;
  private final HoodieTableMetaClient metaClient;
  private final int cleanParallelism;

  public LSMCleanPlanner(HoodieEngineContext context, HoodieTable<T, I, K, O> hoodieTable, HoodieWriteConfig config) {
    super(context, hoodieTable, config);
    ValidationUtils.checkArgument(!config.getCleanerPolicy().equals(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS),
        "Not support KEEP_LATEST_BY_HOURS clean policy for now");
    SyncableFileSystemView fileSystemView = (SyncableFileSystemView) hoodieTable.getSliceView();
    this.pendingReplaceFiles = fileSystemView.getFileGroupsInPendingClustering().map(group -> {
      String partitionPath = group.getLeft().getPartitionPath();
      String filename = group.getLeft().getFileId();
      return partitionPath + filename;
    }).collect(Collectors.toList());
    this.metaClient = hoodieTable.getMetaClient();
    this.storageStrategy = HoodieStorageStrategyFactory.getInstant(metaClient);
    this.cleanParallelism = config.getCleanerParallelism();
  }

  // partitionPath -> List<CleanFileInfo>
  // TODO zhangyue143 only support KEEP_LATEST_COMMITS for now
  public Map<String, List<HoodieCleanFileInfo>> getDeletePaths() throws IOException {
    HoodieCleaningPolicy policy = config.getCleanerPolicy();
    Map<String, List<HoodieCleanFileInfo>> deletePaths;
    if (policy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS) {
      deletePaths = getFilesToCleanKeepingLatestCommits();
    } else {
      throw new IllegalArgumentException("Unknown cleaning policy : " + policy.name());
    }
    return deletePaths;
  }

  // TODO zhangyue143 take care of savepoint
  private Map<String, List<HoodieCleanFileInfo>> getFilesToCleanKeepingLatestCommits() throws IOException {
    int commitsRetained = config.getCleanerCommitsRetained();
    LOG.info("Retaining latest " + commitsRetained + " commits.");
    Map<String, List<HoodieCleanFileInfo>> maps = new HashMap<>();
    if (commitTimeline.countInstants() > commitsRetained) {
      // List<Pair<instant, List<partition, List<files>>>>
      List<Pair<HoodieInstant, List<Pair<String, List<String>>>>> replacedGroups = getFileGroupsReplaced(metaClient.getActiveTimeline());
      replacedGroups.stream().flatMap(ele -> ele.getRight().stream()).forEach(ele -> {
        String partitionPath = ele.getLeft();
        List<HoodieCleanFileInfo> infos = ele.getRight().stream().filter(path -> {
          String key = partitionPath + (new Path(path)).getName();
          return !pendingReplaceFiles.contains(key);
        }).map(path -> {
          return (new CleanFileInfo(path, false)).toHoodieFileCleanInfo();
        }).collect(Collectors.toList());
        if (maps.containsKey(partitionPath)) {
          maps.get(partitionPath).addAll(infos);
        } else {
          maps.put(partitionPath, infos);
        }
      });
    }
    return maps;
  }

  // List<Pair<instant, List<Pair<partition, List<files>>>>>
  private List<Pair<HoodieInstant, List<Pair<String, List<String>>>>> getFileGroupsReplaced(HoodieTimeline timeline) throws IOException {
    Option<HoodieInstant> earliestCommitToRetainOption = getEarliestCommitToRetain();
    if (!earliestCommitToRetainOption.isPresent()) {
      return new ArrayList<>();
    }

    // for each REPLACE instant, get map of (partitionPath -> deleteFileGroup)
    HoodieTimeline replacedTimeline = timeline.getCompletedReplaceTimeline();
    List<String> backtrackInstances = metaClient.getBacktrackInstances();
    List<HoodieInstant> replaceCommitInstantToClean = getReplaceInstantsToClean(replacedTimeline, earliestCommitToRetainOption.get(), backtrackInstances);

    if (replaceCommitInstantToClean.size() == 0) {
      return new ArrayList<>();
    }

    List<Pair<HoodieInstant, HoodieReplaceCommitMetadata>> metas = replaceCommitInstantToClean.stream().map(instant -> {
      HoodieReplaceCommitMetadata replaceMetadata = null;
      try {
        replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(metaClient.getActiveTimeline().getInstantDetails(instant).get(),
            HoodieReplaceCommitMetadata.class);
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
      return Pair.of(instant, replaceMetadata);
    }).collect(Collectors.toList());

    //TODO zhangyue143 clean 与缓冲层适配测试
    HoodieData<Pair<HoodieInstant, List<Pair<String, List<String>>>>> finalGroups = context.parallelize(metas, Math.max(metas.size(), cleanParallelism)).mapPartitions(iterator -> {
      List<Pair<HoodieInstant, List<Pair<String, List<String>>>>> res = new ArrayList<>();
      iterator.forEachRemaining(pair -> {
        HoodieWrapperFileSystem innerFS = metaClient.getFs();
        HoodieInstant instant = pair.getLeft();
        HoodieReplaceCommitMetadata meta = pair.getRight();
        List<Pair<String, List<String>>> collect = meta.getPartitionToReplaceFileIds().entrySet().stream().map(entry -> {
          String partitionPath = entry.getKey();
          List<String> groups = entry.getValue().stream().map(name -> {
            return storageStrategy.getAllLocations(partitionPath + "/" + name, false);
          }).flatMap(Set::stream).filter(path -> {
            // 过滤不存在的文件
            try {
              // getFileStatus 比list调用更轻量，不会对NN造成太大压力
              innerFS.getFileStatus(path);
              return true;
            } catch (FileNotFoundException e) {
              return false;
            } catch (Exception ioe) {
              // 若发生任何其他异常，则直接判定文件是存在的，后续尝试删除（宁错杀不放过）
              return true;
            }
          }).map(Path::toString).collect(Collectors.toList());
          return Pair.of(partitionPath, groups);
        }).collect(Collectors.toList());

        res.add(Pair.of(instant, collect));
      });
      return res.iterator();
    }, true);
    List<Pair<HoodieInstant, List<Pair<String, List<String>>>>> list = new ArrayList<>(finalGroups.collectAsList());
    list.sort((new Comparator<Pair<HoodieInstant, List<Pair<String, List<String>>>>>() {
      @Override
      public int compare(Pair<HoodieInstant, List<Pair<String, List<String>>>> o1, Pair<HoodieInstant, List<Pair<String, List<String>>>> o2) {
        return o1.getLeft().compareTo(o2.getLeft());
      }
    }).reversed());
    return list;
  }

  public List<HoodieInstant> getReplaceInstantsToClean(HoodieTimeline replacedTimeline, HoodieInstant earliestCommitToRetain, List<String> backtrackInstances) throws IOException {
    if (config.incrementalCleanerModeEnabled()) {
      Option<HoodieInstant> lastClean = metaClient.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant();
      if (lastClean.isPresent()) {
        if (metaClient.getActiveTimeline().isEmpty(lastClean.get())) {
          metaClient.getActiveTimeline().deleteEmptyInstantIfExists(lastClean.get());
        } else {
          HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils
              .deserializeHoodieCleanMetadata(metaClient.getActiveTimeline().getInstantDetails(lastClean.get()).get());
          String earliestCommitToRetainFromLastClean = cleanMetadata.getEarliestCommitToRetain();
          if ((earliestCommitToRetainFromLastClean != null)
              && (earliestCommitToRetainFromLastClean.length() > 0)
              && !metaClient.getActiveTimeline().isBeforeTimelineStarts(earliestCommitToRetainFromLastClean)) {
            return replacedTimeline.getReverseOrderedInstants().filter(instant -> {
              return HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, earliestCommitToRetainFromLastClean)
                  && instant.compareTo(earliestCommitToRetain) < 0
                  && !backtrackInstances.contains(instant.getTimestamp());
            }).collect(Collectors.toList());
          }
        }
      }
    }

    return replacedTimeline.getReverseOrderedInstants().filter(instant -> {
      return instant.compareTo(earliestCommitToRetain) < 0 && !backtrackInstances.contains(instant.getTimestamp());
    }).collect(Collectors.toList());
  }
}
