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

package org.apache.hudi.source.split;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Default implementation of HoodieContinuousSplitDiscover.
 */
@Slf4j
public class DefaultHoodieSplitDiscover implements HoodieContinuousSplitDiscover {
  private final HoodieScanContext scanContext;
  private final IncrementalInputSplits incrementalInputSplits;
  private final Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  public DefaultHoodieSplitDiscover(
      HoodieScanContext scanContext) {
    this.scanContext = scanContext;
    this.hadoopConf = HadoopConfigurations.getHadoopConf(scanContext.getConf());
    this.metaClient = getOrCreateMetaClient();
    this.incrementalInputSplits = IncrementalInputSplits.builder()
        .conf(scanContext.getConf())
        .path(new Path(scanContext.getPath().toUri()))
        .rowType(scanContext.getRowType())
        .maxCompactionMemoryInBytes(scanContext.getMaxCompactionMemoryInBytes())
        .skipCompaction(scanContext.isSkipCompaction())
        .skipClustering(scanContext.isSkipClustering())
        .skipInsertOverwrite(scanContext.isSkipInsertOverwrite())
        .partitionPruner(scanContext.getPartitionPruner()).build();
  }

  @Override
  public HoodieContinuousSplitBatch discoverSplits(String lastInstant) {
    if (metaClient == null) {
      return HoodieContinuousSplitBatch.EMPTY;
    }

    return incrementalInputSplits.inputHoodieSourceSplits(metaClient, lastInstant, scanContext.isCdcEnabled());
  }

  @Override
  public List<String> getUndiscoveredCommits(@Nullable String offset, boolean reloadTimeline) {
    if (metaClient == null || offset == null) {
      return Collections.emptyList();
    }
    if (reloadTimeline) {
      metaClient.reloadActiveTimeline();
    }
    HoodieTimeline timeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(
        metaClient,
        metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants(),
        scanContext.isSkipCompaction(),
        scanContext.isSkipClustering(),
        scanContext.isSkipInsertOverwrite());
    return timeline.getInstantsAsStream()
        .map(HoodieInstant::getCompletionTime)
        .filter(completionTime -> completionTime != null && completionTime.compareTo(offset) > 0)
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * Returns the lag info of the splits from the write stats of the commits in their exact-match ranges.
   * A copy-on-write or append-only split only emits the records of the commits that wrote its file
   * group, so those are its read commits. Its records are their inserts and updates, which counts a
   * record written by several of them more than once, and its bytes are the size of the latest file
   * written. Splits without an exact-match range read their latest commit, with unknown estimates.
   */
  @Override
  public Map<String, HoodieSourceSplitLagInfo> getLagInfo(Collection<HoodieSourceSplit> splits) {
    if (metaClient == null || splits.isEmpty()) {
      return HoodieContinuousSplitDiscover.super.getLagInfo(splits);
    }
    Map<String, Pair<HoodieInstant, HoodieTimeline>> commits = getCommits(splits);
    Map<String, Map<String, long[]>> writesByCommit = new HashMap<>();
    for (String commit : getExactMatchCommits(splits)) {
      if (commits.containsKey(commit)) {
        writesByCommit.put(commit, getWritesByFileId(commits.get(commit).getLeft(), commits.get(commit).getRight()));
      }
    }
    Map<String, HoodieSourceSplitLagInfo> lagInfos = new HashMap<>();
    for (HoodieSourceSplit split : splits) {
      lagInfos.put(split.splitId(), getLagInfo(split, commits, writesByCommit));
    }
    return lagInfos;
  }

  private static HoodieSourceSplitLagInfo getLagInfo(
      HoodieSourceSplit split,
      Map<String, Pair<HoodieInstant, HoodieTimeline>> commits,
      Map<String, Map<String, long[]>> writesByCommit) {
    Collection<String> rangeCommits = HoodieSourceSplitLagInfo.getRangeCommits(split);
    List<String> readCommits = new ArrayList<>();
    long records = 0L;
    long bytes = 0L;
    if (isExactMatch(split)) {
      for (String commit : new TreeSet<>(rangeCommits)) {
        Map<String, long[]> writes = writesByCommit.get(commit);
        long[] write = writes == null ? null : writes.get(split.getFileId());
        if (writes == null || write != null) {
          // a commit whose metadata is unknown may have written the file group
          readCommits.add(commit);
        }
        if (write != null) {
          records += write[0];
          bytes = write[1];
        }
      }
    }
    Collection<String> lagCommits = readCommits.isEmpty() ? rangeCommits : readCommits;
    String oldestCompletionTime = lagCommits.stream()
        .map(commits::get)
        .filter(Objects::nonNull)
        .map(commit -> commit.getLeft().getCompletionTime())
        .filter(Objects::nonNull)
        .min(String::compareTo)
        .orElse(null);
    return new HoodieSourceSplitLagInfo(lagCommits, oldestCompletionTime, records, bytes);
  }

  /**
   * Returns the completed commits of the splits keyed by instant time, with the timeline to read their
   * metadata from, looking up the archived timeline for the commits that are not active anymore.
   */
  private Map<String, Pair<HoodieInstant, HoodieTimeline>> getCommits(Collection<HoodieSourceSplit> splits) {
    Set<String> instantTimes = splits.stream()
        .flatMap(split -> HoodieSourceSplitLagInfo.getRangeCommits(split).stream())
        .collect(Collectors.toSet());
    Map<String, Pair<HoodieInstant, HoodieTimeline>> commits = new HashMap<>();
    HoodieTimeline activeTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
    activeTimeline.getInstantsAsStream()
        .filter(instant -> instantTimes.contains(instant.requestedTime()))
        .forEach(instant -> commits.put(instant.requestedTime(), Pair.of(instant, activeTimeline)));
    instantTimes.stream().filter(instantTime -> !commits.containsKey(instantTime)).min(String::compareTo).ifPresent(oldest -> {
      HoodieTimeline archivedTimeline = metaClient.getArchivedTimeline(oldest, false).getCommitsTimeline().filterCompletedInstants();
      archivedTimeline.getInstantsAsStream()
          .filter(instant -> instantTimes.contains(instant.requestedTime()))
          .forEach(instant -> commits.putIfAbsent(instant.requestedTime(), Pair.of(instant, archivedTimeline)));
    });
    return commits;
  }

  private static Set<String> getExactMatchCommits(Collection<HoodieSourceSplit> splits) {
    return splits.stream()
        .filter(DefaultHoodieSplitDiscover::isExactMatch)
        .flatMap(split -> HoodieSourceSplitLagInfo.getRangeCommits(split).stream())
        .collect(Collectors.toSet());
  }

  private static boolean isExactMatch(HoodieSourceSplit split) {
    return split.getInstantRange().isPresent() && split.getInstantRange().get() instanceof InstantRange.ExactMatchRange;
  }

  /**
   * Returns the records written to each file group by the commit, inserts plus updates, and the size of
   * the file written, keyed by file id.
   */
  private Map<String, long[]> getWritesByFileId(HoodieInstant commit, HoodieTimeline timeline) {
    HoodieCommitMetadata metadata;
    try {
      metadata = TimelineUtils.getCommitMetadata(commit, timeline);
    } catch (IOException e) {
      throw new HoodieException("Failed to read the commit metadata of " + commit, e);
    }
    Map<String, long[]> writes = new HashMap<>();
    for (List<HoodieWriteStat> writeStats : metadata.getPartitionToWriteStats().values()) {
      for (HoodieWriteStat writeStat : writeStats) {
        long[] write = writes.computeIfAbsent(writeStat.getFileId(), fileId -> new long[2]);
        write[0] += writeStat.getNumInserts() + writeStat.getNumUpdateWrites();
        write[1] = writeStat.getFileSizeInBytes();
      }
    }
    return writes;
  }

  @Nullable
  private HoodieTableMetaClient getOrCreateMetaClient() {
    if (this.metaClient != null) {
      return this.metaClient;
    }
    if (StreamerUtil.tableExists(this.scanContext.getPath().toString(), hadoopConf)) {
      this.metaClient = StreamerUtil.createMetaClient(this.scanContext.getPath().toString(), hadoopConf);
      return this.metaClient;
    }
    // fallback
    return null;
  }
}
