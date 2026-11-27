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
import org.apache.hudi.configuration.FlinkOptions;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Default implementation of HoodieContinuousSplitDiscover.
 */
@Slf4j
public class DefaultHoodieSplitDiscover implements HoodieContinuousSplitDiscover {
  private static final TreeMap<String, long[]> EMPTY_WRITES = new TreeMap<>();

  private final HoodieScanContext scanContext;
  private final IncrementalInputSplits incrementalInputSplits;
  private final Configuration hadoopConf;
  private HoodieTableMetaClient metaClient;

  public DefaultHoodieSplitDiscover(
      HoodieScanContext scanContext) {
    this.scanContext = scanContext;
    this.hadoopConf = HadoopConfigurations.getHadoopConf(scanContext.getConf());
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
    HoodieTableMetaClient client = getOrCreateMetaClient();
    if (client == null) {
      return HoodieContinuousSplitBatch.EMPTY;
    }

    return incrementalInputSplits.inputHoodieSourceSplits(client, lastInstant, scanContext.isCdcEnabled());
  }

  @Override
  public List<String> getUndiscoveredCommits(@Nullable String offset, boolean reloadTimeline) {
    HoodieTableMetaClient client = getOrCreateMetaClient();
    if (client == null || offset == null) {
      return Collections.emptyList();
    }
    if (reloadTimeline) {
      client.reloadActiveTimeline();
    }
    // filter by completion time before applying user skip configs, so skipClustering and
    // skipInsertOverwrite do not read replace-commit metadata for already-consumed commits
    String endCommit = scanContext.getConf().get(FlinkOptions.READ_END_COMMIT);
    HoodieTimeline candidateTimeline = client.getCommitsAndCompactionTimeline()
        .filterCompletedInstants()
        .filter(instant -> {
          String completionTime = instant.getCompletionTime();
          return completionTime != null
              && completionTime.compareTo(offset) > 0
              && (endCommit == null || compareTimestamps(completionTime, LESSER_THAN_OR_EQUALS, endCommit));
        });
    if (candidateTimeline.empty()) {
      return Collections.emptyList();
    }
    HoodieTimeline timeline = IncrementalQueryAnalyzer.filterInstantsAsPerUserConfigs(
        client,
        candidateTimeline,
        scanContext.isSkipCompaction(),
        scanContext.isSkipClustering(),
        scanContext.isSkipInsertOverwrite());
    return timeline.getInstantsAsStream()
        .map(HoodieInstant::getCompletionTime)
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
    HoodieTableMetaClient client = getOrCreateMetaClient();
    if (client == null || splits.isEmpty()) {
      return HoodieContinuousSplitDiscover.super.getLagInfo(splits);
    }
    Map<String, Pair<HoodieInstant, HoodieTimeline>> commits = getCommits(client, splits);
    Map<String, String> completionTimes = new HashMap<>();
    commits.forEach((instantTime, commit) -> completionTimes.put(instantTime, commit.getLeft().getCompletionTime()));
    // the writes of the exact-match commits by file id, then by instant time, so that each split only
    // visits the commits that wrote its file group
    Map<String, TreeMap<String, long[]>> writesByFileId = new HashMap<>();
    Set<String> unknownCommits = new TreeSet<>();
    for (String commit : getExactMatchCommits(splits)) {
      Pair<HoodieInstant, HoodieTimeline> instant = commits.get(commit);
      if (instant == null) {
        unknownCommits.add(commit);
        continue;
      }
      getWritesByFileId(instant.getLeft(), instant.getRight()).forEach(
          (fileId, write) -> writesByFileId.computeIfAbsent(fileId, k -> new TreeMap<>()).put(commit, write));
    }
    Map<String, HoodieSourceSplitLagInfo> lagInfos = new HashMap<>();
    for (HoodieSourceSplit split : splits) {
      lagInfos.put(split.splitId(), getLagInfo(split, completionTimes, writesByFileId, unknownCommits));
    }
    return lagInfos;
  }

  /**
   * Returns the lag info of the splits from the commits of their ranges in the timeline of the last
   * discovery, without reading the commit metadata.
   */
  @Override
  public Map<String, HoodieSourceSplitLagInfo> getRangeLagInfo(Collection<HoodieSourceSplit> splits) {
    HoodieTableMetaClient client = getOrCreateMetaClient();
    if (client == null || splits.isEmpty()) {
      return HoodieContinuousSplitDiscover.super.getRangeLagInfo(splits);
    }
    Set<String> instantTimes = getRangeCommits(splits);
    Map<String, String> completionTimes = new HashMap<>();
    client.getCommitsTimeline().filterCompletedInstants().getInstantsAsStream()
        .filter(instant -> instantTimes.contains(instant.requestedTime()))
        .forEach(instant -> completionTimes.put(instant.requestedTime(), instant.getCompletionTime()));
    Map<Collection<String>, HoodieSourceSplitLagInfo> lagInfoByRange = new IdentityHashMap<>();
    Map<String, HoodieSourceSplitLagInfo> lagInfos = new HashMap<>(splits.size());
    for (HoodieSourceSplit split : splits) {
      Collection<String> rangeCommits = HoodieSourceSplitLagInfo.getRangeCommits(split);
      HoodieSourceSplitLagInfo lagInfo = lagInfoByRange.computeIfAbsent(
          rangeCommits,
          commits -> new HoodieSourceSplitLagInfo(commits, getOldestCompletionTime(commits, completionTimes), 0L, 0L));
      lagInfos.put(split.splitId(), lagInfo);
    }
    return lagInfos;
  }

  @Override
  public ZoneId getTimelineZone() {
    HoodieTableMetaClient client = getOrCreateMetaClient();
    return client == null ? HoodieContinuousSplitDiscover.super.getTimelineZone() : client.getTableConfig().getTimelineTimezone().getZoneId();
  }

  private static HoodieSourceSplitLagInfo getLagInfo(
      HoodieSourceSplit split,
      Map<String, String> completionTimes,
      Map<String, TreeMap<String, long[]>> writesByFileId,
      Set<String> unknownCommits) {
    Collection<String> rangeCommits = HoodieSourceSplitLagInfo.getRangeCommits(split);
    List<String> readCommits = new ArrayList<>();
    long records = 0L;
    long bytes = 0L;
    if (isExactMatch(split)) {
      Set<String> range = ((InstantRange.ExactMatchRange) split.getInstantRange().get()).getInstants();
      TreeSet<String> commits = new TreeSet<>();
      // a commit whose metadata is unknown may have written the file group
      unknownCommits.stream().filter(range::contains).forEach(commits::add);
      Map<String, long[]> writes = writesByFileId.getOrDefault(split.getFileId(), EMPTY_WRITES);
      for (Map.Entry<String, long[]> write : writes.entrySet()) {
        if (range.contains(write.getKey())) {
          commits.add(write.getKey());
          records += write.getValue()[0];
          bytes = write.getValue()[1];
        }
      }
      readCommits.addAll(commits);
    }
    Collection<String> lagCommits = readCommits.isEmpty() ? rangeCommits : readCommits;
    return new HoodieSourceSplitLagInfo(lagCommits, getOldestCompletionTime(lagCommits, completionTimes), records, bytes);
  }

  @Nullable
  private static String getOldestCompletionTime(Collection<String> commits, Map<String, String> completionTimes) {
    return commits.stream()
        .map(completionTimes::get)
        .filter(Objects::nonNull)
        .min(String::compareTo)
        .orElse(null);
  }

  private static Set<String> getRangeCommits(Collection<HoodieSourceSplit> splits) {
    Set<Collection<String>> seenRanges = Collections.newSetFromMap(new IdentityHashMap<>());
    Set<String> commits = new HashSet<>();
    for (HoodieSourceSplit split : splits) {
      Collection<String> rangeCommits = HoodieSourceSplitLagInfo.getRangeCommits(split);
      if (seenRanges.add(rangeCommits)) {
        commits.addAll(rangeCommits);
      }
    }
    return commits;
  }

  /**
   * Returns the completed commits of the splits keyed by instant time, with the timeline to read their
   * metadata from, looking up the archived timeline for the commits that are not active anymore.
   */
  private Map<String, Pair<HoodieInstant, HoodieTimeline>> getCommits(
      HoodieTableMetaClient client, Collection<HoodieSourceSplit> splits) {
    Set<String> instantTimes = getRangeCommits(splits);
    Map<String, Pair<HoodieInstant, HoodieTimeline>> commits = new HashMap<>();
    HoodieTimeline activeTimeline = client.getCommitsTimeline().filterCompletedInstants();
    activeTimeline.getInstantsAsStream()
        .filter(instant -> instantTimes.contains(instant.requestedTime()))
        .forEach(instant -> commits.put(instant.requestedTime(), Pair.of(instant, activeTimeline)));
    instantTimes.stream().filter(instantTime -> !commits.containsKey(instantTime)).min(String::compareTo).ifPresent(oldest -> {
      HoodieTimeline archivedTimeline = client.getArchivedTimeline(oldest, false).getCommitsTimeline().filterCompletedInstants();
      archivedTimeline.getInstantsAsStream()
          .filter(instant -> instantTimes.contains(instant.requestedTime()))
          .forEach(instant -> commits.putIfAbsent(instant.requestedTime(), Pair.of(instant, archivedTimeline)));
    });
    return commits;
  }

  private static Set<String> getExactMatchCommits(Collection<HoodieSourceSplit> splits) {
    Set<Collection<String>> seenRanges = Collections.newSetFromMap(new IdentityHashMap<>());
    Set<String> commits = new HashSet<>();
    for (HoodieSourceSplit split : splits) {
      if (isExactMatch(split)) {
        Collection<String> rangeCommits = HoodieSourceSplitLagInfo.getRangeCommits(split);
        if (seenRanges.add(rangeCommits)) {
          commits.addAll(rangeCommits);
        }
      }
    }
    return commits;
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
    if (metaClient == null && StreamerUtil.tableExists(scanContext.getPath().toString(), hadoopConf)) {
      metaClient = StreamerUtil.createMetaClient(scanContext.getPath().toString(), hadoopConf);
    }
    return metaClient;
  }
}
