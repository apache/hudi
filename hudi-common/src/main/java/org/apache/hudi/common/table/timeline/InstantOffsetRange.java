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

package org.apache.hudi.common.table.timeline;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InstantOffsetRange {

  public static class InstantOffset {

    private static final int DEFAULT_INDEX = Integer.MAX_VALUE;

    /**
     * Construct {@link InstantOffset} from string value.
     *
     * <p>
     * Note that the param must obey the format `instantTime` or `instantTime_partitionPathIdx_fileIdIdx`.
     * `partitionPathIdx` is calculated by the natural order of PartitionToWriteStats of that instant,
     * `fileIdIdx` is directly the index of the writeStat.
     */
    public static InstantOffset fromString(String instantOffset) {
      String[] values = instantOffset.trim().split("_");
      try {
        Long.parseLong(values[0]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("The instantOffset must contains instant time at first, but got "
            + values[0]);
      }

      String instantTime = values[0];
      int partitionPathIdx = values.length > 1 ? Integer.parseInt(values[1]) : DEFAULT_INDEX;
      int fileIdIdx = values.length > 1 ? Integer.parseInt(values[2]) : DEFAULT_INDEX;
      return new InstantOffset(instantTime, partitionPathIdx, fileIdIdx);
    }

    public final String instantTime;
    public final int partitionPathIdx;
    public final int fileIdIdx;

    public InstantOffset(String instantTime) {
      this(instantTime, DEFAULT_INDEX, DEFAULT_INDEX);
    }

    public InstantOffset(String instantTime, int partitionPathIdx, int fileIdIdx) {
      this.instantTime = instantTime;
      this.partitionPathIdx = partitionPathIdx;
      this.fileIdIdx = fileIdIdx;
    }

    @Override
    public String toString() {
      if (partitionPathIdx == DEFAULT_INDEX) {
        return instantTime;
      }

      return instantTime + "_" + partitionPathIdx + "_" + fileIdIdx;
    }

    @Override
    public int hashCode() {
      return this.toString().hashCode();
    }

    /**
     * Check if the offset is great than {@link other}
     *
     * Note that the offset with only instant time is larger than the offset
     * with same instant time but has partitionPathIdx.
     */
    public boolean gt(InstantOffset other) {
      boolean isTimeStampGtOther = HoodieTimeline
          .compareTimestamps(this.instantTime, HoodieTimeline.GREATER_THAN, other.instantTime);

      if (this.instantTimeOnly() && other.instantTimeOnly()) {
        // We only need to compare the commit time here
        return isTimeStampGtOther;
      }

      if (isTimeStampGtOther) {
        return true;
      }

      if (this.instantTime.equals(other.instantTime)) {
        if (this.partitionPathIdx > other.partitionPathIdx) {
          return true;
        } else if (this.partitionPathIdx == other.partitionPathIdx) {
          return this.fileIdIdx > other.fileIdIdx;
        }
      }

      return false;
    }

    public boolean instantTimeOnly() {
      return partitionPathIdx == DEFAULT_INDEX;
    }
  }

  private static final Cache<InstantOffsetRange, List<HoodieWriteStat>>
      OFFSET_RANGE_WRITE_STATS_CACHE = Caffeine.newBuilder().maximumSize(10).weakValues().build();

  private final int maxFilesLimit;
  private final int maxRowsLimit;
  private final boolean useCompletionTime;
  private final InstantOffset startOffset;
  private final HoodieTimeline timeline;

  /**
   * can either be set by the user manually by the method setEndOffset()
   * or getAvailableEndOffset()
   */
  private InstantOffset endOffset;

  private InstantOffsetRange(int maxFilesLimit, int maxRowsLimit, boolean useCompletionTime,
                             InstantOffset startOffset, HoodieTimeline timeline) {
    this.maxFilesLimit = maxFilesLimit;
    this.maxRowsLimit = maxRowsLimit;
    this.useCompletionTime = useCompletionTime;
    this.startOffset = startOffset;
    this.timeline = timeline;
  }

  private InstantOffsetRange(InstantOffset startOffset, InstantOffset endOffset,
                             boolean useCompletionTime, HoodieTimeline timeline) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.useCompletionTime = useCompletionTime;
    this.timeline = timeline;
    this.maxFilesLimit = Integer.MAX_VALUE;
    this.maxRowsLimit = Integer.MAX_VALUE;
  }

  /**
   * We have to ignore Clustering & compact commits since it rewrites existing commits
   * which might already be read partially before. We could meet duplicates if read it.
   */
  private Option<HoodieCommitMetadata> getAcquiredMetadataOrEmpty(HoodieInstant instant) throws HoodieIOException {
    try {
      HoodieCommitMetadata commitMetadata = TimelineUtils.getCommitMetadata(instant, timeline);
      WriteOperationType operationType = commitMetadata.getOperationType();
      if (operationType == WriteOperationType.CLUSTER
          || operationType == WriteOperationType.COMPACT
          || operationType == WriteOperationType.LOG_COMPACT) {
        return Option.empty();
      }
      return Option.of(commitMetadata);
    } catch (IOException e) {
      throw new HoodieIOException("caught exception when extract commit metadata", e);
    }
  }

  /**
   * Return relative file paths between this range.
   */
  public List<HoodieWriteStat> getWriteStatsBetween() throws HoodieIOException {
    ValidationUtils.checkArgument(endOffset != null,
        "The endOffset must be set when fetching files between");

    List<HoodieWriteStat> writeStats = OFFSET_RANGE_WRITE_STATS_CACHE.getIfPresent(this);
    if (writeStats == null) {
      if (startOffset.instantTimeOnly() && endOffset.instantTimeOnly()) {
        return timeline.findInstantsInRange(startOffset.instantTime, endOffset.instantTime)
            .getInstantsAsStream().map(this::getAcquiredMetadataOrEmpty)
            .filter(Option::isPresent).map(Option::get)
            .flatMap(commitMetadata -> commitMetadata.getPartitionToWriteStats().values().stream()
                .flatMap(List::stream))
            .collect(Collectors.toList());
      }

      Stream<HoodieInstant> instantsStream;
      // We have to use close range here in case startOffset is in middle
      if (useCompletionTime) {
        instantsStream = timeline
            .findInstantsInClosedRangeByCompletionTime(startOffset.instantTime, endOffset.instantTime)
            .getInstantsOrderedByCompletionTime();
      } else {
        instantsStream = timeline
            .findInstantsInClosedRange(startOffset.instantTime, endOffset.instantTime)
            .getInstantsAsStream();
      }
      writeStats = availableEndOffsetWithFiles(instantsStream).getRight();
    } else {
      // Eagerly invalidate this since it's usually used only once.
      OFFSET_RANGE_WRITE_STATS_CACHE.invalidate(this);
    }

    return writeStats;
  }

  /**
   * Return the endOffset by complying specified `maxFilesLimit` and `maxRowsLimit`
   *
   * <p>
   * Note that this method will also save chosen files to `OFFSET_RANGE_FILES_CACHE`
   * in case it will be calculated again in `getFilesBetween`.
   */
  public InstantOffset getEndOffset() throws HoodieIOException {
    if (endOffset != null) {
      return endOffset;
    }

    Stream<HoodieInstant> instantsStream;
    if (useCompletionTime) {
      instantsStream = timeline
          .findInstantsAfterOrEqualsCompletionTime(startOffset.instantTime, Integer.MAX_VALUE)
          .getInstantsAsStream();
    } else {
      instantsStream = timeline.filterCompletedInstants()
          .findInstantsAfterOrEquals(startOffset.instantTime, Integer.MAX_VALUE)
          .getInstantsAsStream();
    }

    if (maxFilesLimit == Integer.MAX_VALUE && maxRowsLimit == Integer.MAX_VALUE) {
      // Return the max available commits directly
      endOffset = instantsStream
          .reduce((first, second) -> second)
          .map(instant -> new InstantOffset(instant.getTimestamp()))
          .orElse(startOffset);
    } else {
      Pair<InstantOffset, List<HoodieWriteStat>> endOffsetWithFiles = availableEndOffsetWithFiles(instantsStream);
      endOffset = endOffsetWithFiles.getKey();
      OFFSET_RANGE_WRITE_STATS_CACHE.put(this, endOffsetWithFiles.getValue());
    }

    return endOffset;
  }

  /**
   * Iterator instantsStream from `startOffset`(exclusive) to return
   * the max end instant offset and chosen files
   * while respecting `maxFilesLimit` and `maxRowsLimit`.
   */
  private Pair<InstantOffset, List<HoodieWriteStat>> availableEndOffsetWithFiles(Stream<HoodieInstant> instantsStream)
      throws HoodieIOException {
    boolean findStartOffset = false;
    int totalFilesCount = 0;
    int totalRowsCount = 0;
    ArrayList<HoodieWriteStat> chosenStats = new ArrayList<>();

    for (HoodieInstant instant : instantsStream.collect(Collectors.toList())) {
      Option<HoodieCommitMetadata> metadataOption = getAcquiredMetadataOrEmpty(instant);
      if (!metadataOption.isPresent()) {
        continue;
      }

      HoodieCommitMetadata commitMetadata = metadataOption.get();
      TreeMap<String, List<HoodieWriteStat>> sortedWriteStats = new TreeMap<>(commitMetadata.getPartitionToWriteStats());
      int currentPartitionPathIdx = 0;
      for (String partitionPath : sortedWriteStats.descendingKeySet()) {
        int currentFileIdx = 0;
        for (HoodieWriteStat writeStat : commitMetadata.getWriteStats(partitionPath)) {
          InstantOffset currentOffset = new InstantOffset(instant.getTimestamp(), currentPartitionPathIdx, currentFileIdx);
          if (!findStartOffset) {
            if (!currentOffset.gt(startOffset)) {
              continue;
            }
            findStartOffset = true;
          }
          totalFilesCount += 1;
          totalRowsCount += (int) (writeStat.getNumInserts() + writeStat.getNumUpdateWrites() + writeStat.getNumDeletes());
          chosenStats.add(writeStat);
          if (totalFilesCount >= maxFilesLimit || totalRowsCount >= maxRowsLimit
              || (endOffset != null && !endOffset.gt(currentOffset))) {
            return Pair.of(new InstantOffset(instant.getTimestamp(), currentPartitionPathIdx, currentFileIdx), chosenStats);
          }
          currentFileIdx++;
        }
        currentPartitionPathIdx++;
      }
    }

    // Return startInstantOffset if no more commits is read
    return Pair.of(startOffset, chosenStats);
  }

  @Override
  public String toString() {
    return "timelineHash: " + timeline.getTimelineHash() + "["
        + startOffset.toString() + "~"
        + (endOffset == null ? "undefined, maxRow: " + maxRowsLimit + ", maxFiles: " + maxFilesLimit
        : endOffset.toString())
        + ", completionTime: " + useCompletionTime + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InstantOffsetRange) {
      return this.hashCode() == obj.hashCode();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  public static InstantOffsetRange.Builder newBuilder() {
    return new InstantOffsetRange.Builder();
  }

  public static class Builder {

    private int maxFilesLimit = Integer.MAX_VALUE;
    private int maxRowsLimit = Integer.MAX_VALUE;
    private boolean useCompletionTime = false;

    public Builder withMaxFiles(int maxFilesLimit) {
      this.maxFilesLimit = maxFilesLimit;
      return this;
    }

    public Builder withMaxRows(int maxRowsLimit) {
      this.maxRowsLimit = maxRowsLimit;
      return this;
    }

    public Builder withCompletionTimeEnabled(boolean useCompletionTime) {
      this.useCompletionTime = useCompletionTime;
      return this;
    }

    public InstantOffsetRange build(InstantOffset startOffset, HoodieTimeline timeline) {
      return new InstantOffsetRange(maxFilesLimit, maxRowsLimit, useCompletionTime, startOffset, timeline);
    }

    public InstantOffsetRange build(InstantOffset startOffset, InstantOffset endOffset, HoodieTimeline timeline) {
      return new InstantOffsetRange(startOffset, endOffset, useCompletionTime, timeline);
    }
  }
}
