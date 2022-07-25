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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieMergeArchiveFilePlan;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/**
 * Represents the Archived Timeline for the Hoodie table. Instants for the last 12 hours (configurable) is in the
 * ActiveTimeline and the rest are in ArchivedTimeline.
 * <p>
 * </p>
 * Instants are read from the archive file during initialization and never refreshed. To refresh, clients need to call
 * reload()
 * <p>
 * </p>
 * This class can be serialized and de-serialized and on de-serialization the FileSystem is re-initialized.
 */
public class HoodieArchivedTimeline extends HoodieDefaultTimeline {
  public static final String MERGE_ARCHIVE_PLAN_NAME = "mergeArchivePlan";
  private static final Pattern ARCHIVE_FILE_PATTERN =
      Pattern.compile("^\\.commits_\\.archive\\.([0-9]+).*");

  private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE_PREFIX = "commits";
  private static final String ACTION_TYPE_KEY = "actionType";
  private static final String ACTION_STATE = "actionState";
  private HoodieTableMetaClient metaClient;
  private final Map<String, byte[]> readCommits = new HashMap<>();

  private static final Logger LOG = LogManager.getLogger(HoodieArchivedTimeline.class);

  /**
   * Loads all the archived instants.
   * Note that there is no lazy loading, so this may not work if the archived timeline range is really long.
   * TBD: Should we enforce maximum time range?
   */
  public HoodieArchivedTimeline(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    setInstants(this.loadInstants(false));
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  /**
   * Loads completed instants from startTs(inclusive).
   * Note that there is no lazy loading, so this may not work if really early startTs is specified.
   */
  public HoodieArchivedTimeline(HoodieTableMetaClient metaClient, String startTs) {
    this.metaClient = metaClient;
    setInstants(loadInstants(new StartTsFilter(startTs), true,
        record -> HoodieInstant.State.COMPLETED.toString().equals(record.get(ACTION_STATE).toString())));
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public HoodieArchivedTimeline() {
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  public static Path getArchiveLogPath(String archiveFolder) {
    return new Path(archiveFolder, HOODIE_COMMIT_ARCHIVE_LOG_FILE_PREFIX);
  }C

  public void loadInstantDetailsInMemory(String startTs, String endTs) {
    loadInstants(startTs, endTs);
  }

  public void loadCompletedInstantDetailsInMemory() {
    loadInstants(null, true,
        record -> HoodieInstant.State.COMPLETED.toString().equals(record.get(ACTION_STATE).toString()));
  }

  public void loadCompactionDetailsInMemory(String compactionInstantTime) {
    loadCompactionDetailsInMemory(compactionInstantTime, compactionInstantTime);
  }

  public void loadCompactionDetailsInMemory(String startTs, String endTs) {
    // load compactionPlan
    loadInstants(new TimeRangeFilter(startTs, endTs), true, record ->
        record.get(ACTION_TYPE_KEY).toString().equals(HoodieTimeline.COMPACTION_ACTION)
            && HoodieInstant.State.INFLIGHT.toString().equals(record.get(ACTION_STATE).toString())
    );
  }

  public void clearInstantDetailsFromMemory(String instantTime) {
    this.readCommits.remove(instantTime);
  }

  public void clearInstantDetailsFromMemory(String startTs, String endTs) {
    this.findInstantsInRange(startTs, endTs).getInstants().forEach(instant ->
            this.readCommits.remove(instant.getTimestamp()));
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return Option.ofNullable(readCommits.get(instant.getTimestamp()));
  }

  public HoodieArchivedTimeline reload() {
    return new HoodieArchivedTimeline(metaClient);
  }

  private HoodieInstant readCommit(GenericRecord record, boolean loadDetails) {
    final String instantTime = record.get(HoodiePartitionMetadata.COMMIT_TIME_KEY).toString();
    final String action = record.get(ACTION_TYPE_KEY).toString();
    if (loadDetails) {
      getMetadataKey(action).map(key -> {
        Object actionData = record.get(key);
        if (actionData != null) {
          if (action.equals(HoodieTimeline.COMPACTION_ACTION)) {
            this.readCommits.put(instantTime, HoodieAvroUtils.indexedRecordToBytes((IndexedRecord) actionData));
          } else {
            this.readCommits.put(instantTime, actionData.toString().getBytes(StandardCharsets.UTF_8));
          }
        }
        return null;
      });
    }
    return new HoodieInstant(HoodieInstant.State.valueOf(record.get(ACTION_STATE).toString()), action, instantTime);
  }

  @Nonnull
  private Option<String> getMetadataKey(String action) {
    switch (action) {
      case HoodieTimeline.CLEAN_ACTION:
        return Option.of("hoodieCleanMetadata");
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        return Option.of("hoodieCommitMetadata");
      case HoodieTimeline.ROLLBACK_ACTION:
        return Option.of("hoodieRollbackMetadata");
      case HoodieTimeline.SAVEPOINT_ACTION:
        return Option.of("hoodieSavePointMetadata");
      case HoodieTimeline.COMPACTION_ACTION:
        return Option.of("hoodieCompactionPlan");
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        return Option.of("hoodieReplaceCommitMetadata");
      case HoodieTimeline.INDEXING_ACTION:
        return Option.of("hoodieIndexCommitMetadata");
      default:
        LOG.error(String.format("Unknown action in metadata (%s)", action));
        return Option.empty();
    }
  }

  private List<HoodieInstant> loadInstants(boolean loadInstantDetails) {
    return loadInstants(null, loadInstantDetails);
  }

  private List<HoodieInstant> loadInstants(String startTs, String endTs) {
    return loadInstants(new TimeRangeFilter(startTs, endTs), true);
  }

  private List<HoodieInstant> loadInstants(TimeRangeFilter filter, boolean loadInstantDetails) {
    return loadInstants(filter, loadInstantDetails, record -> true);
  }

  /**
   * This is method to read selected instants. Do NOT use this directly use one of the helper methods above
   * If loadInstantDetails is set to true, this would also update 'readCommits' map with commit details
   * If filter is specified, only the filtered instants are loaded
   * If commitsFilter is specified, only the filtered records are loaded
   */
  private List<HoodieInstant> loadInstants(TimeRangeFilter filter, boolean loadInstantDetails,
       Function<GenericRecord, Boolean> commitsFilter) {
    try {
      // List all files
      FileStatus[] fsStatuses = metaClient.getFs().globStatus(
              new Path(metaClient.getArchivePath() + "/.commits_.archive*"));

      // Sort files by version suffix in reverse (implies reverse chronological order)
      Arrays.sort(fsStatuses, new ArchiveFileVersionComparator());

      Set<HoodieInstant> instantsInRange = new HashSet<>();
      for (FileStatus fs : fsStatuses) {
        // Read the archived file
        try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient.getFs(),
            new HoodieLogFile(fs.getPath()), HoodieArchivedMetaEntry.getClassSchema())) {
          int instantsInPreviousFile = instantsInRange.size();
          // Read the avro blocks
          while (reader.hasNext()) {
            HoodieLogBlock block = reader.next();
            if (block instanceof HoodieAvroDataBlock) {
              HoodieAvroDataBlock avroBlock = (HoodieAvroDataBlock) block;
              // TODO If we can store additional metadata in datablock, we can skip parsing records
              // (such as startTime, endTime of records in the block)
              try (ClosableIterator<IndexedRecord> itr = avroBlock.getRecordIterator()) {
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(itr, Spliterator.IMMUTABLE), true)
                    // Filter blocks in desired time window
                    .filter(r -> commitsFilter.apply((GenericRecord) r))
                    .map(r -> readCommit((GenericRecord) r, loadInstantDetails))
                    .filter(c -> filter == null || filter.isInRange(c))
                    .forEach(instantsInRange::add);
              }
            }
          }

          if (filter != null) {
            int instantsInCurrentFile = instantsInRange.size() - instantsInPreviousFile;
            if (instantsInPreviousFile > 0 && instantsInCurrentFile == 0) {
              // Note that this is an optimization to skip reading unnecessary archived files
              // This signals we crossed lower bound of desired time window.
              break;
            }
          }
        } catch (Exception originalException) {
          // merge small archive files may left uncompleted archive file which will cause exception.
          // need to ignore this kind of exception here.
          try {
            Path planPath = new Path(metaClient.getArchivePath(), MERGE_ARCHIVE_PLAN_NAME);
            HoodieWrapperFileSystem fileSystem = metaClient.getFs();
            if (fileSystem.exists(planPath)) {
              HoodieMergeArchiveFilePlan plan = TimelineMetadataUtils.deserializeAvroMetadata(FileIOUtils.readDataFromPath(fileSystem, planPath).get(), HoodieMergeArchiveFilePlan.class);
              String mergedArchiveFileName = plan.getMergedArchiveFileName();
              if (!StringUtils.isNullOrEmpty(mergedArchiveFileName) && fs.getPath().getName().equalsIgnoreCase(mergedArchiveFileName)) {
                LOG.warn("Catch exception because of reading uncompleted merging archive file " + mergedArchiveFileName + ". Ignore it here.");
                continue;
              }
            }
            throw originalException;
          } catch (Exception e) {
            // If anything wrong during parsing merge archive plan, we need to throw the original exception.
            // For example corrupted archive file and corrupted plan are both existed.
            throw originalException;
          }
        }
      }

      ArrayList<HoodieInstant> result = new ArrayList<>(instantsInRange);
      Collections.sort(result);
      return result;
    } catch (IOException e) {
      throw new HoodieIOException(
              "Could not load archived commit timeline from path " + metaClient.getArchivePath(), e);
    }
  }

  private static class TimeRangeFilter {
    private final String startTs;
    private final String endTs;

    public TimeRangeFilter(String startTs, String endTs) {
      this.startTs = startTs;
      this.endTs = endTs;
    }

    public boolean isInRange(HoodieInstant instant) {
      return HoodieTimeline.isInRange(instant.getTimestamp(), this.startTs, this.endTs);
    }
  }

  private static class StartTsFilter extends TimeRangeFilter {
    private final String startTs;

    public StartTsFilter(String startTs) {
      super(startTs, null); // endTs is never used
      this.startTs = startTs;
    }

    public boolean isInRange(HoodieInstant instant) {
      return HoodieTimeline.compareTimestamps(instant.getTimestamp(), GREATER_THAN_OR_EQUALS, startTs);
    }
  }

  /**
   * Sort files by reverse order of version suffix in file name.
   */
  public static class ArchiveFileVersionComparator implements Comparator<FileStatus>, Serializable {
    @Override
    public int compare(FileStatus f1, FileStatus f2) {
      return Integer.compare(getArchivedFileSuffix(f2), getArchivedFileSuffix(f1));
    }

    private int getArchivedFileSuffix(FileStatus f) {
      try {
        Matcher fileMatcher = ARCHIVE_FILE_PATTERN.matcher(f.getPath().getName());
        if (fileMatcher.matches()) {
          return Integer.parseInt(fileMatcher.group(1));
        }
      } catch (NumberFormatException e) {
        // log and ignore any format warnings
        LOG.warn("error getting suffix for archived file: " + f.getPath());
      }

      // return default value in case of any errors
      return 0;
    }
  }

  @Override
  public HoodieDefaultTimeline getWriteTimeline() {
    // filter in-memory instants
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return new HoodieDefaultTimeline(getInstants().filter(i ->
        readCommits.containsKey(i.getTimestamp()))
        .filter(s -> validActions.contains(s.getAction())), details);
  }
}
