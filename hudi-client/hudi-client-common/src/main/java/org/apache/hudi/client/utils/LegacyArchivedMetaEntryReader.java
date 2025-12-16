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

package org.apache.hudi.client.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.client.timeline.ActiveActionWithDetails;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.convertMetadataToByteArray;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * Tools used for migrating to new LSM tree style archived timeline.
 */
@Slf4j
public class LegacyArchivedMetaEntryReader {

  private static final Pattern ARCHIVE_FILE_PATTERN =
      Pattern.compile("^\\.commits_\\.archive\\.([0-9]+).*");

  public static final String MERGE_ARCHIVE_PLAN_NAME = "mergeArchivePlan";

  private static final String ACTION_TYPE_KEY = "actionType";
  private static final String ACTION_STATE = "actionState";
  private static final String STATE_TRANSITION_TIME = "stateTransitionTime";

  private final HoodieTableMetaClient metaClient;

  public LegacyArchivedMetaEntryReader(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public ClosableIterator<ActiveAction> getActiveActionsIterator() {
    return loadInstants(null);
  }

  public ClosableIterator<ActiveAction> getActiveActionsIterator(HoodieArchivedTimeline.TimeRangeFilter filter) {
    return loadInstants(filter);
  }

  /**
   * Reads the avro record for instant and details.
   */
  private Pair<HoodieInstant, Option<byte[]>> readInstant(GenericRecord record) {
    final String instantTime = record.get(HoodieTableMetaClient.COMMIT_TIME_KEY).toString();
    final String action = record.get(ACTION_TYPE_KEY).toString();
    final String stateTransitionTime = (String) record.get(STATE_TRANSITION_TIME);
    final Option<byte[]> details = getMetadataKey(action).map(key -> {
      Object actionData = record.get(key);
      if (actionData != null) {
        if (actionData instanceof IndexedRecord) {
          return HoodieAvroUtils.avroToFileBytes((IndexedRecord) actionData);
        } else {
          // should be json bytes.
          try {
            HoodieInstant instant = metaClient.getInstantGenerator().createNewInstant(HoodieInstant.State.COMPLETED, action, instantTime, stateTransitionTime);
            byte[] instantBytes = getUTF8Bytes(actionData.toString());
            org.apache.hudi.common.model.HoodieCommitMetadata commitMetadata = new CommitMetadataSerDeV1().deserialize(
                instant, new ByteArrayInputStream(instantBytes),
                () -> instantBytes.length == 0,
                org.apache.hudi.common.model.HoodieCommitMetadata.class);
            // convert to avro bytes.
            return convertMetadataToByteArray(commitMetadata, metaClient.getCommitMetadataSerDe());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
      return null;
    });
    InstantGenerator instantGenerator = metaClient.getInstantGenerator();
    HoodieInstant instant = instantGenerator.createNewInstant(HoodieInstant.State.valueOf(record.get(ACTION_STATE).toString()), action,
        instantTime, stateTransitionTime);
    return Pair.of(instant,details);
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
      case HoodieTimeline.LOG_COMPACTION_ACTION:
        return Option.of("hoodieCompactionPlan");
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        return Option.of("hoodieReplaceCommitMetadata");
      case HoodieTimeline.CLUSTERING_ACTION:
        return Option.of("hoodieReplaceCommitMetadata");
      case HoodieTimeline.INDEXING_ACTION:
        return Option.of("hoodieIndexCommitMetadata");
      default:
        log.error(String.format("Unknown action in metadata (%s)", action));
        return Option.empty();
    }
  }

  /**
   * This is method to read selected instants. Do NOT use this directly use one of the helper methods above
   * If loadInstantDetails is set to true, this would also update 'readCommits' map with commit details
   * If filter is specified, only the filtered instants are loaded
   * If commitsFilter is specified, only the filtered records are loaded.
   */
  private ClosableIterator<ActiveAction> loadInstants(HoodieArchivedTimeline.TimeRangeFilter filter) {
    try {
      // List all files
      List<StoragePathInfo> pathInfoList = metaClient.getStorage().globEntries(
          new StoragePath(metaClient.getArchivePath(), ".commits_.archive*"));

      // Sort files by version suffix in reverse (implies reverse chronological order)
      pathInfoList.sort(new ArchiveLogVersionComparator());

      ClosableIterator<HoodieRecord<IndexedRecord>> itr = getRecordIterator(pathInfoList);

      return new ClosableIterator<ActiveAction>() {
        private ActiveAction activeAction;

        private Pair<HoodieInstant, Option<byte[]>> nextInstantAndDetail;

        @Override
        public void close() {
          itr.close();
        }

        @Override
        public boolean hasNext() {
          List<Pair<HoodieInstant, Option<byte[]>>> instantAndDetails = new ArrayList<>();
          String lastInstantTime = null;
          if (nextInstantAndDetail != null) {
            instantAndDetails.add(nextInstantAndDetail);
            lastInstantTime = nextInstantAndDetail.getKey().requestedTime();
            nextInstantAndDetail = null;
          }
          while (itr.hasNext()) {
            HoodieRecord<IndexedRecord> record = itr.next();
            Pair<HoodieInstant, Option<byte[]>> instantAndDetail = readInstant((GenericRecord) record.getData());
            String instantTime = instantAndDetail.getKey().requestedTime();
            if (filter == null || filter.isInRange(instantTime)) {
              if (lastInstantTime == null) {
                instantAndDetails.add(instantAndDetail);
                lastInstantTime = instantTime;
              } else if (lastInstantTime.equals(instantTime)) {
                instantAndDetails.add(instantAndDetail);
              } else {
                nextInstantAndDetail = instantAndDetail;
                break;
              }
            }
          }
          if (!instantAndDetails.isEmpty()) {
            this.activeAction = ActiveActionWithDetails.fromInstantAndDetails(instantAndDetails);
            return true;
          }
          return false;
        }

        @Override
        public ActiveAction next() {
          return this.activeAction;
        }
      };
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not load archived commit timeline from path " + metaClient.getArchivePath(), e);
    }
  }

  /**
   * Returns the avro record iterator with given file statuses.
   */
  private ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(
      List<StoragePathInfo> pathInfoList) throws IOException {
    return new ClosableIterator<HoodieRecord<IndexedRecord>>() {

      final Iterator<StoragePathInfo> pathInfoIterator = pathInfoList.iterator();
      HoodieLogFormat.Reader reader;
      ClosableIterator<HoodieRecord<IndexedRecord>> recordItr;

      @Override
      public void close() {
        if (this.reader != null) {
          closeLogFormatReader(reader);
        }
      }

      @Override
      public boolean hasNext() {
        if (recordItr != null && recordItr.hasNext()) {
          return true;
        }
        // new reader if possible
        if (reader != null) {
          while (reader.hasNext()) {
            HoodieLogBlock block = reader.next();
            if (block instanceof HoodieAvroDataBlock) {
              HoodieAvroDataBlock avroBlock = (HoodieAvroDataBlock) block;
              recordItr = avroBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO);
              if (recordItr.hasNext()) {
                return true;
              }
            }
          }
          // no records in the reader, close the reader
          closeLogFormatReader(reader);
          reader = null;
        }
        // new reader
        while (pathInfoIterator.hasNext()) {
          StoragePathInfo pathInfo = pathInfoIterator.next();
          try {
            reader = HoodieLogFormat.newReader(
                metaClient.getStorage(),
                new HoodieLogFile(pathInfo.getPath()),
                HoodieSchema.fromAvroSchema(HoodieArchivedMetaEntry.getClassSchema()));
          } catch (IOException ioe) {
            throw new HoodieIOException(
                "Error initializing the reader for archived log: " + pathInfo.getPath(), ioe);
          }
          while (reader.hasNext()) {
            HoodieLogBlock block = reader.next();
            if (block instanceof HoodieAvroDataBlock) {
              HoodieAvroDataBlock avroBlock = (HoodieAvroDataBlock) block;
              recordItr = avroBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO);
              if (recordItr.hasNext()) {
                return true;
              }
            }
          }
          if (!reader.hasNext()) {
            try {
              reader.close();
            } catch (IOException e) {
              throw new HoodieIOException("Failed to close log reader " + pathInfo.getPath());
            }
          }
        }
        return false;
      }

      @Override
      public HoodieRecord<IndexedRecord> next() {
        return this.recordItr.next();
      }
    };
  }

  private void closeLogFormatReader(HoodieLogFormat.Reader reader) {
    try {
      reader.close();
    } catch (IOException ioe) {
      throw new HoodieIOException("Error closing log format reader", ioe);
    }
  }

  /**
   * Sort files by reverse order of version suffix in file name.
   */
  public static class ArchiveLogVersionComparator
      implements Comparator<StoragePathInfo>, Serializable {
    @Override
    public int compare(StoragePathInfo f1, StoragePathInfo f2) {
      return Integer.compare(getArchivedFileSuffix(f2), getArchivedFileSuffix(f1));
    }
  }

  private static int getArchivedFileSuffix(StoragePathInfo f) {
    try {
      Matcher fileMatcher = ARCHIVE_FILE_PATTERN.matcher(f.getPath().getName());
      if (fileMatcher.matches()) {
        return Integer.parseInt(fileMatcher.group(1));
      }
    } catch (NumberFormatException e) {
      // log and ignore any format warnings
      log.warn("error getting suffix for archived file: {}", f.getPath());
    }
    // return default value in case of any errors
    return 0;
  }
}
