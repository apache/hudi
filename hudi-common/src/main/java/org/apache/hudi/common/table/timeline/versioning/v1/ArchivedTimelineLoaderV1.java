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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieMergeArchiveFilePlan;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class ArchivedTimelineLoaderV1 implements ArchivedTimelineLoader {
  private static final String MERGE_ARCHIVE_PLAN_NAME = "mergeArchivePlan";
  private static final Pattern ARCHIVE_FILE_PATTERN =
      Pattern.compile("^\\.commits_\\.archive\\.([0-9]+).*");
  private static final Logger LOG = LoggerFactory.getLogger(ArchivedTimelineLoaderV1.class);

  @Override
  public Option<String> loadInstants(HoodieTableMetaClient metaClient,
                                     @Nullable HoodieArchivedTimeline.TimeRangeFilter filter,
                                     HoodieArchivedTimeline.LoadMode loadMode,
                                     Function<GenericRecord, Boolean> commitsFilter,
                                     BiConsumer<String, GenericRecord> recordConsumer) {
    return loadInstants(metaClient, filter, Option.empty(), loadMode, commitsFilter, recordConsumer, Option.empty());
  }

  @Override
  public Option<String> loadInstants(HoodieTableMetaClient metaClient,
                                     @Nullable HoodieArchivedTimeline.TimeRangeFilter filter,
                                     HoodieArchivedTimeline.LoadMode loadMode,
                                     Function<GenericRecord, Boolean> commitsFilter,
                                     BiConsumer<String, GenericRecord> recordConsumer,
                                     Option<Integer> limit) {
    return loadInstants(metaClient, filter, Option.empty(), loadMode, commitsFilter, recordConsumer, limit);
  }

  public Option<String> loadInstants(HoodieTableMetaClient metaClient,
                                     @Nullable HoodieArchivedTimeline.TimeRangeFilter filter,
                                     Option<ArchivedTimelineV1.LogFileFilter> logFileFilter,
                                     HoodieArchivedTimeline.LoadMode loadMode,
                                     Function<GenericRecord, Boolean> commitsFilter,
                                     BiConsumer<String, GenericRecord> recordConsumer,
                                     Option<Integer> limit) {
    Set<String> instantsInRange = new HashSet<>();
    AtomicInteger loadedCount = new AtomicInteger(0);
    boolean hasLimit = limit.isPresent() && limit.get() > 0;
    AtomicReference<String> lastInstantTime = new AtomicReference<>(null);
    try {
      // List all files
      List<StoragePathInfo> entryList = metaClient.getStorage().globEntries(
          new StoragePath(metaClient.getArchivePath(), ".commits_.archive*"));

      // Sort files by version suffix in reverse (implies reverse chronological order)
      entryList.sort(new ArchiveFileVersionComparator());

      for (StoragePathInfo fs : entryList) {
        if (hasLimit && loadedCount.get() >= limit.get()) {
          break;
        }
        
        if (logFileFilter.isPresent() && !logFileFilter.get().shouldLoadFile(fs)) {
          continue;
        }
        // Read the archived file
        try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient.getStorage(),
            new HoodieLogFile(fs.getPath()), HoodieSchema.fromAvroSchema(HoodieArchivedMetaEntry.getClassSchema()))) {
          int instantsInPreviousFile = instantsInRange.size();
          // Read the avro blocks
          while (reader.hasNext() && (!hasLimit || loadedCount.get() < limit.get())) {
            HoodieLogBlock block = reader.next();
            if (block instanceof HoodieAvroDataBlock) {
              HoodieAvroDataBlock avroBlock = (HoodieAvroDataBlock) block;
              // TODO If we can store additional metadata in datablock, we can skip parsing records
              // (such as startTime, endTime of records in the block)
              try (ClosableIterator<HoodieRecord<IndexedRecord>> itr = avroBlock.getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
                int commitTimeFieldPosition = avroBlock.getSchema().getField(HoodieTableMetaClient.COMMIT_TIME_KEY).map(HoodieSchemaField::pos)
                    .orElseThrow(() -> new HoodieIOException("Unable to find commit time field in archived timeline"));
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(itr, Spliterator.IMMUTABLE), true)
                    // Filter blocks in desired time window
                    .map(r -> (GenericRecord) r.getData())
                    .filter(commitsFilter::apply)
                    .forEach(r -> {
                      if (hasLimit && loadedCount.get() >= limit.get()) {
                        return;
                      }
                      String instantTime = r.get(commitTimeFieldPosition).toString();
                      if (filter == null || filter.isInRange(instantTime)) {
                        boolean isNewInstant = instantsInRange.add(instantTime);
                        recordConsumer.accept(instantTime, r);
                        // Oldest instant will be the first one processed
                        lastInstantTime.compareAndSet(null, instantTime);
                        if (hasLimit && isNewInstant) {
                          loadedCount.incrementAndGet();
                        }
                      }
                    });
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
            StoragePath planPath = new StoragePath(metaClient.getArchivePath(), MERGE_ARCHIVE_PLAN_NAME);
            HoodieStorage storage = metaClient.getStorage();
            if (storage.exists(planPath)) {
              HoodieMergeArchiveFilePlan plan = TimelineMetadataUtils.deserializeAvroMetadataLegacy(FileIOUtils.readDataFromPath(storage, planPath).get(), HoodieMergeArchiveFilePlan.class);
              String mergedArchiveFileName = plan.getMergedArchiveFileName();
              if (!StringUtils.isNullOrEmpty(mergedArchiveFileName) && fs.getPath().getName().equalsIgnoreCase(mergedArchiveFileName)) {
                LOG.debug("Catch exception because of reading uncompleted merging archive file {}. Ignore it here.", mergedArchiveFileName);
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
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not load archived commit timeline from path " + metaClient.getArchivePath(), e);
    }
    return Option.ofNullable(lastInstantTime.get());
  }

  /**
   * Sort files by reverse order of version suffix in file name.
   */
  public static class ArchiveFileVersionComparator implements Comparator<StoragePathInfo>, Serializable {
    @Override
    public int compare(StoragePathInfo f1, StoragePathInfo f2) {
      return Integer.compare(getArchivedFileSuffix(f2), getArchivedFileSuffix(f1));
    }

    private int getArchivedFileSuffix(StoragePathInfo f) {
      try {
        Matcher fileMatcher = ARCHIVE_FILE_PATTERN.matcher(f.getPath().getName());
        if (fileMatcher.matches()) {
          return Integer.parseInt(fileMatcher.group(1));
        }
      } catch (NumberFormatException e) {
        // log and ignore any format warnings
        LOG.warn("error getting suffix for archived file: {}", f.getPath());
      }

      // return default value in case of any errors
      return 0;
    }
  }
}
