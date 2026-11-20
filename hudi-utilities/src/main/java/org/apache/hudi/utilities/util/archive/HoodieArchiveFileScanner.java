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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieMergeArchiveFilePlan;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Reads v6 archive files off {@link HoodieStorage} through the log format.
 *
 * <p><b>No file is skipped on a probed range.</b> A file's first and last records do not bound
 * what it holds: the archiver sorts each batch only in recent releases (older ones wrote a batch's
 * cleans ahead of its commits), and one file can hold several archival runs where the storage lets
 * the writer append. A range read from the ends would therefore let a file holding a target be
 * skipped, and an instant it holds come back absent, so {@link #probe} always declines and every
 * file is scanned in full.
 *
 * <p><b>A corrupt block is reported, not dropped.</b> {@code HoodieLogFileReader.readBlock} returns
 * a {@code HoodieCorruptBlock} rather than throwing, and {@code ArchivedTimelineLoaderV1} silently
 * skips it. Here it makes the scan unclean, which turns the file's instants into
 * {@code INDETERMINATE} rather than absent.
 */
public class HoodieArchiveFileScanner implements ArchiveFileScanner {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieArchiveFileScanner.class);
  private static final String MERGE_ARCHIVE_PLAN_NAME = "mergeArchivePlan";

  private final HoodieTableMetaClient metaClient;

  public HoodieArchiveFileScanner(HoodieTableMetaClient metaClient) {
    this.metaClient = Objects.requireNonNull(metaClient, "metaClient");
  }

  /** Always empty: a V1 archive file's ends do not bound its content; see the class javadoc. */
  @Override
  public Option<InstantRange> probe(ArchiveFile file) {
    return Option.empty();
  }

  @Override
  public ScanOutcome scan(ArchiveFile file,
                          InstantRange coverage,
                          LiveTargets targets,
                          BiConsumer<String, GenericRecord> onHit) {
    boolean corrupt = false;
    try (HoodieLogFormat.Reader reader = openForward(file)) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (block instanceof HoodieCorruptBlock) {
          corrupt = true;
          continue;
        }
        if (!(block instanceof HoodieAvroDataBlock)) {
          continue;
        }
        try (ClosableIterator<HoodieRecord<IndexedRecord>> records =
                 ((HoodieAvroDataBlock) block).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
          while (records.hasNext()) {
            GenericRecord record = (GenericRecord) records.next().getData();
            Object commitTime = record.get(HoodieTableMetaClient.COMMIT_TIME_KEY);
            if (commitTime == null) {
              continue;
            }
            String instantTime = commitTime.toString();
            if (targets.isTarget(instantTime)) {
              targets.markFound(instantTime, file.path());
              onHit.accept(instantTime, record);
            }
          }
        }
      }
    } catch (Exception e) {
      if (isIncompleteMergedArchiveFile(file)) {
        // Expected: an in-progress merge leaves a partial file whose content still lives in the
        // sources it was built from. Not damage.
        LOG.debug("Ignoring the incomplete merged archive file {}", file.path());
        return ScanOutcome.clean();
      }
      LOG.warn("Archive file {} could not be read", file.path(), e);
      return ScanOutcome.damaged("the file could not be read: " + e.getClass().getSimpleName());
    }
    return corrupt
        ? ScanOutcome.damaged("the file holds at least one corrupt block")
        : ScanOutcome.clean();
  }

  private HoodieLogFormat.Reader openForward(ArchiveFile file) throws IOException {
    return HoodieLogFormat.newReader(metaClient, logFileOf(file),
        HoodieSchema.fromAvroSchema(HoodieArchivedMetaEntry.getClassSchema()));
  }

  private static HoodieLogFile logFileOf(ArchiveFile file) {
    return new HoodieLogFile(file.path(), file.sizeBytes());
  }

  /**
   * Whether this file is the half-written output of a merge-archive plan that is still in progress.
   * Mirrors the carve-out {@code ArchivedTimelineLoaderV1} makes.
   */
  private boolean isIncompleteMergedArchiveFile(ArchiveFile file) {
    try {
      StoragePath planPath = new StoragePath(metaClient.getArchivePath(), MERGE_ARCHIVE_PLAN_NAME);
      HoodieStorage storage = metaClient.getStorage();
      if (!storage.exists(planPath)) {
        return false;
      }
      HoodieMergeArchiveFilePlan plan = TimelineMetadataUtils.deserializeAvroMetadataLegacy(
          FileIOUtils.readDataFromPath(storage, planPath).get(), HoodieMergeArchiveFilePlan.class);
      String mergedName = plan.getMergedArchiveFileName();
      return !StringUtils.isNullOrEmpty(mergedName)
          && file.path().getName().equalsIgnoreCase(mergedName);
    } catch (Exception e) {
      // A corrupt plan alongside a corrupt archive file is not a licence to call the file benign.
      LOG.warn("Could not read the merge-archive plan while classifying {}; treating the file as "
          + "damaged", file.path(), e);
      return false;
    }
  }
}
