/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cli;

import org.apache.hudi.client.timeline.LSMTimelineWriter;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cleans up expired V1 archive history files (`.commits_.archive.<version>[_<write-token>]`)
 * under the `.hoodie/archived` directory.
 *
 * <p>A file is deleted only when ALL of the following hold:
 * <ul>
 *   <li>It matches the V1 archive file naming pattern;</li>
 *   <li>Its version number is strictly lower than the highest version present
 *       (the highest version is always retained);</li>
 *   <li>Its last modification time is strictly earlier than the cutoff time,
 *       where cutoff = requested time of the earliest commit/deltacommit instant
 *       in the active timeline minus {@code retainDays} days.</li>
 * </ul>
 *
 * <p>Before deletion each candidate file is re-checked via {@link HoodieStorage#getPathInfo};
 * if its size or modification time changed since the initial listing, it is skipped.
 * Parquet files, manifests and any other files that do not match the V1 archive
 * naming pattern are never touched.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ArchiveHistoryFileCleaner {

  private static final Pattern ARCHIVE_FILE_PATTERN =
      Pattern.compile("^\\.commits_\\.archive\\.([0-9]+).*");
  private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

  /** Metadata of a single deleted archive file. */
  @AllArgsConstructor
  public static class DeletedFileInfo {
    private final String path;
    private final long sizeBytes;
    private final long modificationTime;

    public String getPath() {
      return path;
    }

    public long getSizeBytes() {
      return sizeBytes;
    }

    public long getModificationTime() {
      return modificationTime;
    }
  }

  /** Aggregate result of a clean run. */
  @AllArgsConstructor
  public static class CleanResult {
    private final String archivePath;
    /** Cutoff time in Unix millis; {@code null} when no commit/deltacommit instant exists. */
    private final Long cutoffTime;
    private final long archiveSizeBytes;
    private final List<DeletedFileInfo> deletedFiles;

    public String getArchivePath() {
      return archivePath;
    }

    public Long getCutoffTime() {
      return cutoffTime;
    }

    public long getArchiveSizeBytes() {
      return archiveSizeBytes;
    }

    public List<DeletedFileInfo> getDeletedFiles() {
      return deletedFiles;
    }
  }

  /**
   * Deletes expired archive history files under the protection of the same
   * transaction lock configuration used by the archiver.
   *
   * @param metaClient meta client of the target table
   * @param config     write config carrying lock configuration
   * @param retainDays days to retain before the earliest commit/deltacommit; must be positive
   * @return clean result
   */
  public static CleanResult clean(HoodieTableMetaClient metaClient,
                                  HoodieWriteConfig config,
                                  int retainDays) {
    if (retainDays <= 0) {
      throw new IllegalArgumentException(
          "retain_days must be a positive integer, got: " + retainDays);
    }

    HoodieStorage storage = metaClient.getStorage();
    TransactionManager txnManager = new TransactionManager(config, storage);
    try {
      txnManager.beginStateChange(Option.empty(), Option.empty());
      if (TimelineLayoutVersion.LAYOUT_VERSION_2.equals(metaClient.getTimelineLayoutVersion())) {
        StoragePath archive = metaClient.getArchivePath();
        Long cutoff = computeCutoffTime(metaClient, retainDays);
        try {
          if (!storage.exists(archive)) {
            return new CleanResult(archive.toString(), cutoff, 0L, java.util.Collections.emptyList());
          }
          if (cutoff != null) {
            LSMTimelineWriter.getInstance(config, new LocalTaskContextSupplier(), metaClient).trimBefore(cutoff);
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to trim archive snapshot at " + archive, e);
        }
        // Old snapshots retain their files until the normal archive cleaner reclaims them.
        return new CleanResult(archive.toString(), cutoff, computeDirectorySize(storage, archive), java.util.Collections.emptyList());
      }
      return doClean(metaClient, storage, retainDays);
    } finally {
      txnManager.endStateChange(Option.empty());
      txnManager.close();
    }
  }

  private static CleanResult doClean(HoodieTableMetaClient metaClient,
                                     HoodieStorage storage,
                                     int retainDays) {
    StoragePath archivePath = metaClient.getArchivePath();

    // 1. Compute the cutoff time from the earliest commit/deltacommit instant.
    Long cutoffTimeMs = computeCutoffTime(metaClient, retainDays);

    // 2. List archive files matching the V1 naming pattern and parse their versions.
    List<StoragePathInfo> archiveFiles;
    try {
      archiveFiles = storage.globEntries(
          new StoragePath(archivePath, ".commits_.archive*"));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to list archive files under " + archivePath, e);
    }

    long maxVersion = -1L;
    Map<StoragePathInfo, Long> versionedFiles = new HashMap<>();
    for (StoragePathInfo info : archiveFiles) {
      if (!info.isFile()) {
        continue;
      }
      Matcher matcher = ARCHIVE_FILE_PATTERN.matcher(info.getPath().getName());
      if (matcher.matches()) {
        long version = Long.parseLong(matcher.group(1));
        versionedFiles.put(info, version);
        if (version > maxVersion) {
          maxVersion = version;
        }
      }
    }

    // 3. Delete eligible files: older version AND modification time strictly before cutoff.
    List<DeletedFileInfo> deletedFiles = new ArrayList<>();
    if (cutoffTimeMs != null) {
      for (Map.Entry<StoragePathInfo, Long> entry : versionedFiles.entrySet()) {
        StoragePathInfo scanned = entry.getKey();
        long version = entry.getValue();
        if (version >= maxVersion) {
          continue; // the highest version is always retained
        }
        if (scanned.getModificationTime() >= cutoffTimeMs) {
          continue; // not old enough
        }
        // Re-check the file right before deletion; skip if size or mtime changed.
        StoragePathInfo latest;
        try {
          latest = storage.getPathInfo(scanned.getPath());
        } catch (IOException e) {
          throw new HoodieIOException(
              "Failed to re-check archive file before deletion: " + scanned.getPath(), e);
        }
        if (latest.getLength() != scanned.getLength()
            || latest.getModificationTime() != scanned.getModificationTime()) {
          log.warn("Archive file {} changed during the clean operation (size or mtime mismatch), "
              + "skipping deletion.", scanned.getPath());
          continue;
        }
        boolean deleted;
        try {
          deleted = storage.deleteFile(scanned.getPath());
        } catch (IOException e) {
          throw new HoodieIOException("Failed to delete archive file: " + scanned.getPath(), e);
        }
        if (!deleted) {
          throw new HoodieIOException("Failed to delete archive file: " + scanned.getPath());
        }
        log.info("Deleted archive history file: {}", scanned.getPath());
        deletedFiles.add(new DeletedFileInfo(
            scanned.getPath().toString(),
            scanned.getLength(),
            scanned.getModificationTime()));
      }
    } else {
      log.info("No commit/deltacommit instant found in the active timeline; skipping deletion.");
    }

    // 4. Compute the total archive directory size after cleaning (recursive).
    long archiveSizeBytes = computeDirectorySize(storage, archivePath);

    return new CleanResult(
        archivePath.toString(),
        cutoffTimeMs,
        archiveSizeBytes,
        deletedFiles);
  }

  /**
   * Returns the cutoff time in Unix millis, or {@code null} when the active timeline
   * contains no commit/deltacommit instant (or its requested time cannot be parsed).
   */
  private static Long computeCutoffTime(HoodieTableMetaClient metaClient, int retainDays) {
    Option<HoodieInstant> earliestInstant = metaClient.getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(
            HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
        .firstInstant();
    if (!earliestInstant.isPresent()) {
      return null;
    }
    Option<Date> date = TimelineUtils.parseDateFromInstantTimeSafely(
        earliestInstant.get().requestedTime());
    if (!date.isPresent()) {
      log.warn("Failed to parse requested time of the earliest instant {}; skipping deletion.",
          earliestInstant.get());
      return null;
    }
    return date.get().getTime() - retainDays * MILLIS_PER_DAY;
  }

  private static long computeDirectorySize(HoodieStorage storage, StoragePath dirPath) {
    long total = 0L;
    try {
      for (StoragePathInfo info : storage.listFiles(dirPath)) {
        if (info.isFile()) {
          total += info.getLength();
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException(
          "Failed to compute archive directory size for " + dirPath, e);
    }
    return total;
  }
}