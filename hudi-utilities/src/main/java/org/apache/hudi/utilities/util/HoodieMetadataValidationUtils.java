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

package org.apache.hudi.utilities.util;

import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.toStringWithThreshold;

/**
 * Comparison helpers shared by {@link org.apache.hudi.utilities.HoodieMetadataTableValidator} and
 * {@code org.apache.hudi.utilities.HoodieMetadataTableValidatorV2}.
 *
 * <p>Both validators must reach the same verdict for the same table, so the file-slice comparison,
 * its tolerance for in-flight cleaning and uncommitted log files, and the availability checks that
 * gate a run all live here rather than being duplicated or reached into across classes.
 */
public class HoodieMetadataValidationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataValidationUtils.class);

  /**
   * The namespace this validator writes its own per-table output under.
   *
   * <p><b>It is a naming convention and not a guard.</b> Nothing skips a listing on the strength of
   * it, and nothing needs to: a directory is a partition <b>iff</b> it carries
   * {@code .hoodie_partition_metadata} ({@code HoodiePartitionMetadata.hasPartitionMetadata}, via
   * {@code FSUtils.getAllPartitionPaths}), and inside a partition only entries
   * {@code FSUtils.isBaseFile}/{@code isLogFile} accept are considered. A report directory is
   * neither, whatever it is called, and no prefix is consulted anywhere in the walk.
   *
   * <p>Two consumers, both of them naming rather than filtering:
   * {@code Config.DEFAULT_VALIDATION_OUTPUT_DIR} is built from it, so the default output directory
   * and this constant cannot drift apart; and {@code HoodieMetadataTableValidatorV2.validateConfig}
   * warns when reports are pointed under the table but outside it, on the grounds that an
   * unrecognised report directory under a table is usually a typo.
   *
   * <p>Vendor-neutral on purpose — the directory names a Hudi tool, not whoever is running it.
   */
  public static final String TOOLING_DIR_PREFIX = ".hoodie_validation";

  private HoodieMetadataValidationUtils() {
  }

  /** Base file names referenced by inflight cleaner plans; such files may legitimately differ between listings. */
  public static Set<String> getBaseFilesForCleaning(HoodieTableMetaClient metaClient) {
    HoodieTimeline inflightCleaningTimeline = metaClient.getActiveTimeline().getCleanerTimeline().filterInflights();

    return inflightCleaningTimeline.getInstantsAsStream().flatMap(instant -> {
      try {
        // convert inflight instant to requested and get clean plan
        instant = metaClient.createNewInstant(HoodieInstant.State.REQUESTED, instant.getAction(),
            instant.requestedTime());
        HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(metaClient, instant);

        return cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().flatMap(cleanerFileInfoList ->
          cleanerFileInfoList.stream().map(fileInfo -> new Path(fileInfo.getFilePath()).getName())
        );

      } catch (IOException e) {
        throw new HoodieIOException("Error reading cleaner metadata for " + instant);
      }
      // only take care of base files here.
    }).filter(path -> {
      String fileExtension = FSUtils.getFileExtension(path);
      return HoodieFileFormat.BASE_FILE_EXTENSIONS.contains(fileExtension);
    }).collect(Collectors.toSet());
  }

  public static boolean checkMetadataTableIsAvailable(JavaSparkContext jsc, String basePath,
                                               HoodieTableMetaClient dataMetaClient, String taskLabels) {
    try {
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(new Path(basePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH).toString())
          .setLoadActiveTimelineOnLoad(true)
          .build();
      int finishedInstants = mdtMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
      if (finishedInstants == 0) {
        if (dataMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
          LOG.info("There is no completed commit in both metadata table and corresponding data table: {}", taskLabels);
          return false;
        } else {
          throw new HoodieValidationException("There is no completed instant for metadata table: " + basePath);
        }
      }
      return true;
    } catch (TableNotFoundException tbe) {
      // Suppress the TableNotFound exception if Metadata table is not available to read for now
      LOG.warn("Metadata table is not found for table: {}. Skip current validation.", basePath);
      return false;
    } catch (Exception ex) {
      LOG.warn("Metadata table is not available to read for now for table: {}, ", basePath, ex);
      return false;
    }
  }

  public static List<FileSlice> filterFileSliceBasedOnInflightCleaning(List<FileSlice> sortedLatestFileSliceList, Set<String> baseDataFilesForCleaning) {
    return sortedLatestFileSliceList.stream()
        .filter(fileSlice -> {
          if (!fileSlice.getBaseFile().isPresent()) {
            return true;
          } else {
            return !baseDataFilesForCleaning.contains(fileSlice.getBaseFile().get().getFileName());
          }
        }).collect(Collectors.toList());
  }

  public static void validateFileSlices(
      List<FileSlice> fileSliceListFromMetadataTable, List<FileSlice> fileSliceListFromFS,
      String partitionPath, HoodieTableMetaClient metaClient, String label, String basePath,
      int logDetailMaxLength) {
    boolean mismatch = false;
    String errorDetails = "";
    if (fileSliceListFromMetadataTable.size() != fileSliceListFromFS.size()) {
      errorDetails = String.format(
          "Number of file slices based on the file system does not match that based on the "
              + "metadata table. File system-based listing: %d & MDT-based listing: %d. %s",
          fileSliceListFromFS.size(),
          fileSliceListFromMetadataTable.size(),
          computeDiffSummary(fileSliceListFromMetadataTable, fileSliceListFromFS, logDetailMaxLength));
      mismatch = true;
    } else if (!fileSliceListFromMetadataTable.equals(fileSliceListFromFS)) {
      // In-memory cache for the set of committed files of commits of interest
      Map<String, Set<String>> committedFilesMap = new HashMap<>();
      for (int i = 0; i < fileSliceListFromMetadataTable.size(); i++) {
        FileSlice fileSlice1 = fileSliceListFromMetadataTable.get(i);
        FileSlice fileSlice2 = fileSliceListFromFS.get(i);
        if (!Objects.equals(fileSlice1.getFileGroupId(), fileSlice2.getFileGroupId())
            || !Objects.equals(fileSlice1.getBaseInstantTime(), fileSlice2.getBaseInstantTime())) {
          errorDetails = String.format("File group ID (missing a file group in MDT) "
                  + "or base instant time mismatches. File slice from file system-based listing: %s; "
                  + "File slice from MDT-based listing: %s.",
              fileSlice2, fileSlice1);
          mismatch = true;
          break;
        }
        if (!assertBaseFilesEquality(fileSlice1, fileSlice2)) {
          errorDetails = String.format("Base files mismatch. "
                  + "File slice from file system-based listing: %s; "
                  + "File slice from MDT-based listing: %s.",
              fileSlice2, fileSlice1);
          mismatch = true;
          break;
        }
        // test for log files equality
        Pair<Boolean, String> matchingResult = areFileSliceCommittedLogFilesMatching(
            fileSlice1, fileSlice2, metaClient, committedFilesMap);
        if (Boolean.FALSE.equals(matchingResult.getLeft())) {
          errorDetails = "Log files mismatch (first file slice based on MDT, "
              + "second file slice based on the file system). "
              + matchingResult.getRight();
          mismatch = true;
          break;
        } else {
          LOG.info("There are uncommitted log files in the latest file slices but the committed log files match: {} {}", fileSlice1, fileSlice2);
        }
      }
    }

    if (mismatch) {
      String message = String.format("Validation of %s for partition %s failed for table: %s. %s",
          label, partitionPath, basePath, errorDetails);
      LOG.error(message);
      throw new HoodieValidationException(message);
    } else {
      LOG.info("Validation of {} succeeded for partition {} for table: {}", label, partitionPath, basePath);
    }
  }

  /**
   * Summarises which file slices each side holds and the other does not.
   *
   * <p>Consults no timeline, and cannot: it takes two slice listings and a length budget. It used
   * to load the archived timeline to work out whether the storage-only slices belonged to commits
   * that exist nowhere, and log a warning about it — a diagnostic that never affected the verdict
   * and that cost an archived-timeline load on the executor for every failing partition. The
   * mismatch is already established by the time this is called; the two listings below are the
   * evidence, and no further timeline reading is needed to act on them.
   */
  public static String computeDiffSummary(List<FileSlice> fileSliceListFromMetadataTable,
                                          List<FileSlice> fileSliceListFromFS,
                                          int logDetailMaxLength) {
    Set<HoodieFileGroupId> fileGroupIdsFromFS = fileSliceListFromFS.stream()
        .map(FileSlice::getFileGroupId).collect(Collectors.toSet());
    Set<HoodieFileGroupId> fileGroupIdsFromMetadataTable = fileSliceListFromMetadataTable.stream()
        .map(FileSlice::getFileGroupId).collect(Collectors.toSet());
    List<FileSlice> fileSlicesInFileSystemOnly = fileSliceListFromFS.stream()
        .filter(fileSlice -> !fileGroupIdsFromMetadataTable.contains(fileSlice.getFileGroupId()))
        .collect(Collectors.toList());
    List<FileSlice> fileSlicesInMetadataTableOnly = fileSliceListFromMetadataTable.stream()
        .filter(fileSlice -> !fileGroupIdsFromFS.contains(fileSlice.getFileGroupId()))
        .collect(Collectors.toList());
    return "File Slices in file system only: ["
        + toStringWithThreshold(fileSlicesInFileSystemOnly, logDetailMaxLength)
        + "]; File Slices in metadata table only: ["
        + toStringWithThreshold(fileSlicesInMetadataTableOnly, logDetailMaxLength)
        + "]";
  }

  static boolean assertBaseFilesEquality(FileSlice fileSlice1, FileSlice fileSlice2) {
    if (fileSlice1.getBaseFile().isPresent() && fileSlice2.getBaseFile().isPresent()) {
      HoodieBaseFile baseFile1 = fileSlice1.getBaseFile().get();
      HoodieBaseFile baseFile2 = fileSlice2.getBaseFile().get();
      return baseFile1.getFileName().equals(baseFile2.getFileName()) && baseFile1.getFileId().equals(baseFile2.getFileId())
          && baseFile1.getFileSize() == baseFile2.getFileSize();
    } else {
      return fileSlice1.getBaseFile().isPresent() == fileSlice2.getBaseFile().isPresent();
    }
  }

  /**
   * Compares committed log files from two file slices.
   *
   * @param fs1               File slice 1
   * @param fs2               File slice 2
   * @param metaClient        {@link HoodieTableMetaClient} instance
   * @param committedFilesMap In-memory map for caching committed files of commits
   * @return {@code true} if matching; {@code false} and the error message otherwise.
   */
  private static Pair<Boolean, String> areFileSliceCommittedLogFilesMatching(
      FileSlice fs1,
      FileSlice fs2,
      HoodieTableMetaClient metaClient,
      Map<String, Set<String>> committedFilesMap) {
    Set<String> fs1LogPathSet =
        fs1.getLogFiles().map(f -> f.getPath().toString()).collect(Collectors.toSet());
    Set<String> fs2LogPathSet =
        fs2.getLogFiles().map(f -> f.getPath().toString()).collect(Collectors.toSet());
    Set<String> commonLogPathSet = new HashSet<>(fs1LogPathSet);
    commonLogPathSet.retainAll(fs2LogPathSet);
    // Only keep log file paths that differ
    fs1LogPathSet.removeAll(commonLogPathSet);
    fs2LogPathSet.removeAll(commonLogPathSet);
    // Check if the remaining log files are uncommitted.  If there is any log file
    // that is committed, the committed log files of two file slices are different
    HoodieStorage storage = metaClient.getStorage();

    Pair<Boolean, String> checkResult =
        hasCommittedLogFiles(storage, fs1LogPathSet, metaClient, committedFilesMap);
    if (Boolean.TRUE.equals(checkResult.getLeft())) {
      return Pair.of(false, String.format(
          "The first file slice has committed log files that cause mismatching: %s"
              + "; Different log files are: %s. Details: %s.",
          fs1, fs1LogPathSet, checkResult.getRight()));
    }
    checkResult =
        hasCommittedLogFiles(storage, fs2LogPathSet, metaClient, committedFilesMap);
    if (Boolean.TRUE.equals(checkResult.getLeft())) {
      return Pair.of(false, String.format(
          "The second file slice has committed log files that cause mismatching: %s"
              + "; Different log files are: %s.  Details: %s.",
          fs2, fs2LogPathSet, checkResult.getRight()));
    }
    return Pair.of(true, "");
  }

  public static Pair<Boolean, String> hasCommittedLogFiles(
      HoodieStorage storage,
      Set<String> logFilePathSet,
      HoodieTableMetaClient metaClient,
      Map<String, Set<String>> committedFilesMap) {
    if (logFilePathSet.isEmpty()) {
      return Pair.of(false, "");
    }

    String basePath = metaClient.getBasePath().toString();
    HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline();
    HoodieTimeline completedInstantsTimeline = commitsTimeline.filterCompletedInstants();
    HoodieTimeline inflightInstantsTimeline = commitsTimeline.filterInflights();

    for (String logFilePathStr : logFilePathSet) {
      HoodieLogFormat.Reader reader = null;
      try {
        HoodieSchema readerSchema = TableSchemaResolver.readSchemaFromLogFile(metaClient, new StoragePath(logFilePathStr));
        if (readerSchema == null) {
          LOG.warn("Cannot read schema from log file {}. Skip the check as it's likely being written by an inflight instant.", logFilePathStr);
          continue;
        }
        reader =
            HoodieLogFormat.newReader(metaClient, new HoodieLogFile(logFilePathStr), readerSchema, false);
        // read the avro blocks
        if (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          final String instantTime = block.getLogBlockHeader().get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME);
          if (completedInstantsTimeline.containsInstant(instantTime)) {
            // The instant is completed, in active timeline
            // Checking commit metadata only as log files can only be written by COMMIT or DELTA_COMMIT
            if (!committedFilesMap.containsKey(instantTime)) {
              HoodieInstant instant =  completedInstantsTimeline.filter(i -> i.requestedTime().equals(instantTime))
                  .firstInstant().get();
              HoodieCommitMetadata commitMetadata =
                  completedInstantsTimeline.readCommitMetadata(instant);
              committedFilesMap.put(
                  instantTime,
                  commitMetadata.getWriteStats().stream()
                      .map(HoodieWriteStat::getPath).collect(Collectors.toSet())
              );
            }

            // Here we check if the commit metadata contains the log file.
            // Note that, a log file may be written by a successful write transaction
            // leading to a delta commit, but such a log file can be uncommitted and
            // not be part of any snapshot, due to Spark task retries for example.
            // In such a case, the log file can stay in the file system, but the metadata
            // table does not contain the log file for file listing, which is an expected
            // behavior.
            String relativeLogFilePathStr = getRelativePath(basePath, logFilePathStr);
            if (committedFilesMap.get(instantTime).contains(relativeLogFilePathStr)) {
              return Pair.of(true, String.format(
                  "Log file is committed in an instant in active timeline: instantTime=%s %s",
                  instantTime, logFilePathStr));
            } else {
              LOG.warn("Log file is uncommitted in a completed instant, likely due to retry: instantTime={} {}", instantTime, logFilePathStr);
            }
          } else if (completedInstantsTimeline.isBeforeTimelineStarts(instantTime)) {
            // The instant is in archived timeline
            return Pair.of(true, String.format(
                "Log file is committed in an instant in archived timeline: instantTime=%s %s",
                instantTime, logFilePathStr));
          } else if (inflightInstantsTimeline.containsInstant(instantTime)) {
            // The instant is inflight in active timeline
            // hit an uncommitted block possibly from a failed write
            LOG.warn("Log file is uncommitted because of an inflight instant: instantTime={} {}", instantTime, logFilePathStr);
          } else {
            // The instant is after the start of the active timeline,
            // but it cannot be found in the active timeline
            LOG.warn("Log file is uncommitted because the instant is after the start of the active timeline but absent or in requested in the active timeline: instantTime={} {}",
                instantTime, logFilePathStr);
          }
        } else {
          LOG.warn("There is no log block in {}", logFilePathStr);
        }
      } catch (IOException e) {
        LOG.warn("Cannot read log file {}. Skip the check as it's likely being written by an inflight instant.",
            logFilePathStr, e);
      } finally {
        FileIOUtils.closeQuietly(reader);
      }
    }
    return Pair.of(false, "");
  }

  private static String getRelativePath(String basePath, String absoluteFilePath) {
    String basePathStr = new StoragePath(basePath).getPathWithoutSchemeAndAuthority().toString();
    String absoluteFilePathStr = new StoragePath(absoluteFilePath).getPathWithoutSchemeAndAuthority().toString();

    if (!absoluteFilePathStr.startsWith(basePathStr)) {
      throw new IllegalArgumentException("File path does not belong to the base path! basePath="
          + basePathStr + " absoluteFilePathStr=" + absoluteFilePathStr);
    }

    String relativePathStr = absoluteFilePathStr.substring(basePathStr.length());
    return relativePathStr.startsWith("/") ? relativePathStr.substring(1) : relativePathStr;
  }

  public static class FileSliceComparator implements Comparator<FileSlice>, Serializable {

    @Override
    public int compare(FileSlice o1, FileSlice o2) {
      return (o1.getPartitionPath() + o1.getFileId() + o1.getBaseInstantTime())
          .compareTo(o2.getPartitionPath() + o2.getFileId() + o2.getBaseInstantTime());
    }
  }
}
