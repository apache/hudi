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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Compiles <b>every</b> disagreement between a partition's latest file slices as the metadata
 * table reports them and as a storage listing reports them.
 *
 * <p>The exhaustive counterpart to v1's {@code HoodieMetadataTableValidator#validateFileSlices}, which
 * throws at the first difference. It keys by file group so it survives the first difference, and
 * applies the same base-file rule as v1 ({@link HoodieMetadataValidationUtils#assertBaseFilesEquality}).
 *
 * <p>Records carry no verdict: phase 2 adjudicates against the pinned timeline. This class supplies
 * the writing instant while the file is in hand. A file whose instant cannot be attributed (command
 * block, unreadable log file, unparseable FS-side base name) is dropped and counted, never given a
 * placeholder instant.
 *
 * <p>Both inputs project one base file per file group and only the latest slice, so duplicate base
 * files and older slices of a file group both sides know are invisible here
 * ({@link DuplicateBaseFileDetector} covers duplicates). The exception is a file group the metadata
 * table does not know at all: every slice of it on storage is reported, since no slice of a group
 * the metadata table never recorded can be one it vouches for. Reporting only the latest would
 * leave the older slices to surface one per run, each once the slice above it is deleted.
 */
public final class FileSliceMismatchAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(FileSliceMismatchAnalyzer.class);

  private FileSliceMismatchAnalyzer() {
  }

  /**
   * @param fileSliceListFromMetadataTable latest slices as the metadata table reports them
   * @param fileSliceListFromFS            latest slices as the storage listing reports them
   * @param allFileSlicesFromFS            every slice the storage listing reports for the partition,
   *                                       latest included; called at most once, and only if some
   *                                       file group is on storage and not in the metadata table
   * @param partitionPath                  relative partition path, empty for a non-partitioned table
   * @param logFileReader                  resolves an FS-exclusive log file's writing instant and
   *                                       its first block's type; never called for an MDT-exclusive
   *                                       log file, which is not on storage to read
   */
  public static DiffTally analyze(List<FileSlice> fileSliceListFromMetadataTable,
                                  List<FileSlice> fileSliceListFromFS,
                                  Supplier<List<FileSlice>> allFileSlicesFromFS,
                                  String partitionPath,
                                  LogFileReader logFileReader) {
    DiffTally tally = new DiffTally();
    Map<String, FileSlice> mdtByFileGroup = byFileGroup(fileSliceListFromMetadataTable, "metadata table");
    Map<String, FileSlice> fsByFileGroup = byFileGroup(fileSliceListFromFS, "file system");

    Set<String> divergedFileGroups = addFsExclusiveGroupsAndSlices(tally, fsByFileGroup,
        mdtByFileGroup, partitionPath, logFileReader);
    addOlderSlicesOfFsExclusiveGroups(tally, fsByFileGroup, mdtByFileGroup, allFileSlicesFromFS,
        partitionPath, logFileReader);

    for (Map.Entry<String, FileSlice> entry : mdtByFileGroup.entrySet()) {
      if (!fsByFileGroup.containsKey(entry.getKey())) {
        addSliceFiles(tally, MismatchPattern.MDT_EXCLUSIVE_FILE_GROUP, entry.getValue(),
            partitionPath, logFileReader,
            "file group is in the metadata table and not in the storage listing");
      }
    }

    for (Map.Entry<String, FileSlice> entry : fsByFileGroup.entrySet()) {
      FileSlice mdtSlice = mdtByFileGroup.get(entry.getKey());
      // Already fully reported above; there is no shared slice to compare file by file.
      if (mdtSlice == null || divergedFileGroups.contains(entry.getKey())) {
        continue;
      }
      compareMatchingSlices(tally, partitionPath, mdtSlice, entry.getValue(), logFileReader);
    }
    return tally;
  }

  /**
   * Emits {@code FS_EXCLUSIVE_FILE_GROUP}, and for a diverged slice both
   * {@code FS_EXCLUSIVE_FILE_SLICE} and {@code MDT_EXCLUSIVE_FILE_SLICE}: either side may be the
   * wrong one, and this class does not judge which.
   *
   * @return the file group ids whose slices diverged, which the caller must not compare file by file
   */
  private static Set<String> addFsExclusiveGroupsAndSlices(
      DiffTally tally, Map<String, FileSlice> fsByFileGroup,
      Map<String, FileSlice> mdtByFileGroup, String partitionPath,
      LogFileReader logFileReader) {
    Set<String> divergedFileGroups = new HashSet<>();
    for (Map.Entry<String, FileSlice> entry : fsByFileGroup.entrySet()) {
      FileSlice fsSlice = entry.getValue();
      FileSlice mdtSlice = mdtByFileGroup.get(entry.getKey());
      if (mdtSlice == null) {
        addSliceFiles(tally, MismatchPattern.FS_EXCLUSIVE_FILE_GROUP, fsSlice,
            partitionPath, logFileReader,
            "file group is in the storage listing and not in the metadata table");
        continue;
      }
      if (Objects.equals(fsSlice.getBaseInstantTime(), mdtSlice.getBaseInstantTime())) {
        continue;
      }
      divergedFileGroups.add(entry.getKey());
      addSliceFiles(tally, MismatchPattern.FS_EXCLUSIVE_FILE_SLICE, fsSlice, partitionPath,
          logFileReader, "the metadata table's latest slice for this file "
              + "group is " + mdtSlice.getBaseInstantTime());
      addSliceFiles(tally, MismatchPattern.MDT_EXCLUSIVE_FILE_SLICE, mdtSlice, partitionPath,
          logFileReader, "the storage listing's latest slice for this file "
              + "group is " + fsSlice.getBaseInstantTime());
    }
    return divergedFileGroups;
  }

  /**
   * Emits {@code FS_EXCLUSIVE_FILE_GROUP} for every slice below the latest of each file group the
   * metadata table does not know, so one run reports the whole group rather than one layer of it.
   * The older slices' files count toward files seen, which otherwise counts the latest slices only.
   */
  private static void addOlderSlicesOfFsExclusiveGroups(
      DiffTally tally, Map<String, FileSlice> fsByFileGroup,
      Map<String, FileSlice> mdtByFileGroup, Supplier<List<FileSlice>> allFileSlicesFromFS,
      String partitionPath, LogFileReader logFileReader) {
    Map<String, FileSlice> fsExclusiveLatest = fsByFileGroup.entrySet().stream()
        .filter(entry -> !mdtByFileGroup.containsKey(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    if (fsExclusiveLatest.isEmpty()) {
      return;
    }
    for (FileSlice slice : allFileSlicesFromFS.get()) {
      FileSlice latest = fsExclusiveLatest.get(slice.getFileId());
      // The latest slice was reported by addFsExclusiveGroupsAndSlices.
      if (latest == null || Objects.equals(latest.getBaseInstantTime(), slice.getBaseInstantTime())) {
        continue;
      }
      tally.addFilesSeen((slice.getBaseFile().isPresent() ? 1 : 0) + slice.getLogFiles().count());
      addSliceFiles(tally, MismatchPattern.FS_EXCLUSIVE_FILE_GROUP, slice, partitionPath,
          logFileReader, "file group is in the storage listing and not in the metadata table; "
              + "an older slice than the latest, " + latest.getBaseInstantTime());
    }
  }

  /** Both sides hold the same file group at the same base instant; compares the files inside. */
  private static void compareMatchingSlices(DiffTally tally, String partitionPath,
                                            FileSlice mdtSlice, FileSlice fsSlice,
                                            LogFileReader logFileReader) {
    compareBaseFiles(tally, partitionPath, mdtSlice, fsSlice);
    compareLogFiles(tally, partitionPath, mdtSlice, fsSlice, logFileReader);
  }

  private static void compareBaseFiles(DiffTally tally, String partitionPath,
                                       FileSlice mdtSlice, FileSlice fsSlice) {
    if (HoodieMetadataValidationUtils.assertBaseFilesEquality(mdtSlice, fsSlice)) {
      return;
    }
    boolean mdtHasBaseFile = mdtSlice.getBaseFile().isPresent();
    boolean fsHasBaseFile = fsSlice.getBaseFile().isPresent();
    if (fsHasBaseFile && !mdtHasBaseFile) {
      addBaseFile(tally, MismatchPattern.FS_EXCLUSIVE_BASE_FILE, fsSlice, partitionPath,
          "the metadata table's slice for this file group has no base file");
      return;
    }
    if (mdtHasBaseFile && !fsHasBaseFile) {
      addBaseFile(tally, MismatchPattern.MDT_EXCLUSIVE_BASE_FILE, mdtSlice, partitionPath,
          "the storage listing's slice for this file group has no base file");
      return;
    }
    HoodieBaseFile mdtBaseFile = mdtSlice.getBaseFile().get();
    HoodieBaseFile fsBaseFile = fsSlice.getBaseFile().get();
    if (!mdtBaseFile.getFileName().equals(fsBaseFile.getFileName())) {
      // Different names for one slice: emit only the storage-side record. Each side's view keeps one
      // of a task retry's duplicate base files, so the MDT file is usually still on storage and an
      // MDT_EXCLUSIVE_* record (read downstream as "rebuild the MDT") would be false. Phase 2 settles
      // the slice from the writing instant's commit metadata, which matches on exact file name; the
      // MDT's file name stays visible in the detail. Accepted gap: a real deletion of the committed
      // file in a slice that also holds a retry orphan reports DANGLING on the orphan, not
      // OUTBOUND_DELETION.
      addBaseFile(tally, MismatchPattern.FS_EXCLUSIVE_BASE_FILE, fsSlice, partitionPath,
          "the metadata table names " + mdtBaseFile.getFileName() + " for this slice");
      return;
    }
    FileDiffRecord.Builder builder = baseFileRecordBuilder(MismatchPattern.BASE_FILE_ATTRIBUTE_MISMATCH,
        fsSlice, fsBaseFile, partitionPath)
        .withSizes(fsBaseFile.getFileSize(), mdtBaseFile.getFileSize())
        .withSizeBytes(null)
        .withDetail("same base file name on both sides; fileId or length differs (storage fileId="
            + fsBaseFile.getFileId() + ", metadata table fileId=" + mdtBaseFile.getFileId() + ")");
    if (attributeBaseFileInstant(builder, MismatchPattern.BASE_FILE_ATTRIBUTE_MISMATCH,
        fsBaseFile.getFileName(), tally)) {
      tally.add(builder.build());
    }
  }

  private static void compareLogFiles(DiffTally tally, String partitionPath,
                                      FileSlice mdtSlice, FileSlice fsSlice,
                                      LogFileReader logFileReader) {
    Map<String, HoodieLogFile> mdtLogFiles = byFileName(mdtSlice);
    Map<String, HoodieLogFile> fsLogFiles = byFileName(fsSlice);
    for (Map.Entry<String, HoodieLogFile> entry : fsLogFiles.entrySet()) {
      if (mdtLogFiles.containsKey(entry.getKey())) {
        continue;
      }
      addLogFile(tally, MismatchPattern.FS_EXCLUSIVE_LOG_FILE, fsSlice, entry.getValue(),
          partitionPath, logFileReader,
          "log file is on storage and not in the metadata table's slice");
    }
    for (Map.Entry<String, HoodieLogFile> entry : mdtLogFiles.entrySet()) {
      if (fsLogFiles.containsKey(entry.getKey())) {
        continue;
      }
      addLogFile(tally, MismatchPattern.MDT_EXCLUSIVE_LOG_FILE, mdtSlice, entry.getValue(),
          partitionPath, logFileReader,
          "log file is in the metadata table's slice and not in the storage listing");
    }
  }

  /** Every file of one slice, reported under the same pattern. */
  private static void addSliceFiles(DiffTally tally, MismatchPattern pattern,
                                    FileSlice slice, String partitionPath,
                                    LogFileReader logFileReader, String detail) {
    if (slice.getBaseFile().isPresent()) {
      addBaseFile(tally, pattern, slice, partitionPath, detail);
    }
    slice.getLogFiles().forEach(logFile -> addLogFile(tally, pattern, slice, logFile, partitionPath,
        logFileReader, detail));
  }

  private static void addBaseFile(DiffTally tally, MismatchPattern pattern,
                                  FileSlice slice, String partitionPath, String detail) {
    HoodieBaseFile baseFile = slice.getBaseFile().get();
    FileDiffRecord.Builder builder = baseFileRecordBuilder(pattern, slice, baseFile, partitionPath)
        .withDetail(detail);
    if (attributeBaseFileInstant(builder, pattern, baseFile.getFileName(), tally)) {
      tally.add(builder.build());
    }
  }

  private static void addLogFile(DiffTally tally, MismatchPattern pattern, FileSlice slice,
                                 HoodieLogFile logFile, String partitionPath,
                                 LogFileReader logFileReader, String detail) {
    FileDiffRecord.Builder builder = logFileRecordBuilder(pattern, slice, logFile, partitionPath)
        .withDetail(detail);
    if (pattern.getSide() != MismatchSide.FS) {
      // Not on storage, so there is no block to read an instant out of.
      tally.add(builder.withWritingInstant(null, MismatchRecord.InstantSource.NOT_APPLICABLE).build());
      return;
    }
    if (attributeFsExclusiveLogFileInstant(builder, logFileReader, partitionPath, logFile, tally)) {
      tally.add(builder.build());
    }
  }

  private static FileDiffRecord.Builder baseFileRecordBuilder(MismatchPattern pattern, FileSlice slice,
                                                               HoodieBaseFile baseFile,
                                                               String partitionPath) {
    return FileDiffRecord.newBuilder()
        .withPartition(partitionPath)
        .withDiffPattern(DiffPattern.of(pattern.getSide()))
        .withSourcePattern(pattern)
        .withFileGroupId(slice.getFileId())
        .withBaseInstantTime(slice.getBaseInstantTime())
        .withFileName(baseFile.getFileName())
        .withFileType(MismatchRecord.FileType.BASE)
        .withSizeBytes(baseFile.getFileSize());
  }

  private static FileDiffRecord.Builder logFileRecordBuilder(MismatchPattern pattern, FileSlice slice,
                                                              HoodieLogFile logFile,
                                                              String partitionPath) {
    return FileDiffRecord.newBuilder()
        .withPartition(partitionPath)
        .withDiffPattern(DiffPattern.of(pattern.getSide()))
        .withSourcePattern(pattern)
        .withFileGroupId(slice.getFileId())
        .withBaseInstantTime(slice.getBaseInstantTime())
        .withFileName(logFile.getFileName())
        .withFileType(MismatchRecord.FileType.LOG)
        .withSizeBytes(logFile.getFileSize());
  }

  /**
   * Attributes the writing instant from the base file's name. An unparseable name is dropped and
   * counted on the {@code FS} side, which {@link FileDiffAdjudicator} could not adjudicate without an
   * instant; on other sides it is still reported with the reason, since dropping it would lose the
   * finding.
   *
   * @return {@code true} if the record should be added to the tally, {@code false} if it was
   *     dropped and counted instead
   */
  private static boolean attributeBaseFileInstant(FileDiffRecord.Builder builder,
                                                  MismatchPattern pattern, String fileName,
                                                  DiffTally tally) {
    try {
      String instant = FSUtils.getCommitTime(fileName);
      if (!isHudiGeneratedBaseFileName(fileName, instant)) {
        throw new IllegalArgumentException(
            "base file name " + fileName + " does not name an instant this validator can adjudicate");
      }
      builder.withWritingInstant(instant, MismatchRecord.InstantSource.FILE_NAME);
      return true;
    } catch (Exception e) {
      LOG.warn("Could not parse an instant out of base file name {}", fileName, e);
      if (pattern.getSide() == MismatchSide.FS) {
        tally.countDrop(MismatchRecord.InstantUnresolvedReason.NAME_NOT_PARSEABLE.name());
        return false;
      }
      builder.withUnresolvedInstant(MismatchRecord.InstantUnresolvedReason.NAME_NOT_PARSEABLE);
      return true;
    }
  }

  /** Require a write token and a numeric instant, beyond what the permissive parser accepts. */
  private static boolean isHudiGeneratedBaseFileName(String fileName, String commitTime) {
    if (commitTime == null || commitTime.isEmpty()) {
      return false;
    }
    for (int i = 0; i < commitTime.length(); i++) {
      if (!Character.isDigit(commitTime.charAt(i))) {
        return false;
      }
    }
    return fileName.indexOf('_') != fileName.lastIndexOf('_');
  }

  /**
   * Reads an FS-exclusive log file's writing instant from its first block header, since below table
   * version 8 a log file's name carries only its slice's base commit time. Dropped and counted: a
   * {@link HoodieLogBlock.HoodieLogBlockType#COMMAND_BLOCK} (a rollback marker, which must never be
   * adjudicated because deleting it resurrects rolled-back data) and an unreadable instant.
   *
   * @return {@code true} if the record should be added to the tally, {@code false} if it was
   *     dropped and counted instead
   */
  private static boolean attributeFsExclusiveLogFileInstant(FileDiffRecord.Builder builder,
                                                            LogFileReader logFileReader,
                                                            String partitionPath, HoodieLogFile logFile,
                                                            DiffTally tally) {
    LogFileInstantReader.Result result = logFileReader.read(partitionPath, logFile.getFileName());
    if (result.isCommandBlock()) {
      tally.countDrop(HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK.name());
      return false;
    }
    if (!result.isResolved()) {
      tally.countDrop(result.getReason().name());
      return false;
    }
    builder.withWritingInstant(result.getInstantTime(), MismatchRecord.InstantSource.LOG_BLOCK_HEADER)
        .withLogBlockType(result.getBlockType());
    return true;
  }

  /** One slice per file group; a second slice for a group is unexpected, so keep the first and warn. */
  private static Map<String, FileSlice> byFileGroup(List<FileSlice> slices, String side) {
    Map<String, FileSlice> byFileGroup = new LinkedHashMap<>();
    for (FileSlice slice : slices) {
      FileSlice previous = byFileGroup.putIfAbsent(slice.getFileId(), slice);
      if (previous != null) {
        LOG.warn("The {} listing holds more than one slice for file group {} ({} and {}); "
                + "comparing against the first", side, slice.getFileId(),
            previous.getBaseInstantTime(), slice.getBaseInstantTime());
      }
    }
    return byFileGroup;
  }

  private static Map<String, HoodieLogFile> byFileName(FileSlice slice) {
    return slice.getLogFiles().collect(Collectors.toMap(HoodieLogFile::getFileName,
        logFile -> logFile, (first, second) -> first, LinkedHashMap::new));
  }
}
