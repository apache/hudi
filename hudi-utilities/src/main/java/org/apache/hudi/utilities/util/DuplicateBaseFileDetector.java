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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Finds duplicate base files in one partition's raw storage listing.
 *
 * <p><b>Cardinality decides the run's verdict.</b> A file slice holding two or more base files is
 * a broken invariant &mdash; a Spark task retry writes a second base file for the same slice under a
 * new write token, and the commit records only one of them &mdash; so every member of that set is
 * emitted with {@code failsValidation = true}. That verdict does not depend on anything the timeline
 * might later say: it is decided from the listing alone, before any adjudication happens.
 *
 * <p><b>Base files only.</b> This detector is named for the one invariant a raw listing can settle
 * by itself, and it reports nothing else. Log files are not examined, not counted and not emitted:
 * a log file's presence breaks no cardinality rule, so a listing alone can say nothing about one.
 * Every question about a log file &mdash; is it referenced, what wrote it, is it an orphan &mdash;
 * belongs to the FS-MDT diff dimension, which compares the two sides rather than reading one.
 *
 * <p><b>Whether a duplicate is tolerated is not decided here.</b> A retried Spark task writes its
 * second attempt under a new write token ({@code FSUtils.makeWriteToken} carries the attempt id), so
 * from the retry until {@code HoodieTable.finalizeWrite} reconciles against markers an
 * <em>inflight</em> commit can have exactly the shape this detector looks for without the table being
 * broken. Telling that case apart from a duplicate under an instant with no file anywhere &mdash; the
 * finding this detector exists to catch &mdash; needs two timeline reads bracketing the run, not one,
 * so that question now belongs entirely to {@link DuplicateBaseFileDimension}'s classify stage and
 * {@link DuplicateInstantWindow}, once phase 1's grouping is done. This class reads no timeline at
 * grouping time at all: {@code containsOrBeforeTimelineStarts} &mdash; the same check
 * {@code HoodieFileGroup.isFileSliceCommitted}, {@code RepairUtils.findInstantFilesToRemove} and
 * {@code HoodieDataTableValidator} use &mdash; is unrelated to this class and always was; it is not
 * used here.
 *
 * <p>Grouping keys and instants come from {@link HoodieBaseFile}. A path accepted by
 * {@link FSUtils#isBaseFile} still needs a generated base-file name before it can join a slice:
 * the parser also accepts single-underscore names that Hudi did not generate.
 */
public final class DuplicateBaseFileDetector {

  private static final Logger LOG = LoggerFactory.getLogger(DuplicateBaseFileDetector.class);

  private DuplicateBaseFileDetector() {
  }

  /**
   * @param partitionFiles the raw storage listing for one partition
   * @param partitionPath  relative partition path, empty for a non-partitioned table
   * @return every member of every file slice that holds more than one base file
   */
  public static List<MismatchRecord> detectSuspects(List<StoragePathInfo> partitionFiles,
                                                    String partitionPath) {
    AtomicLong droppedNames = new AtomicLong();
    List<MismatchRecord> suspects = detectSuspects(partitionFiles, partitionPath, droppedNames);
    if (droppedNames.get() > 0) {
      LOG.warn("Dropped {} non-generated base-file names from duplicate check in partition {}",
          droppedNames.get(), partitionPath);
    }
    return suspects;
  }

  static List<MismatchRecord> detectSuspects(List<StoragePathInfo> partitionFiles,
                                             String partitionPath, AtomicLong droppedNames) {
    List<MismatchRecord> suspects = new ArrayList<>();
    for (List<HoodieBaseFile> duplicateSet : baseFilesBySlice(partitionFiles, droppedNames).values()) {
      if (duplicateSet.size() < 2) {
        BranchBeacon.hit(Branch.DUP_SLICE_HOLDS_ONE_BASE_FILE);
        continue;
      }
      BranchBeacon.hit(Branch.DUP_SLICE_CARDINALITY_FINDING);
      // Sorted so that two runs over an unchanged table produce byte-identical reports.
      duplicateSet.sort(Comparator.comparing(HoodieBaseFile::getFileName));
      for (HoodieBaseFile baseFile : duplicateSet) {
        suspects.add(duplicateBaseFileSuspect(baseFile, duplicateSet.size(), partitionPath));
      }
    }
    return suspects;
  }

  /**
   * The listing's base files, grouped by the slice they belong to.
   *
   * <p>Keyed by {@code fileId + ":" + commitTime}, the file-slice identity, so a count of keys is a
   * count of slices. Insertion ordered, so the grouping does not reorder what the listing returned.
   */
  private static Map<String, List<HoodieBaseFile>> baseFilesBySlice(
      List<StoragePathInfo> partitionFiles, AtomicLong droppedNames) {
    Map<String, List<HoodieBaseFile>> baseFilesBySlice = new LinkedHashMap<>();
    for (StoragePathInfo pathInfo : partitionFiles) {
      StoragePath path = pathInfo.getPath();
      if (!FSUtils.isBaseFile(path)) {
        continue;
      }
      HoodieBaseFile baseFile = new HoodieBaseFile(pathInfo);
      if (!isHudiGeneratedBaseFileName(baseFile.getFileName(), baseFile.getCommitTime())) {
        droppedNames.incrementAndGet();
        continue;
      }
      String key = baseFile.getFileId() + ":" + baseFile.getCommitTime();
      baseFilesBySlice.computeIfAbsent(key, ignored -> new ArrayList<>()).add(baseFile);
    }
    return baseFilesBySlice;
  }

  /** Keep the generated-name boundary used by the later FS-MDT analyzer on strict lines. */
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

  private static MismatchRecord duplicateBaseFileSuspect(HoodieBaseFile baseFile,
                                                         int duplicateSetSize,
                                                         String partitionPath) {
    return MismatchRecord.newBuilder()
        .withPartition(partitionPath)
        .withPattern(MismatchPattern.DANGLING_FILE_SUSPECTS)
        .withFileGroupId(baseFile.getFileId())
        .withBaseInstantTime(baseFile.getCommitTime())
        .withFileName(baseFile.getFileName())
        .withFileType(MismatchRecord.FileType.BASE)
        .withSizeBytes(baseFile.getFileSize())
        .withInstant(baseFile.getCommitTime(), MismatchRecord.InstantSource.FILE_NAME)
        .withSuspectReason(MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE)
        .withDuplicateSet(duplicateSetSize, null)
        .withFailsValidation(true)
        .withDetail("file slice holds " + duplicateSetSize + " base files; the timeline has not "
            + "been consulted, so every member is reported as a suspect")
        .build();
  }
}
