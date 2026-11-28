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

import java.io.Serializable;

/**
 * One file the metadata table and the storage listing disagree about.
 *
 * <p>The unit is a <b>file</b>, not a file slice: a slice-grained finding cannot be handed to
 * anything that acts on files, and every consumer of this report — starting with the dangling-file
 * adjudication that will read it — asks its questions one file at a time.
 *
 * <p>Fields are serialized as they stand ({@code JsonUtils} reads fields, not getters), so this
 * class is the written contract. Add fields; do not rename them.
 */
public class MismatchRecord implements Serializable, Comparable<MismatchRecord> {

  private static final long serialVersionUID = 1L;

  /** Whether the file is a base file or a log file. */
  public enum FileType {
    BASE, LOG
  }

  /**
   * Where {@link MismatchRecord#instantTime} came from.
   *
   * <p>A base file's name carries the instant that wrote it. A log file's name does not reliably
   * name the delta commit that wrote it — below table version 8 it carries the base commit time of
   * the slice it belongs to — so its instant is read out of the first log block's header instead.
   */
  public enum InstantSource {
    /** Parsed from the file name; base files only. */
    FILE_NAME,
    /** Read from the first log block's {@code INSTANT_TIME} header. */
    LOG_BLOCK_HEADER,
    /** The instant could not be established. See {@link MismatchRecord#instantUnresolvedReason}. */
    UNRESOLVED,
    /** Not a question that can be asked: the file is not on storage to be read. */
    NOT_APPLICABLE
  }

  /** Why an instant could not be established. Only ever set alongside {@link InstantSource#UNRESOLVED}. */
  public enum InstantUnresolvedReason {
    /** The log file held no block to read a header from. */
    NO_LOG_BLOCK,
    /** The file could not be opened or read at all. */
    UNREADABLE,
    /** A block was read, but it carried no {@code INSTANT_TIME} header. */
    NO_INSTANT_HEADER,
    /** The file name does not follow the convention the instant would be parsed from. */
    NAME_NOT_PARSEABLE
  }

  /** Why the dangling dimension flagged a file. Set on suspect and dangling records alike. */
  public enum SuspectReason {
    /**
     * The file's slice holds more than one base file. A base file is
     * {@code <fileId>_<writeToken>_<instant>.<ext>}; a Spark task retry writes a second one for the
     * same slice under a new token, and the commit records only one of them. One per slice is an
     * invariant, so this fails the run whatever the timeline later says.
     */
    DUPLICATE_BASE_FILE,
    /**
     * A log file the timeline does not obviously vouch for. Its name does not reliably name the delta
     * commit that wrote it (below table version 8 it carries the slice's base commit), so
     * establishing the writing instant costs a read of the first log block's header — which is why
     * this reason is exhaustive-mode only.
     */
    UNREFERENCED_LOG_FILE
  }

  private String partition;
  private MismatchPattern pattern;
  private MismatchSide side;
  private String fileGroupId;
  private String baseInstantTime;
  private String fileName;
  private FileType fileType;
  private Long sizeBytes;
  private Long fsSizeBytes;
  private Long mdtSizeBytes;
  private String instantTime;
  private InstantSource instantSource;
  private InstantUnresolvedReason instantUnresolvedReason;
  /** Set on dangling-dimension records only. */
  private SuspectReason suspectReason;
  /**
   * How many base files storage holds for this slice, this one included. Null for a log file.
   * Carried because a reader must be able to see the set from any one of its members.
   */
  private Integer duplicateSetSize;
  /**
   * <b>Always {@code null}.</b> Retained as a field of the written contract, not as a value anyone
   * may read.
   *
   * <p>It held {@code ExpectedBaseFileResolver}'s answer — which member of a duplicate base-file
   * set the commit named, the <em>keeper</em>. That resolver was deleted, and deliberately: naming
   * a keeper is a question this tool does not ask. The adjudication asks the reverse one, of one
   * file at a time — <em>is this file referenced?</em> — which is answerable when "which is the
   * keeper?" is not, and which is the only question that can license a delete
   * ({@code DanglingAdjudicator}). Both writers pass {@code null}
   * ({@code DuplicateBaseFileDetector}), and the phase-2 promotion in
   * {@code DuplicateBaseFileDimension} only copies whatever it is handed.
   *
   * <p>Not removed, because this class is the schema of a report parsed outside this repository and
   * {@code JsonUtils} serializes null fields, so the key is present in every record written so far
   * and dropping it changes the document's shape. Do not start populating it without reintroducing
   * something that can answer the question it names — a consumer told which file is the keeper is a
   * consumer one step from deleting the others.
   */
  private String expectedFileName;
  /**
   * Whether this finding is why the partition fails validation.
   *
   * <p>False for exactly one shape: a differing log file the timeline shows as uncommitted — the
   * Spark-retry orphan the validator has always tolerated. It is recorded because an unaccounted
   * log file on storage is precisely what a dangling-file adjudication is looking for, and
   * suppressed from the verdict because reporting it as a failure would fail tables that pass
   * today.
   */
  private boolean failsValidation;
  private String detail;
  /**
   * What the timeline said about this file, as a {@code DanglingAdjudicator.Outcome} name.
   *
   * <p>Set only on records the reconciliation pass actually asked about: FS-side records that
   * survived it. Null everywhere else — on a record from a raw report, on an {@code MDT_EXCLUSIVE_*}
   * record (asking whether a file that is not on storage is a storage orphan is a category error),
   * and on a {@code DANGLING_FILES} record, whose pattern already carries the answer.
   *
   * <p><b>Carries no delete authority.</b> A survivor is never deletable whatever this says; the
   * only thing a consumer may act on destructively is {@link MismatchPattern#DANGLING_FILES}. This
   * field distinguishes "the metadata table lost a real file" (rebuild) from "the timeline could not
   * answer" (refuse, and have a human look).
   *
   * <p>A {@code String} rather than the enum on purpose: this class is the written contract, and a
   * reader must be able to refuse an outcome it does not recognise rather than fail to parse the
   * document at all.
   */
  private String adjudicationOutcome;

  /** Jackson, and nothing else. */
  public MismatchRecord() {
  }

  private MismatchRecord(Builder builder) {
    this.partition = builder.partition;
    this.pattern = builder.pattern;
    this.side = builder.pattern == null ? null : builder.pattern.getSide();
    this.fileGroupId = builder.fileGroupId;
    this.baseInstantTime = builder.baseInstantTime;
    this.fileName = builder.fileName;
    this.fileType = builder.fileType;
    this.sizeBytes = builder.sizeBytes;
    this.fsSizeBytes = builder.fsSizeBytes;
    this.mdtSizeBytes = builder.mdtSizeBytes;
    this.instantTime = builder.instantTime;
    this.instantSource = builder.instantSource;
    this.instantUnresolvedReason = builder.instantUnresolvedReason;
    this.suspectReason = builder.suspectReason;
    this.duplicateSetSize = builder.duplicateSetSize;
    this.expectedFileName = builder.expectedFileName;
    this.failsValidation = builder.failsValidation;
    this.detail = builder.detail;
    this.adjudicationOutcome = builder.adjudicationOutcome;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public String getPartition() {
    return partition;
  }

  public MismatchPattern getPattern() {
    return pattern;
  }

  public MismatchSide getSide() {
    return side;
  }

  public String getFileGroupId() {
    return fileGroupId;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public String getFileName() {
    return fileName;
  }

  public FileType getFileType() {
    return fileType;
  }

  public Long getSizeBytes() {
    return sizeBytes;
  }

  public Long getFsSizeBytes() {
    return fsSizeBytes;
  }

  public Long getMdtSizeBytes() {
    return mdtSizeBytes;
  }

  public String getInstantTime() {
    return instantTime;
  }

  public InstantSource getInstantSource() {
    return instantSource;
  }

  public InstantUnresolvedReason getInstantUnresolvedReason() {
    return instantUnresolvedReason;
  }

  public SuspectReason getSuspectReason() {
    return suspectReason;
  }

  public Integer getDuplicateSetSize() {
    return duplicateSetSize;
  }

  public String getExpectedFileName() {
    return expectedFileName;
  }

  public boolean isFailsValidation() {
    return failsValidation;
  }

  public String getDetail() {
    return detail;
  }

  public String getAdjudicationOutcome() {
    return adjudicationOutcome;
  }

  /**
   * A total, deterministic order over the two fields that identify a file: where it is, and what it
   * is called.
   *
   * <p>Phase 2 shuffles records rather than reports, so this is the ordering
   * {@code repartitionAndSortWithinPartitions} applies inside
   * {@code InstantRangeBalancer.repartition} -- which is where the {@code Comparable} bound comes
   * from. Two runs over an unchanged table must repartition identically, so it may not depend on
   * anything the run itself decides.
   *
   * <p>Null-safe on both fields, and deliberately so rather than defensively: this comparator runs
   * inside a Spark shuffle, where a {@code NullPointerException} fails the whole stage rather than
   * one record. Nulls sort first.
   */
  @Override
  public int compareTo(MismatchRecord other) {
    int byPartition = compareNullable(partition, other.partition);
    if (byPartition != 0) {
      return byPartition;
    }
    return compareNullable(fileName, other.fileName);
  }

  private static int compareNullable(String mine, String theirs) {
    if (mine == null) {
      return theirs == null ? 0 : -1;
    }
    return theirs == null ? 1 : mine.compareTo(theirs);
  }

  @Override
  public String toString() {
    return "MismatchRecord{partition=" + partition + ", pattern=" + pattern
        + ", fileGroupId=" + fileGroupId + ", baseInstantTime=" + baseInstantTime
        + ", fileName=" + fileName + ", instantTime=" + instantTime
        + ", instantSource=" + instantSource + ", suspectReason=" + suspectReason
        + ", failsValidation=" + failsValidation + ", adjudicationOutcome=" + adjudicationOutcome
        + '}';
  }

  /** Builder, because a fifteen-argument constructor is unreadable at the call site. */
  public static class Builder {

    private String partition;
    private MismatchPattern pattern;
    private String fileGroupId;
    private String baseInstantTime;
    private String fileName;
    private FileType fileType;
    private Long sizeBytes;
    private Long fsSizeBytes;
    private Long mdtSizeBytes;
    private String instantTime;
    private InstantSource instantSource = InstantSource.NOT_APPLICABLE;
    private InstantUnresolvedReason instantUnresolvedReason;
    private SuspectReason suspectReason;
    private Integer duplicateSetSize;
    private String expectedFileName;
    private boolean failsValidation = true;
    private String detail = "";
    private String adjudicationOutcome;

    public Builder withPartition(String partition) {
      this.partition = partition;
      return this;
    }

    public Builder withPattern(MismatchPattern pattern) {
      this.pattern = pattern;
      return this;
    }

    public Builder withFileGroupId(String fileGroupId) {
      this.fileGroupId = fileGroupId;
      return this;
    }

    public Builder withBaseInstantTime(String baseInstantTime) {
      this.baseInstantTime = baseInstantTime;
      return this;
    }

    public Builder withFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public Builder withFileType(FileType fileType) {
      this.fileType = fileType;
      return this;
    }

    public Builder withSizeBytes(Long sizeBytes) {
      this.sizeBytes = sizeBytes;
      return this;
    }

    public Builder withSizes(Long fsSizeBytes, Long mdtSizeBytes) {
      this.fsSizeBytes = fsSizeBytes;
      this.mdtSizeBytes = mdtSizeBytes;
      return this;
    }

    public Builder withInstant(String instantTime, InstantSource instantSource) {
      this.instantTime = instantTime;
      this.instantSource = instantSource;
      return this;
    }

    public Builder withUnresolvedInstant(InstantUnresolvedReason reason) {
      this.instantTime = null;
      this.instantSource = InstantSource.UNRESOLVED;
      this.instantUnresolvedReason = reason;
      return this;
    }

    public Builder withSuspectReason(SuspectReason suspectReason) {
      this.suspectReason = suspectReason;
      return this;
    }

    /**
     * @param expectedFileName always {@code null} — see {@link MismatchRecord#expectedFileName};
     *                         the parameter survives because the field is part of the written
     *                         contract, not because anything may set it
     */
    public Builder withDuplicateSet(Integer duplicateSetSize, String expectedFileName) {
      this.duplicateSetSize = duplicateSetSize;
      this.expectedFileName = expectedFileName;
      return this;
    }

    public Builder withFailsValidation(boolean failsValidation) {
      this.failsValidation = failsValidation;
      return this;
    }

    public Builder withDetail(String detail) {
      this.detail = detail;
      return this;
    }

    public Builder withAdjudicationOutcome(String adjudicationOutcome) {
      this.adjudicationOutcome = adjudicationOutcome;
      return this;
    }

    public MismatchRecord build() {
      return new MismatchRecord(this);
    }
  }
}
