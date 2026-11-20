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

import org.apache.hudi.common.table.log.block.HoodieLogBlock;

import java.io.Serializable;
import java.util.Objects;

/**
 * One file the metadata table and the storage listing disagree about, before and after adjudication.
 *
 * <p>Serialised field by field ({@code JsonUtils} reads fields, not getters), so this class is the
 * written contract for {@code fs_mdt_diff/}. Add fields freely; never rename one, and remove one
 * only once no consumer of the documents reads it.
 *
 * <p>Deliberately distinct from {@link MismatchRecord}, which is unchanged and still serves the
 * dangling-file dimension.
 */
public class FileDiffRecord implements Serializable, Comparable<FileDiffRecord> {

  private static final long serialVersionUID = 1L;

  private String partition;
  private String fileName;
  private String fileGroupId;
  private String baseInstantTime;
  private MismatchRecord.FileType fileType;
  private DiffPattern diffPattern;
  /** The {@link MismatchPattern} this collapsed from. For the reader; nothing switches on it. */
  private MismatchPattern sourcePattern;
  private Long sizeBytes;
  private Long fsSizeBytes;
  private Long mdtSizeBytes;
  /**
   * The instant that WROTE the file. For a base file, parsed from the name. For a log file, read
   * from the first log block's {@code INSTANT_TIME} header — never from the name, which below table
   * version 8 carries the slice's base commit instead.
   */
  private String writingInstant;
  private MismatchRecord.InstantSource instantSource;
  private MismatchRecord.InstantUnresolvedReason instantUnresolvedReason;
  private HoodieLogBlock.HoodieLogBlockType logBlockType;
  private Verdict verdict;
  private String verdictDetail;
  private String detail;

  /** Jackson, and nothing else. */
  public FileDiffRecord() {
  }

  private FileDiffRecord(Builder builder) {
    this.partition = builder.partition;
    this.fileName = builder.fileName;
    this.fileGroupId = builder.fileGroupId;
    this.baseInstantTime = builder.baseInstantTime;
    this.fileType = builder.fileType;
    this.diffPattern = builder.diffPattern;
    this.sourcePattern = builder.sourcePattern;
    this.sizeBytes = builder.sizeBytes;
    this.fsSizeBytes = builder.fsSizeBytes;
    this.mdtSizeBytes = builder.mdtSizeBytes;
    this.writingInstant = builder.writingInstant;
    this.instantSource = builder.instantSource;
    this.instantUnresolvedReason = builder.instantUnresolvedReason;
    this.logBlockType = builder.logBlockType;
    this.verdict = builder.verdict;
    this.verdictDetail = builder.verdictDetail;
    this.detail = builder.detail;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** A copy carrying the verdict. The source record is untouched, so phase 1 output stays immutable. */
  public FileDiffRecord withVerdict(Verdict decided, String why) {
    Builder builder = toBuilder();
    builder.verdict = decided;
    builder.verdictDetail = why;
    return new FileDiffRecord(builder);
  }

  private Builder toBuilder() {
    Builder builder = new Builder();
    builder.partition = partition;
    builder.fileName = fileName;
    builder.fileGroupId = fileGroupId;
    builder.baseInstantTime = baseInstantTime;
    builder.fileType = fileType;
    builder.diffPattern = diffPattern;
    builder.sourcePattern = sourcePattern;
    builder.sizeBytes = sizeBytes;
    builder.fsSizeBytes = fsSizeBytes;
    builder.mdtSizeBytes = mdtSizeBytes;
    builder.writingInstant = writingInstant;
    builder.instantSource = instantSource;
    builder.instantUnresolvedReason = instantUnresolvedReason;
    builder.logBlockType = logBlockType;
    builder.verdict = verdict;
    builder.verdictDetail = verdictDetail;
    builder.detail = detail;
    return builder;
  }

  public String getPartition() {
    return partition;
  }

  public String getFileName() {
    return fileName;
  }

  public String getFileGroupId() {
    return fileGroupId;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public MismatchRecord.FileType getFileType() {
    return fileType;
  }

  public DiffPattern getDiffPattern() {
    return diffPattern;
  }

  public MismatchPattern getSourcePattern() {
    return sourcePattern;
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

  public String getWritingInstant() {
    return writingInstant;
  }

  public MismatchRecord.InstantSource getInstantSource() {
    return instantSource;
  }

  public MismatchRecord.InstantUnresolvedReason getInstantUnresolvedReason() {
    return instantUnresolvedReason;
  }

  public HoodieLogBlock.HoodieLogBlockType getLogBlockType() {
    return logBlockType;
  }

  public Verdict getVerdict() {
    return verdict;
  }

  public String getVerdictDetail() {
    return verdictDetail;
  }

  public String getDetail() {
    return detail;
  }

  /**
   * A total, deterministic order over what identifies a FINDING: where the file is, what it is
   * called, and which difference is being reported about it.
   *
   * <p>Phase 2 shuffles records, so this is the ordering {@code repartitionAndSortWithinPartitions}
   * applies inside {@code InstantRangeBalancer.repartition} -- which is where the
   * {@code Comparable} bound comes from. Two runs over an unchanged table must repartition
   * identically, so it may not depend on anything the run itself decides: not the verdict, not a
   * size, not a hash.
   *
   * <p>{@code diffPattern} is the third key rather than a tiebreak. {@code (partition, fileName)}
   * alone identifies a FILE, and the same file can be reported by more than one pattern; two
   * findings that compared equal would be indistinguishable to the sort.
   *
   * <p>Null-safe on every field, and deliberately so rather than defensively: this comparator runs
   * inside a Spark shuffle, where a {@code NullPointerException} fails the whole stage rather than
   * one record. Nulls sort first.
   */
  @Override
  public int compareTo(FileDiffRecord other) {
    int byPartition = compareNullable(partition, other.partition);
    if (byPartition != 0) {
      return byPartition;
    }
    int byFileName = compareNullable(fileName, other.fileName);
    if (byFileName != 0) {
      return byFileName;
    }
    return compareNullable(diffPattern == null ? null : diffPattern.name(),
        other.diffPattern == null ? null : other.diffPattern.name());
  }

  private static int compareNullable(String mine, String theirs) {
    if (mine == null) {
      return theirs == null ? 0 : -1;
    }
    return theirs == null ? 1 : mine.compareTo(theirs);
  }

  /**
   * Equal exactly when {@link #compareTo} returns zero, which {@code Comparable} asks for and which
   * is meaningful here rather than merely conformant: the three keys it orders on are what identify
   * a finding, one file reported under one pattern. The remaining fields describe that finding; they
   * do not distinguish one from another.
   *
   * <p>Note the contrast with {@link MismatchRecord}, whose {@code compareTo} is a coarser shuffle
   * key over {@code (partition, fileName)} alone and deliberately lets two distinct findings compare
   * equal. Matching that one's equals to its ordering would conflate them, so it is left alone.
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FileDiffRecord)) {
      return false;
    }
    return compareTo((FileDiffRecord) other) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, fileName, diffPattern == null ? null : diffPattern.name());
  }

  /** Builder. Every setter returns {@code this}. */
  public static class Builder {
    private String partition;
    private String fileName;
    private String fileGroupId;
    private String baseInstantTime;
    private MismatchRecord.FileType fileType;
    private DiffPattern diffPattern;
    private MismatchPattern sourcePattern;
    private Long sizeBytes;
    private Long fsSizeBytes;
    private Long mdtSizeBytes;
    private String writingInstant;
    private MismatchRecord.InstantSource instantSource;
    private MismatchRecord.InstantUnresolvedReason instantUnresolvedReason;
    private HoodieLogBlock.HoodieLogBlockType logBlockType;
    private Verdict verdict;
    private String verdictDetail;
    private String detail;

    public Builder withPartition(String value) {
      this.partition = value;
      return this;
    }

    public Builder withFileName(String value) {
      this.fileName = value;
      return this;
    }

    public Builder withFileGroupId(String value) {
      this.fileGroupId = value;
      return this;
    }

    public Builder withBaseInstantTime(String value) {
      this.baseInstantTime = value;
      return this;
    }

    public Builder withFileType(MismatchRecord.FileType value) {
      this.fileType = value;
      return this;
    }

    public Builder withDiffPattern(DiffPattern value) {
      this.diffPattern = value;
      return this;
    }

    public Builder withSourcePattern(MismatchPattern value) {
      this.sourcePattern = value;
      return this;
    }

    public Builder withSizeBytes(Long value) {
      this.sizeBytes = value;
      return this;
    }

    public Builder withSizes(Long fsSize, Long mdtSize) {
      this.fsSizeBytes = fsSize;
      this.mdtSizeBytes = mdtSize;
      return this;
    }

    public Builder withWritingInstant(String instant, MismatchRecord.InstantSource source) {
      this.writingInstant = instant;
      this.instantSource = source;
      return this;
    }

    public Builder withUnresolvedInstant(MismatchRecord.InstantUnresolvedReason reason) {
      this.writingInstant = null;
      this.instantSource = MismatchRecord.InstantSource.UNRESOLVED;
      this.instantUnresolvedReason = reason;
      return this;
    }

    public Builder withLogBlockType(HoodieLogBlock.HoodieLogBlockType value) {
      this.logBlockType = value;
      return this;
    }

    public Builder withDetail(String value) {
      this.detail = value;
      return this;
    }

    public FileDiffRecord build() {
      return new FileDiffRecord(this);
    }
  }
}
