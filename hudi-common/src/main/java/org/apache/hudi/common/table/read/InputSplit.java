/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a split of input data for reading, which includes the partition path the data belongs to along with an optional base file and a list of log files.
 * If there is only a base file, it is possible for the reader to specify a particular range of the file with the start and length parameters.
 */
@Getter
public class InputSplit {

  private final Option<HoodieBaseFile> baseFileOption;
  @Getter(AccessLevel.NONE)
  private final List<HoodieLogFile> logFiles;
  @Getter(AccessLevel.NONE)
  private final Option<Iterator<HoodieRecord>> recordIterator;
  private final String partitionPath;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;

  @Builder
  private InputSplit(
      Option<HoodieBaseFile> baseFileOption,
      Stream<HoodieLogFile> logFileStream,
      Iterator<HoodieRecord> recordIterator,
      String partitionPath,
      long start,
      long length) {

    // Ensure we do not have both sources of data to merge
    // i.e. logFileStream and recordIterator cannot be both non-null
    ValidationUtils.checkArgument(!(logFileStream != null && recordIterator != null),
        "Cannot provide both logFileStream and recordIterator");

    this.baseFileOption = Option.ofNullable(baseFileOption).orElse(Option.empty());
    this.partitionPath = partitionPath;
    this.start = start;
    this.length = length;

    if (logFileStream != null) {
      // Process Log Files (if provided)
      this.logFiles = logFileStream
          .sorted(HoodieLogFile.getLogFileComparator())
          .filter(logFile -> !logFile.getFileName().endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      this.recordIterator = Option.empty();
    } else if (recordIterator != null) {
      // Process Record Iterator (if provided)
      this.logFiles = Collections.emptyList();
      this.recordIterator = Option.of(recordIterator);
    } else {
      // Handle Case with neither (Base file only read)
      this.logFiles = Collections.emptyList();
      this.recordIterator = Option.empty();
    }
  }

  public List<HoodieLogFile> getLogFiles() {
    ValidationUtils.checkArgument(recordIterator.isEmpty(), "Log files are not initialized");
    return logFiles;
  }

  public boolean hasLogFiles() {
    return !logFiles.isEmpty();
  }

  public boolean isParquetBaseFile() {
    return baseFileOption.map(baseFile -> HoodieFileFormat.fromFileExtension(baseFile.getStoragePath().getFileExtension()) == HoodieFileFormat.PARQUET).orElse(false);
  }

  public boolean hasNoRecordsToMerge() {
    return this.logFiles.isEmpty() && recordIterator.isEmpty();
  }

  public Iterator<HoodieRecord> getRecordIterator() {
    return this.recordIterator.orElseThrow(() -> new IllegalStateException("The record iterator has not been setup"));
  }
}
