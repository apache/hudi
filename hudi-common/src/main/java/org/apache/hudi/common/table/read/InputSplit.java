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
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a split of input data for reading, which includes the partition path the data belongs to along with an optional base file and a list of log files.
 * If there is only a base file, it is possible for the reader to specify a particular range of the file with the start and length parameters.
 */
public class InputSplit {
  private final Option<HoodieBaseFile> baseFileOption;
  private final List<HoodieLogFile> logFiles;
  private final Option<Iterator<Pair<Serializable, BufferedRecord>>> recordIterator;
  private final String partitionPath;
  // Byte offset to start reading from the base file
  private final long start;
  // Length of bytes to read from the base file
  private final long length;

  InputSplit(Option<HoodieBaseFile> baseFileOption, Either<Stream<HoodieLogFile>,Iterator<Pair<Serializable, BufferedRecord>>> logFilesOrRecordIterator,
             String partitionPath, long start, long length) {
    this.baseFileOption = baseFileOption;
    if (logFilesOrRecordIterator.isLeft()) {
      this.logFiles = logFilesOrRecordIterator.asLeft().sorted(HoodieLogFile.getLogFileComparator())
          .filter(logFile -> !logFile.getFileName().endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
          .collect(Collectors.toList());
      this.recordIterator = Option.empty();
    } else {
      this.logFiles = Collections.emptyList();
      this.recordIterator = Option.of(logFilesOrRecordIterator.asRight());
    }
    this.partitionPath = partitionPath;
    this.start = start;
    this.length = length;
  }

  public Option<HoodieBaseFile> getBaseFileOption() {
    return baseFileOption;
  }

  public List<HoodieLogFile> getLogFiles() {
    return logFiles;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public long getStart() {
    return start;
  }

  public long getLength() {
    return length;
  }

  public boolean isParquetBaseFile() {
    return baseFileOption.map(baseFile -> HoodieFileFormat.fromFileExtension(baseFile.getStoragePath().getFileExtension()) == HoodieFileFormat.PARQUET).orElse(false);
  }

  public boolean hasNoRecordsToMerge() {
    return this.logFiles.isEmpty() && recordIterator.isEmpty();
  }

  public Option<Iterator<Pair<Serializable, BufferedRecord>>> getRecordIterator() {
    ValidationUtils.checkArgument(recordIterator.isPresent(), "Record iterator is not initialized");
    return this.recordIterator;
  }
}
