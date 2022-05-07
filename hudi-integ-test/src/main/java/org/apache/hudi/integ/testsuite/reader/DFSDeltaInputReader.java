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

package org.apache.hudi.integ.testsuite.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.collection.Pair;

/**
 * This class helps to estimate the number of files to read a given number of total records.
 * Use this class for all DFS based implementations of {@link DeltaInputReader}
 */
public abstract class DFSDeltaInputReader implements DeltaInputReader<GenericRecord> {

  protected List<String> getFilePathsToRead(String basePath, PathFilter filter, long totalRecordsToRead) throws
      IOException {
    FileSystem fs = FSUtils.getFs(basePath, new Configuration());
    // TODO : Sort list by file size and take the median file status to ensure fair calculation and change to remote
    // iterator
    List<FileStatus> fileStatuses = Arrays.asList(fs.globStatus(new Path(basePath, "*/*"), filter));
    if (fileStatuses.size() > 0) {
      FileStatus status = fileStatuses.get(0);
      long avgNumRecordsPerFile = analyzeSingleFile(status.getPath().toString());
      long numFilesToMatchExpectedRecords = (long) Math.ceil((double) totalRecordsToRead / (double)
          avgNumRecordsPerFile);
      long avgSizeOfEachFile = status.getLen();
      long totalSizeToRead = avgSizeOfEachFile * numFilesToMatchExpectedRecords;
      // choose N files with that length
      Pair<Integer, Integer> fileStatusIndexRange = getFileStatusIndexRange(fileStatuses, avgSizeOfEachFile,
          totalSizeToRead);
      int startIndex = fileStatusIndexRange.getLeft();
      List<String> filePaths = new ArrayList<>();
      while (startIndex == 0 || startIndex < fileStatusIndexRange.getRight()) {
        filePaths.add(fileStatuses.get(startIndex).getPath().toString());
        startIndex++;
      }
      return filePaths;
    }
    return Collections.emptyList();
  }

  protected Pair<Integer, Integer> getFileStatusIndexRange(List<FileStatus> fileStatuses, long averageFileSize, long
      totalSizeToRead) {
    long totalSizeOfFilesPresent = 0;
    int startOffset = 0;
    int endOffset = 0;
    for (FileStatus fileStatus : fileStatuses) {
      // If current file length is greater than averageFileSize, increment by averageFileSize since our
      // totalSizeToRead calculation is based on the averageRecordSize * numRecordsToRead.
      if (fileStatus.getLen() > averageFileSize) {
        totalSizeOfFilesPresent += averageFileSize;
      } else {
        totalSizeOfFilesPresent += fileStatus.getLen();
      }
      if (totalSizeOfFilesPresent <= totalSizeToRead) {
        endOffset++;
      } else {
        return Pair.of(startOffset, endOffset);
      }
    }
    return Pair.of(startOffset, endOffset);
  }

  /**
   * Implementation of {@link DeltaInputReader}s to provide a way to read a single file on DFS and provide an
   * average number of records across N files.
   */
  protected long analyzeSingleFile(String filePath) {
    throw new UnsupportedOperationException("No implementation found");
  }
}
