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

package org.apache.hudi.testsuite.writer;

import org.apache.hudi.common.util.collection.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class holds the write statistics for an instance of {@link DeltaInputWriter}.
 */
public class WriteStats implements Serializable {

  // The file path (if any) for the data written
  private String filePath;
  // Number of bytes written before being closed
  private long bytesWritten;
  // Number of records written before being closed
  private long recordsWritten;

  private List<Pair<String, String>> partitionPathRecordKey = new ArrayList<>();

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public long getBytesWritten() {
    return bytesWritten;
  }

  public void setBytesWritten(long bytesWritten) {
    this.bytesWritten = bytesWritten;
  }

  public List<Pair<String, String>> getPartitionPathRecordKey() {
    return partitionPathRecordKey;
  }

  public void setPartitionPathRecordKey(List<Pair<String, String>> partitionPathRecordKey) {
    this.partitionPathRecordKey = partitionPathRecordKey;
  }

  public long getRecordsWritten() {
    return recordsWritten;
  }

  public void setRecordsWritten(long recordsWritten) {
    this.recordsWritten = recordsWritten;
  }

}
