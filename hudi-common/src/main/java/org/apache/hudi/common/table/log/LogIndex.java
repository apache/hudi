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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieIndexedLogFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public final class LogIndex implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LogIndex.class);
  private static final LogIndex EMPTY_INDEX = new LogIndex("0", Collections.emptyMap());

  public static LogIndex getEmptyIndex() {
    return EMPTY_INDEX;
  }

  private final String startInstant;
  private final Map<String, Long> fileToOffset;

  public LogIndex(String startInstant, Map<String, Long> fileToOffset) {
    this.startInstant = startInstant;
    this.fileToOffset = fileToOffset;
  }

  HoodieLogFile indexFile(HoodieLogFile logFile) {
    String fileName = logFile.getFileName();
    Long offset = fileToOffset.get(fileName);
    if (offset != null && offset > 0) {
      LOG.info("Created indexed log file " + logFile + " at instant " + startInstant + " with offset " + offset);
      return new HoodieIndexedLogFile(logFile, startInstant, offset);
    }
    return logFile;
  }

  @Override
  public String toString() {
    return "LogIndex{"
      + "startInstant='" + startInstant + '\''
      + ", fileToOffset=" + fileToOffset
      + '}';
  }
}