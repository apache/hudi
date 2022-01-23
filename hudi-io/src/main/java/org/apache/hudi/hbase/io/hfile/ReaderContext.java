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

package org.apache.hudi.hbase.io.hfile;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.hbase.fs.HFileSystem;
import org.apache.hudi.hbase.io.FSDataInputStreamWrapper;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Carries the information on some of the meta data about the HFile Reader
 */
@InterfaceAudience.Private
public class ReaderContext {
  @InterfaceAudience.Private
  public enum ReaderType {
    PREAD,
    STREAM
  }
  private final Path filePath;
  private final FSDataInputStreamWrapper fsdis;
  private final long fileSize;
  private final HFileSystem hfs;
  private final boolean primaryReplicaReader;
  private final ReaderType type;

  public ReaderContext(Path filePath, FSDataInputStreamWrapper fsdis, long fileSize,
                       HFileSystem hfs, boolean primaryReplicaReader, ReaderType type) {
    this.filePath = filePath;
    this.fsdis = fsdis;
    this.fileSize = fileSize;
    this.hfs = hfs;
    this.primaryReplicaReader = primaryReplicaReader;
    this.type = type;
  }

  public Path getFilePath() {
    return this.filePath;
  }

  public FSDataInputStreamWrapper getInputStreamWrapper() {
    return this.fsdis;
  }

  public long getFileSize() {
    return this.fileSize;
  }

  public HFileSystem getFileSystem() {
    return this.hfs;
  }

  public boolean isPrimaryReplicaReader() {
    return this.primaryReplicaReader;
  }

  public ReaderType getReaderType() {
    return this.type;
  }
}
