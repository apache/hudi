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

import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.hbase.fs.HFileSystem;
import org.apache.hudi.hbase.io.FSDataInputStreamWrapper;
import org.apache.hudi.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A builder that helps in building up the ReaderContext
 */
@InterfaceAudience.Private
public class ReaderContextBuilder {
  private Path filePath;
  private FSDataInputStreamWrapper fsdis;
  private long fileSize;
  private HFileSystem hfs;
  private boolean primaryReplicaReader = true;
  private ReaderType type = ReaderType.PREAD;

  public ReaderContextBuilder() {}

  public ReaderContextBuilder withFilePath(Path filePath) {
    this.filePath = filePath;
    return this;
  }

  public ReaderContextBuilder withFileSize(long fileSize) {
    this.fileSize = fileSize;
    return this;
  }

  public ReaderContextBuilder withInputStreamWrapper(FSDataInputStreamWrapper fsdis) {
    this.fsdis = fsdis;
    return this;
  }

  public ReaderContextBuilder withFileSystem(HFileSystem hfs) {
    this.hfs = hfs;
    return this;
  }

  public ReaderContextBuilder withFileSystem(FileSystem fs) {
    if (!(fs instanceof HFileSystem)) {
      this.hfs = new HFileSystem(fs);
    } else {
      this.hfs = (HFileSystem) fs;
    }
    return this;
  }

  public ReaderContextBuilder withPrimaryReplicaReader(boolean primaryReplicaReader) {
    this.primaryReplicaReader = primaryReplicaReader;
    return this;
  }

  public ReaderContextBuilder withReaderType(ReaderType type) {
    this.type = type;
    return this;
  }

  public ReaderContextBuilder withFileSystemAndPath(FileSystem fs, Path filePath)
      throws IOException {
    this.withFileSystem(fs)
        .withFilePath(filePath)
        .withFileSize(fs.getFileStatus(filePath).getLen())
        .withInputStreamWrapper(new FSDataInputStreamWrapper(fs, filePath));
    return this;
  }

  public ReaderContext build() {
    validateFields();
    return new ReaderContext(filePath, fsdis, fileSize, hfs, primaryReplicaReader, type);
  }

  private void validateFields() throws IllegalArgumentException {
    checkNotNull(filePath, "Illegal ReaderContext, no filePath specified.");
    checkNotNull(fsdis, "Illegal ReaderContext, no StreamWrapper specified.");
    checkNotNull(hfs, "Illegal ReaderContext, no HFileSystem specified.");
    checkArgument(fileSize > 0L, "Illegal ReaderContext, fileSize <= 0");
  }
}
