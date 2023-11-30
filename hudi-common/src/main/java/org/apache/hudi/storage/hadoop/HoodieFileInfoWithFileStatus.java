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

package org.apache.hudi.storage.hadoop;

import org.apache.hudi.storage.HoodieFileInfo;
import org.apache.hudi.storage.HoodieLocation;

import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public class HoodieFileInfoWithFileStatus extends HoodieFileInfo {
  private final FileStatus fileStatus;

  public HoodieFileInfoWithFileStatus(FileStatus fileStatus) {
    super();
    this.fileStatus = fileStatus;
  }

  public HoodieLocation getLocation() {
    return new HoodieLocation(fileStatus.getPath().toUri().toString());
  }

  public long getLength() {
    return fileStatus.getLen();
  }

  public boolean isFile() {
    return fileStatus.isFile();
  }

  public boolean isDirectory() {
    return fileStatus.isDirectory();
  }

  public long getAccessTime() {
    return fileStatus.getAccessTime();
  }

  public long getModificationTime() {
    return fileStatus.getModificationTime();
  }

  public short getReplication() {
    return fileStatus.getReplication();
  }

  public long getBlockSize() {
    return fileStatus.getBlockSize();
  }

  public boolean isSymlink() {
    return fileStatus.isSymlink();
  }

  public HoodieLocation getSymlink() throws IOException {
    return new HoodieLocation(fileStatus.getSymlink().toUri().toString());
  }

  public String getOwner() {
    return fileStatus.getOwner();
  }

  public String getGroup() {
    return fileStatus.getGroup();
  }
}
