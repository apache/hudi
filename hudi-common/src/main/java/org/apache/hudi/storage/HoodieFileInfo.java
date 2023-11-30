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

package org.apache.hudi.storage;

import java.io.IOException;
import java.io.Serializable;

public class HoodieFileInfo implements Serializable {
  private HoodieLocation loc;
  private long length;
  private boolean isDir;
  private long modificationTime;

  public HoodieFileInfo() {

  }

  public HoodieFileInfo(HoodieLocation loc,
                        long length,
                        boolean isDir,
                        long modificationTime) {
    this.loc = loc;
    this.length = length;
    this.isDir = isDir;
    this.modificationTime = modificationTime;
  }

  public HoodieLocation getLocation() {
    return loc;
  }

  public long getLength() {
    return length;
  }

  public boolean isFile() {
    return !isDir;
  }

  public boolean isDirectory() {
    return isDir;
  }

  public long getAccessTime() {
    return 0;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public short getReplication() {
    return 0;
  }

  public long getBlockSize() {
    return 0;
  }

  public boolean isSymlink() {
    return false;
  }

  public HoodieLocation getSymlink() throws IOException {
    return null;
  }

  public String getOwner() {
    return "";
  }

  public String getGroup() {
    return "";
  }
}
