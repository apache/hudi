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

package org.apache.hudi.io.storage;

import java.io.Serializable;

public class HoodieFileStatus implements Serializable {
  private HoodieLocation location;
  private long length;
  private boolean isDirectory;
  private long modificationTime;

  public HoodieFileStatus(HoodieLocation location,
                          long length,
                          boolean isDirectory,
                          long modificationTime) {
    this.location = location;
    this.length = length;
    this.isDirectory = isDirectory;
    this.modificationTime = modificationTime;
  }

  public HoodieLocation getLocation() {
    return location;
  }

  public long getLength() {
    return length;
  }

  public boolean isFile() {
    return !isDirectory;
  }

  public boolean isDirectory() {
    return isDirectory;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public boolean isSymlink() {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieFileStatus that = (HoodieFileStatus) o;
    return getLocation().equals(that.getLocation());
  }

  @Override
  public int hashCode() {
    return getLocation().hashCode();
  }
}
