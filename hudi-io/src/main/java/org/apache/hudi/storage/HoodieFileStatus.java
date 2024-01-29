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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;

import java.io.Serializable;

/**
 * Represents the information of a directory or a file.
 * The APIs are mainly based on {@code org.apache.hadoop.fs.FileStatus} class
 * with simplification based on what Hudi needs.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public class HoodieFileStatus implements Serializable {
  private final HoodieLocation location;
  private final long length;
  private final boolean isDirectory;
  private final long modificationTime;

  public HoodieFileStatus(HoodieLocation location,
                          long length,
                          boolean isDirectory,
                          long modificationTime) {
    this.location = location;
    this.length = length;
    this.isDirectory = isDirectory;
    this.modificationTime = modificationTime;
  }

  /**
   * @return the location.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public HoodieLocation getLocation() {
    return location;
  }

  /**
   * @return the length of a file in bytes.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public long getLength() {
    return length;
  }

  /**
   * @return whether this is a file.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public boolean isFile() {
    return !isDirectory;
  }

  /**
   * @return whether this is a directory.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public boolean isDirectory() {
    return isDirectory;
  }

  /**
   * @return the modification of a file.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  public long getModificationTime() {
    return modificationTime;
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
    // PLEASE NOTE that here we follow the same contract hadoop's FileStatus provides,
    // i.e., the equality is purely based on the location.
    return getLocation().equals(that.getLocation());
  }

  @Override
  public int hashCode() {
    // PLEASE NOTE that here we follow the same contract hadoop's FileStatus provides,
    // i.e., the hash code is purely based on the location.
    return getLocation().hashCode();
  }

  @Override
  public String toString() {
    return "HoodieFileStatus{"
        + "location=" + location
        + ", length=" + length
        + ", isDirectory=" + isDirectory
        + ", modificationTime=" + modificationTime
        + '}';
  }
}
