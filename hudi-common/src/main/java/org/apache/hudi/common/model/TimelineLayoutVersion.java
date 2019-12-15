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

package org.apache.hudi.common.model;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata Layout Version. Add new version when timeline format changes
 */
public class TimelineLayoutVersion implements Serializable, Comparable<TimelineLayoutVersion> {

  public static final Integer VERSION_0 = 0; // pre 0.5.1  version format
  public static final Integer VERSION_1 = 1; // current version with no renames

  public static final Integer CURR_VERSION = VERSION_1;
  public static final TimelineLayoutVersion CURR_LAYOUT_VERSION = new TimelineLayoutVersion(CURR_VERSION);

  private Integer version;

  public TimelineLayoutVersion(Integer version) {
    Preconditions.checkArgument(version <= CURR_VERSION);
    Preconditions.checkArgument(version >= VERSION_0);
    this.version = version;
  }

  /**
   * For Pre 0.5.1 release, there was no metadata version. This method is used to detect
   * this case.
   * @return
   */
  public boolean isNullVersion() {
    return Objects.equals(version, VERSION_0);
  }

  public Integer getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimelineLayoutVersion that = (TimelineLayoutVersion) o;
    return Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }

  @Override
  public int compareTo(TimelineLayoutVersion o) {
    return Integer.compare(version, o.version);
  }
}
