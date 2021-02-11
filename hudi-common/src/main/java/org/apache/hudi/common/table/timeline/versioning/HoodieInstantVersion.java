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

package org.apache.hudi.common.table.timeline.versioning;

import java.io.Serializable;
import java.util.Objects;

/**
 * Hoodie Instant Disk Format Version. Add new version when instant format changes.
 */
public class HoodieInstantVersion implements Serializable, Comparable<HoodieInstantVersion> {

  public static final Integer VERSION_0 = 0; // current version with state transition time
  public static final Integer VERSION_1 = 1; // current version with state transition time

  public static final Integer CURR_VERSION = VERSION_1;
  public static final HoodieInstantVersion CURR_INSTANT_VERSION = new HoodieInstantVersion(CURR_VERSION);

  private Integer version;

  public HoodieInstantVersion(Integer version) {
    this.version = version;
  }

  @Override
  public int compareTo(HoodieInstantVersion o) {
    return Integer.compare(version, o.version);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieInstantVersion that = (HoodieInstantVersion) o;
    return version.equals(that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }
}