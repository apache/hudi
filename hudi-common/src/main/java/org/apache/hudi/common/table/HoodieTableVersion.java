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

package org.apache.hudi.common.table;

import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.HoodieException;

import java.util.Arrays;
import java.util.List;

/**
 * Table's version that controls what version of writer/readers can actually read/write
 * to a given table.
 */
public enum HoodieTableVersion {
  // < 0.6.0 versions
  ZERO(0, CollectionUtils.createImmutableList("0.3.0"), TimelineLayoutVersion.LAYOUT_VERSION_0),
  // 0.6.0 onwards
  ONE(1, CollectionUtils.createImmutableList("0.6.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 0.9.0 onwards
  TWO(2, CollectionUtils.createImmutableList("0.9.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 0.10.0 onwards
  THREE(3, CollectionUtils.createImmutableList("0.10.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 0.11.0 onwards
  FOUR(4, CollectionUtils.createImmutableList("0.11.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 0.12.0 onwards
  FIVE(5, CollectionUtils.createImmutableList("0.12.0", "0.13.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 0.14.0 onwards
  SIX(6, CollectionUtils.createImmutableList("0.14.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 0.16.0
  SEVEN(7, CollectionUtils.createImmutableList("0.16.0"), TimelineLayoutVersion.LAYOUT_VERSION_1),
  // 1.0
  EIGHT(8, CollectionUtils.createImmutableList("1.0.0"), TimelineLayoutVersion.LAYOUT_VERSION_2),
  // 1.1
  NINE(9, CollectionUtils.createImmutableList("1.1.0"), TimelineLayoutVersion.LAYOUT_VERSION_2);

  private final int versionCode;

  private final List<String> releaseVersions;

  private final TimelineLayoutVersion timelineLayoutVersion;

  HoodieTableVersion(int versionCode, List<String> releaseVersions, TimelineLayoutVersion timelineLayoutVersion) {
    this.versionCode = versionCode;
    this.releaseVersions = releaseVersions;
    this.timelineLayoutVersion = timelineLayoutVersion;
  }

  public TimelineLayoutVersion getTimelineLayoutVersion() {
    return timelineLayoutVersion;
  }

  public int versionCode() {
    return versionCode;
  }

  public static HoodieTableVersion current() {
    return NINE;
  }

  public static HoodieTableVersion fromVersionCode(int versionCode) {
    return Arrays.stream(HoodieTableVersion.values())
        .filter(v -> v.versionCode == versionCode).findAny()
        .orElseThrow(() -> new HoodieException("Unknown table versionCode:" + versionCode));
  }

  public static HoodieTableVersion fromReleaseVersion(String releaseVersion) {
    return Arrays.stream(HoodieTableVersion.values())
        .filter(v -> v.releaseVersions.contains(releaseVersion)).findAny()
        .orElseThrow(() -> new HoodieException("Unknown table firstReleaseVersion:" + releaseVersion));
  }

  public boolean greaterThanOrEquals(HoodieTableVersion other) {
    return greaterThan(other) || this.versionCode == other.versionCode;
  }

  public boolean greaterThan(HoodieTableVersion other) {
    return this.versionCode > other.versionCode;
  }

  public boolean lesserThan(HoodieTableVersion other) {
    return this.versionCode < other.versionCode;
  }
}
