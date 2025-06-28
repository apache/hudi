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

package org.apache.hudi.metadata.indexversion;

import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.Arrays;
import java.util.List;

public enum SecondaryIndexVersion implements HoodieIndexVersion {
  V1(1, Arrays.asList("1.0.0")),
  V2(2, Arrays.asList("1.1.0"));

  private final int versionCode;
  private final List<String> releaseVersions;

  SecondaryIndexVersion(int versionCode, List<String> releaseVersions) {
    this.versionCode = versionCode;
    this.releaseVersions = releaseVersions;
  }

  @Override
  public MetadataPartitionType getPartitionType() {
    return MetadataPartitionType.SECONDARY_INDEX;
  }

  @Override
  public int versionCode() {
    return versionCode;
  }

  @Override
  public List<String> getReleaseVersions() {
    return releaseVersions;
  }

  @Override
  public String toString() {
    return name();
  }
}
