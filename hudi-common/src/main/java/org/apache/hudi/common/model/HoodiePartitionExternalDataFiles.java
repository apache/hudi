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

import java.io.Serializable;
import java.util.List;

public class HoodiePartitionExternalDataFiles implements Serializable {

  private final String externalBasePath;
  private final String externalPartitionPath;
  private final String hoodiePartitionPath;
  private final List<HoodieExternalFileIdMapping> externalFileIdMappings;

  public HoodiePartitionExternalDataFiles(String externalBasePath, String externalPartitionPath,
      String hoodiePartitionPath,
      List<HoodieExternalFileIdMapping> externalFileIdMappings) {
    this.externalBasePath = externalBasePath;
    this.externalPartitionPath = externalPartitionPath;
    this.hoodiePartitionPath = hoodiePartitionPath;
    this.externalFileIdMappings = externalFileIdMappings;
  }

  public String getExternalBasePath() {
    return externalBasePath;
  }

  public String getExternalPartitionPath() {
    return externalPartitionPath;
  }

  public String getHoodiePartitionPath() {
    return hoodiePartitionPath;
  }

  public List<HoodieExternalFileIdMapping> getExternalFileIdMappings() {
    return externalFileIdMappings;
  }
}
