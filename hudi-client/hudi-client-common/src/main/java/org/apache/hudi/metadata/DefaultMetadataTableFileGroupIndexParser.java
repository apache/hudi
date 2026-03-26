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

package org.apache.hudi.metadata;

/**
 * Class to assist with finding an index for a given file group in MDT partition.
 * Most partitions in MDT are flat or one level where all file groups are numbered from 1 to N
 * and we can determine the index just based on file Id.
 * This is the default class to be used in most of such cases
 */
public class DefaultMetadataTableFileGroupIndexParser implements MetadataTableFileGroupIndexParser {

  private final int numFileGroups;

  public DefaultMetadataTableFileGroupIndexParser(int numFileGroups) {
    this.numFileGroups = numFileGroups;
  }

  @Override
  public int getFileGroupIndex(String fileID) {
    return HoodieTableMetadataUtil.getFileGroupIndexFromFileId(fileID);
  }

  @Override
  public int getNumberOfFileGroups() {
    return numFileGroups;
  }
}
