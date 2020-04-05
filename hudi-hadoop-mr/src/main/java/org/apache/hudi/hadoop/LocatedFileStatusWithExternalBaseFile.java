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

package org.apache.hudi.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Sub-Type of File Status tracking both skeleton and external base file's status.
 */
public class LocatedFileStatusWithExternalBaseFile extends LocatedFileStatus {

  private final FileStatus externalFileStatus;

  public LocatedFileStatusWithExternalBaseFile(LocatedFileStatus fileStatus, FileStatus externalFileStatus)
      throws IOException {
    super(fileStatus, fileStatus.getBlockLocations());
    this.externalFileStatus = externalFileStatus;
  }

  @Override
  public Path getPath() {
    return new HoodiePathWithExternalFileStatus(super.getPath(), externalFileStatus);
  }
}