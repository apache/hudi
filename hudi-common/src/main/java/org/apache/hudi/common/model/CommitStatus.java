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

import org.apache.hudi.common.util.ValidationUtils;

public class CommitStatus {

  public static CommitStatus success(HoodieCommitMetadata metadata) {
    ValidationUtils.checkArgument(metadata != null, "The commit metadata should not null");
    return new CommitStatus(true, metadata);
  }

  public static CommitStatus successWithEmptyCommit() {
    return new CommitStatus(true, null);
  }

  public static CommitStatus fail() {
    return new CommitStatus(false, null);
  }

  private boolean success;

  private HoodieCommitMetadata metadata;

  private CommitStatus(boolean success, HoodieCommitMetadata metadata) {
    this.success = success;
    this.metadata = metadata;
  }

  public boolean isSuccess() {
    return success;
  }

  public HoodieCommitMetadata getMetadata() {
    return metadata;
  }
}
