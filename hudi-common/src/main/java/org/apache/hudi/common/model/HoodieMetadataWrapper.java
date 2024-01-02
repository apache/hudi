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

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;

/**
 * Wrapper for Hudi metadata.
 */
public class HoodieMetadataWrapper {

  private HoodieArchivedMetaEntry avroMetadataFromTimeline;
  private HoodieCommitMetadata commitMetadata;
  private boolean isAvroMetadata = false;

  public HoodieMetadataWrapper(HoodieArchivedMetaEntry avroMetadataFromTimeline) {
    this.avroMetadataFromTimeline = avroMetadataFromTimeline;
    this.isAvroMetadata = true;
  }

  public HoodieMetadataWrapper(HoodieCommitMetadata commitMetadata) {
    this.commitMetadata = commitMetadata;
  }

  public HoodieArchivedMetaEntry getMetadataFromTimeline() {
    return avroMetadataFromTimeline;
  }

  public HoodieCommitMetadata getCommitMetadata() {
    return commitMetadata;
  }

  public boolean isAvroMetadata() {
    return isAvroMetadata;
  }
}
