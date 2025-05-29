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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

import java.util.stream.Stream;

/**
 * Conflict resolution strategy to be used with metadata table when NBCC is enabled.
 * This resolution will not conflict any operations and let everything succeed the conflict resolution step.
 */
public class MetadataTableNonBlockingWritesConflictResolutionStrategy implements ConflictResolutionStrategy {

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant, Option<HoodieInstant> lastSuccessfulInstant) {
    ValidationUtils.checkArgument(metaClient.isMetadataTable(), "MetadataTableNonBlockingWritesConflictResolutionStrategy can only be used for metadata table");
    return Stream.empty();
  }

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    return false;
  }

  @Override
  public Option<HoodieCommitMetadata> resolveConflict(HoodieTable table, ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) throws HoodieWriteConflictException {
    throw new HoodieMetadataException("No conflicts should have been deducted in metadata table ");
  }

  @Override
  public boolean isPreCommitRequired() {
    return false;
  }
}
