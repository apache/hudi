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

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.model.TestHoodieCommitMetadata.verifyMetadataFieldNames;

public class TestHoodieReplaceCommitMetadata {

  private static final List<String> EXPECTED_FIELD_NAMES = Arrays.asList(
      "partitionToWriteStats", "partitionToReplaceFileIds", "compacted", "extraMetadata", "operationType");

  @Test
  public void verifyFieldNamesInReplaceCommitMetadata() throws IOException {
    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(10);
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> {
      commitMetadata.addWriteStat(stat.getPartitionPath(), stat);
      commitMetadata.addReplaceFileId(stat.getPartitionPath(), stat.getFileId());
    });
    verifyMetadataFieldNames(commitMetadata, EXPECTED_FIELD_NAMES);
  }
}
