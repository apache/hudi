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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.BaseTestCommitMetadataSerDe;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;

public class TestCommitMetadataSerDeV2 extends BaseTestCommitMetadataSerDe {

  @Override
  protected CommitMetadataSerDe getSerDe() {
    return new CommitMetadataSerDeV2();
  }

  @Override
  protected HoodieInstant createTestInstant(String action, String id) {
    return INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, action, id);
  }

  @Test
  public void testEmptyMetadataSerDe() throws Exception {
    super.testEmptyMetadataSerDe();
  }

  @Test
  public void testPopulatedMetadataSerDe() throws Exception {
    super.testPopulatedMetadataSerDe();
  }

  @Test
  public void testReplaceCommitMetadataSerDe() throws Exception {
    super.testReplaceCommitMetadataSerDe();
  }
}
