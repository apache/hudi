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

package org.apache.hudi;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import java.util.Collections;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;

/**
 * Instant triple for testing.
 */
public class DummyActiveAction extends ActiveAction {
  private final byte[] commitMetadata;

  /**
   * Only for testing purpose.
   */
  public DummyActiveAction(HoodieInstant completed, byte[] commitMetadata) {
    super(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, completed.getAction(), completed.requestedTime()),
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, completed.getAction(), completed.requestedTime()),
        Collections.singletonList(completed));
    this.commitMetadata = commitMetadata;
  }

  @Override
  public Option<byte[]> getCommitMetadata(HoodieTableMetaClient metaClient) {
    return Option.of(this.commitMetadata);
  }
}
