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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FailingPreCommitValidator;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A failed commit leaves the checkpoint where it was, and the rerun reads the same events and commits exactly the keys a
 * clean run commits, with the same vectors and no duplicate: one writer needs no existing-row probe (the failed round's instant
 * is rolled back, and the keys are deterministic).
 */
@Tag("functional")
class TestEmbeddingRoundRerunAfterFailedCommit extends EmbeddingRoundTestBase {

  private void announceFirstObject() throws Exception {
    announce(text("a", "alpha", "beta"));
  }

  private String latestCheckpoint() {
    List<HoodieInstant> commits = EmbeddingStreamers.commits(table(), storage());
    return EmbeddingStreamers.checkpoint(table(), storage(), commits.get(commits.size() - 1));
  }

  @Test
  void afterAFailedCommitTheCheckpointHoldsAndTheRerunCommitsTheKeysOfACleanRun() throws Exception {
    announceFirstObject();
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    round(props);
    String checkpoint = latestCheckpoint();
    announce(text("b", "gamma", "delta", "epsilon"), text("c", "zeta"));

    // a failure after the write, before the commit: the streamer rolls the instant back
    TypedProperties failing = TypedProperties.copy(props);
    failing.setProperty(HoodiePreCommitValidatorConfig.VALIDATOR_CLASS_NAMES.key(), FailingPreCommitValidator.class.getName());
    Exception failure = assertThrows(Exception.class, () -> round(failing));
    assertTrue(EmbeddingStreamers.causeOf(failure, HoodieValidationException.class).isPresent(), EmbeddingStreamers.causes(failure));
    assertEquals(checkpoint, latestCheckpoint(), "the checkpoint does not advance");
    assertEquals(2, rows(table()).size(), "nothing of the failed round is visible");

    Round rerun = round(props);
    assertEquals(4, rerun.getRowsWritten(), rerun.toString());
    assertEquals(latestEventsCheckpoint(), latestCheckpoint());

    // a clean run of the same events into another table commits the same rows
    round(props(table("clean"), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE));
    assertEquals(describe(rows(table("clean"))), describe(rows(table())));
  }

  @Test
  void afterAComputeFailureTheRerunCommitsTheKeysOfACleanRun() throws Exception {
    announceFirstObject();
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    ScriptedEmbeddingClient.OUTAGE.set(Integer.MAX_VALUE);
    assertThrows(Exception.class, () -> round(props));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "no commit, so no checkpoint");

    ScriptedEmbeddingClient.OUTAGE.set(0);
    round(props);
    round(props(table("clean"), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE));
    assertEquals(describe(rows(table("clean"))), describe(rows(table())));
  }
}
