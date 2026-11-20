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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A whole-batch provider outage fails the round: its instant is rolled back, nothing is committed and the checkpoint does not
 * move; the round's prep is re-run with the round once the provider is back. Compute runs inside the commit, so an outage stops
 * the whole stream until the provider is back.
 */
@Tag("functional")
public class TestEmbeddingRoundProviderOutage extends EmbeddingRoundTestBase {

  @Test
  public void anOutageFailsTheRoundWithNoCommitAndTheNextRoundCommitsEverything() throws Exception {
    announce(text("a", "alpha", "beta"));
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    round(props);
    List<HoodieInstant> before = EmbeddingStreamers.commits(table(), storage());
    String checkpointBefore = EmbeddingStreamers.checkpoint(table(), storage(), before.get(before.size() - 1));
    announce(text("b", "gamma", "delta"), text("c", "epsilon"));
    TextPipePreprocessor.OPENED.set(0);

    ScriptedEmbeddingClient.OUTAGE.set(Integer.MAX_VALUE);
    Exception failure = assertThrows(Exception.class, () -> round(props));

    assertTrue(EmbeddingStreamers.causes(failure).contains("EMBED-COMPUTE-PROVIDER-UNAVAILABLE"), EmbeddingStreamers.causes(failure));
    List<HoodieInstant> after = EmbeddingStreamers.commits(table(), storage());
    assertEquals(before, after, "no commit");
    assertEquals(checkpointBefore, EmbeddingStreamers.checkpoint(table(), storage(), after.get(after.size() - 1)),
        "the checkpoint does not move");
    assertEquals(2, rows(table()).size(), "nothing of the failed round is visible");
    assertEquals(2, TextPipePreprocessor.OPENED.get(), "the failed round prepared its two files");

    ScriptedEmbeddingClient.OUTAGE.set(0);
    TextPipePreprocessor.OPENED.set(0);
    Round recovered = round(props);
    assertEquals(3, recovered.getRowsWritten(), recovered.toString());
    assertEquals(2, TextPipePreprocessor.OPENED.get(), "the rerun prepares the round again");
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), recovered.getCommit().get()));
    assertEquals(5, rows(table()).size());
  }
}
