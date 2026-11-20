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
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The embedding stream's checkpoint is the stock S3 events checkpoint {@code <events commit>#<key>} (V1), as the blob stream's
 * is. One events commit split across rounds by the stock files-per-sync budget (the source limit is set but above the objects'
 * bytes, so it does not split) resumes at the exact key the last committed round names; a round that fails mid-commit commits
 * nothing and the rerun resumes from the same key.
 */
@Tag("functional")
public class TestEmbeddingSplitRoundResume extends EmbeddingRoundTestBase {

  /** At most two objects per round, by the stock files-per-sync budget. */
  private TypedProperties twoPerRound() {
    return props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, INLINE_MAX_BYTES,
        CloudSourceConfig.SOURCE_MAX_FILES_PER_SYNC.key(), "2");
  }

  private Round limitedRound(TypedProperties props) {
    return EmbeddingStreamers.round(spark(), props, cfg -> cfg.sourceLimit = 64);
  }

  private Set<String> committedObjects() {
    Map<String, Row> rows = rows(table());
    return rows.values().stream().map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_FILE_NAME))
        .collect(Collectors.toCollection(TreeSet::new));
  }

  @Test
  public void oneEventsCommitSplitAcrossRoundsResumesAtTheExactKey() throws Exception {
    List<FixtureFile> files = Arrays.asList(text("a", "alpha"), text("b", "beta"), text("c", "gamma"));
    announce(files.toArray(new FixtureFile[0]));
    String events = EmbeddingStreamers.commits(objects().eventsPath(), storage()).get(0).requestedTime();

    Round first = limitedRound(twoPerRound());
    assertEquals(events + "#" + keyOf(files.get(1)), EmbeddingStreamers.checkpoint(table(), storage(), first.getCommit().get()));
    assertEquals(new TreeSet<>(Arrays.asList("a." + TEXT_EXTENSION, "b." + TEXT_EXTENSION)), committedObjects());

    Round second = limitedRound(twoPerRound());
    assertEquals(events + "#" + keyOf(files.get(2)), EmbeddingStreamers.checkpoint(table(), storage(), second.getCommit().get()));
    assertEquals(1, second.getRowsWritten(), "only the rest of the events commit: " + second);
    assertEquals(3, rows(table()).size(), "every object once, no duplicate");

    assertFalse(limitedRound(twoPerRound()).getCommit().isPresent(), "nothing is left to read");
    assertEquals(3, TextPipePreprocessor.OPENED.get(), "each object is prepared once");
  }

  @Test
  public void roundThatFailsMidCommitCommitsNothingAndTheRerunResumesFromTheSameKey() throws Exception {
    List<FixtureFile> files = Arrays.asList(text("a", "alpha"), text("b", "beta"), text("c", "gamma"));
    announce(files.toArray(new FixtureFile[0]));
    String events = EmbeddingStreamers.commits(objects().eventsPath(), storage()).get(0).requestedTime();
    limitedRound(twoPerRound());
    List<HoodieInstant> before = EmbeddingStreamers.commits(table(), storage());

    // the provider is down for the round that would read the rest of the events commit
    ScriptedEmbeddingClient.OUTAGE.set(Integer.MAX_VALUE);
    Exception failure = assertThrows(Exception.class, () -> limitedRound(twoPerRound()));
    assertTrue(EmbeddingStreamers.causeOf(failure, EmbeddingProviderUnavailableException.class).isPresent(),
        EmbeddingStreamers.causes(failure));
    assertEquals(before, EmbeddingStreamers.commits(table(), storage()), "the failed round commits nothing");
    assertEquals(events + "#" + keyOf(files.get(1)), EmbeddingStreamers.checkpoint(table(), storage(), before.get(before.size() - 1)));

    ScriptedEmbeddingClient.OUTAGE.set(0);
    Round rerun = limitedRound(twoPerRound());
    assertEquals(events + "#" + keyOf(files.get(2)), EmbeddingStreamers.checkpoint(table(), storage(), rerun.getCommit().get()));
    assertEquals(1, rerun.getRowsWritten(), rerun.toString());
    assertEquals(new TreeSet<>(Arrays.asList("a." + TEXT_EXTENSION, "b." + TEXT_EXTENSION, "c." + TEXT_EXTENSION)), committedObjects());
    assertTrue(rows(table()).values().stream().allMatch(row -> vector(row) != null), "every row has its embedding");
  }
}
