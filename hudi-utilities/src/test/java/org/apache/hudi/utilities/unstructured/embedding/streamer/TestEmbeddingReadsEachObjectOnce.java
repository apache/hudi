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

import org.apache.hudi.utilities.sources.blob.SourceOpenHooks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code prep_and_compute} round over the unstructured data source on S3 reads each object exactly once: an inline object when the source
 * builds its row (the round caches the batch, so the summary, the balancer and preprocessing read that cache), an object above
 * the inline ceiling when preprocessing streams it. Compute reads only the prepared rows, never an object.
 */
@Tag("functional")
class TestEmbeddingReadsEachObjectOnce extends EmbeddingRoundTestBase {

  private static final long INLINE_CEILING = 32;

  @AfterEach
  void clearOpens() {
    SourceOpenHooks.clear();
  }

  @Test
  void inlineObjectsAreReadOnlyWhenTheSourceBuildsTheirRows() throws Exception {
    assertEachObjectOpenedOnce(Arrays.asList(text("a", "alpha", "beta"), text("b", "gamma"), text("c", "delta", "epsilon")), 5);
  }

  @Test
  void eachObjectIsOpenedOncePerRound() throws Exception {
    List<FixtureFile> files = Arrays.asList(text("a", "alpha", "beta"), text("b", "gamma"), text("c", "delta", "epsilon"),
        text("d", "zeta zeta zeta zeta", "eta eta eta eta eta", "theta theta theta"));
    assertTrue(files.get(3).bytes.length > INLINE_CEILING && files.get(0).bytes.length <= INLINE_CEILING);
    assertEachObjectOpenedOnce(files, 8);
  }

  private void assertEachObjectOpenedOnce(List<FixtureFile> files, long expectedRows) throws Exception {
    SourceOpenHooks.clear();
    announce(files.toArray(new FixtureFile[0]));

    Round round = round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, INLINE_CEILING,
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "3"));

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(expectedRows, round.getRowsWritten(), round.toString());
    assertTrue(ScriptedEmbeddingClient.CALLS.get() > 0, "the round computed embeddings");
    assertEquals(files.size(), TextPipePreprocessor.OPENED.get(), "each object is prepared once");
    Map<String, Integer> opens = new LinkedHashMap<>();
    Map<String, Integer> once = new LinkedHashMap<>();
    for (FixtureFile file : files) {
      opens.put(keyOf(file), SourceOpenHooks.opens(objects().file(keyOf(file)).toAbsolutePath().toString()));
      once.put(keyOf(file), 1);
    }
    assertEquals(once, opens, "object opens per key");
  }
}
