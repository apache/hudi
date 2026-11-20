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
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingPrepFailedException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingComputePermanentFailureException;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-row failures go to the job's one error table: a file prep fails is an error record of stage {@code prep} and no
 * row; an input compute fails for good keeps its data row (prepared text, NULL vector) and adds an error record naming the
 * model. With the error table off, either fails the round and nothing is committed.
 */
@Tag("functional")
public class TestEmbeddingRoundErrorTable extends EmbeddingRoundTestBase {

  @Test
  public void prepAndComputeFailuresLandInTheOneErrorTableAndTheDataRowKeepsItsText() throws Exception {
    FixtureFile good = text("good", "alpha", "beta");
    FixtureFile broken = text("broken", TextPipePreprocessor.BROKEN);
    FixtureFile permanent = text("permanent", "gamma", "delta " + ScriptedEmbeddingClient.PERMANENT);
    announce(good, broken, permanent);

    Round round = round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE));

    Map<String, Row> rows = rows(table());
    assertEquals(4, rows.size(), rows.keySet().toString());
    Row failed = rows.get(key(permanent, EmbeddingTableSchema.MODALITY_TEXT, 1));
    assertEquals("delta " + ScriptedEmbeddingClient.PERMANENT, failed.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK),
        "the data row keeps its prepared text");
    assertNull(vector(failed), "and has no vector");
    assertNotNull(vector(rows.get(key(permanent, EmbeddingTableSchema.MODALITY_TEXT, 0))));
    assertTrue(rows.keySet().stream().noneMatch(key -> key.startsWith("source_path_sha:" + broken.sha + ",")), "a failed file has no row");

    List<Map<String, Object>> errors = round.getErrors();
    assertEquals(2, errors.size(), errors.toString());
    List<Map<String, Object>> prepErrors = errors.stream().filter(error -> error.containsKey("stage")).collect(Collectors.toList());
    List<Map<String, Object>> computeErrors = errors.stream().filter(error -> error.containsKey("model")).collect(Collectors.toList());
    assertEquals(1, prepErrors.size(), errors.toString());
    assertEquals(broken.sha, prepErrors.get(0).get("source_path_sha"));
    assertEquals("prep", prepErrors.get(0).get("stage"));
    assertEquals(1, computeErrors.size(), errors.toString());
    assertEquals(EmbeddingStreamers.MODEL, computeErrors.get(0).get("model"));
    Map<String, Object> computeError = computeErrors.get(0);
    assertEquals(permanent.sha, computeError.get("source_path_sha"));
    assertEquals(versionOf(permanent).modificationTime, ((Number) computeError.get("source_modification_time")).longValue());
    assertEquals(versionOf(permanent).size, ((Number) computeError.get("source_file_size")).longValue());
    assertEquals("text", computeError.get("modality"));
    assertEquals(1L, ((Number) computeError.get("chunk_id")).longValue());
    assertTrue(String.valueOf(computeErrors.get(0).get("error")).contains("injected permanent failure"), errors.toString());
  }

  @Test
  public void withTheErrorTableOffAPermanentInputFailsTheRound() throws Exception {
    announce(text("permanent", "gamma " + ScriptedEmbeddingClient.PERMANENT));
    // only text is prepared: content of a modality the text model does not declare is refused at start with the error table off
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false", EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), TEXT_EXTENSION);

    Exception failure = assertThrows(Exception.class, () -> round(props));

    assertTrue(EmbeddingStreamers.causeOf(failure, EmbeddingComputePermanentFailureException.class).isPresent(),
        EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "no commit");
  }

  @Test
  public void withTheErrorTableOffAFailedFileFailsTheRound() throws Exception {
    announce(text("good", "alpha"), text("broken", TextPipePreprocessor.BROKEN));
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_ONLY,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false");

    Exception failure = assertThrows(Exception.class, () -> round(props));

    assertTrue(EmbeddingStreamers.causeOf(failure, EmbeddingPrepFailedException.class).isPresent(), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "no commit");
  }
}
