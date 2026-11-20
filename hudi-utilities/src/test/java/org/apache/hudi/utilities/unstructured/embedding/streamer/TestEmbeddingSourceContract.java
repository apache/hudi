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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobSourceSetup;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.sources.blob.SourceOpenHooks;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

/**
 * The unstructured source's contract, with an embedding job's configuration: the embedding stream selects what the blob stream
 * selects and refuses what it refuses. Structured objects are never selected nor read; the extension list narrows the selection;
 * an explicit data format or a structured extension is refused when the job starts; a removal event writes
 * nothing. Reads that fail take the source's and prep's own failure paths: an inline object's read fails the round in the source, an out-of-line
 * object's transient open failure is retried in the round, and an out-of-line object gone before prep is a failed file whose
 * error row the error table holds.
 */
@Tag("functional")
class TestEmbeddingSourceContract extends EmbeddingRoundTestBase {

  public static class CountingValidationPreprocessor extends TextPipePreprocessor {
    static final AtomicInteger VALIDATIONS = new AtomicInteger();

    @Override
    public void validateConfig(TypedProperties props, Tokenizer tokenizer) {
      VALIDATIONS.incrementAndGet();
    }
  }

  @Test
  void propertiesAreValidatedAtStartupAndAgainForTheRound() throws Exception {
    CountingValidationPreprocessor.VALIDATIONS.set(0);
    announce(text("a", "alpha"));
    Round result = round(prepOnly(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + TEXT_EXTENSION
        + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX, CountingValidationPreprocessor.class.getName()));
    assertTrue(result.getCommit().isPresent(), result.toString());
    assertEquals(2, CountingValidationPreprocessor.VALIDATIONS.get(), "startup and round entry each validate the properties");
  }

  @Test
  void existingTableStartupValidatesTheJobAndTableOnce() throws Exception {
    announce(text("a", "alpha"));
    Round control = round(prepOnly());
    assertTrue(control.getCommit().isPresent(), control.toString());
    try (MockedStatic<BlobSourceSetup> jobs = mockStatic(BlobSourceSetup.class, CALLS_REAL_METHODS);
         MockedStatic<UnstructuredDataSource> tables = mockStatic(UnstructuredDataSource.class, CALLS_REAL_METHODS)) {
      round(prepOnly());
      jobs.verify(() -> BlobSourceSetup.validate(any(TypedProperties.class), eq(UnstructuredMode.EMBEDDING), anyString()), times(1));
      tables.verify(() -> UnstructuredDataSource.validate(eq(UnstructuredMode.EMBEDDING), anyString(), any(HoodieTableConfig.class)), times(1));
    }
  }

  /** Objects above this many bytes are out of line. */
  private static final long INLINE_CEILING = 40;

  @AfterEach
  void clearStatuses() {
    SourceOpenHooks.clear();
  }

  private TypedProperties prepOnly(String... overrides) {
    return props(table(), EmbeddingTableConfig.MODE_PREP_ONLY, INLINE_CEILING, overrides);
  }

  private Set<String> committedObjects() {
    return rows(table()).values().stream().map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_FILE_NAME))
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private String local(FixtureFile file) {
    return objects().file(keyOf(file)).toAbsolutePath().toString();
  }

  @Test
  void structuredObjectsAreNeverSelectedNorRead() throws Exception {
    FixtureFile parquet = object("data/a.parquet", "PAR1 not read".getBytes(StandardCharsets.UTF_8));
    FixtureFile json = object("data/b.json", "{\"not\":\"read\"}".getBytes(StandardCharsets.UTF_8));
    FixtureFile text = text("data/c", "gamma");
    announce(parquet, json, text);

    Round round = round(prepOnly());

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(Collections.singleton("c." + TEXT_EXTENSION), committedObjects());
    assertEquals(0, SourceOpenHooks.opens(local(parquet)), "the parquet object is never opened");
    assertEquals(0, SourceOpenHooks.opens(local(json)), "the json object is never opened");
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
  }

  @Test
  void theExtensionListNarrowsTheSelection() throws Exception {
    FixtureFile text = text("a", "alpha");
    FixtureFile pdf = object("b.pdf", org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures.text("Omicron pi."));
    announce(text, pdf);

    round(prepOnly(BlobTableConfig.FILE_EXTENSIONS.key(), "pdf"));

    assertEquals(Collections.singleton("b.pdf"), committedObjects());
    assertEquals(0, TextPipePreprocessor.OPENED.get(), "the text object is not selected");
    assertEquals(0, SourceOpenHooks.opens(local(text)));
  }

  @ParameterizedTest(name = "{0}={1}")
  @CsvSource(delimiter = '|', value = {
      "hoodie.streamer.source.cloud.data.datafile.format|parquet|must be empty for UnstructuredDataSource (mode embedding)",
      "hoodie.blob.table.file.extensions|parquet|Structured file extension is not supported by UnstructuredDataSource (mode embedding)",
      "hoodie.blob.table.file.extensions|txt|UnstructuredDataSource (mode embedding): unsupported unstructured file extension: txt"})
  void policyOutsideTheSourceContractIsRefusedWhenTheJobStarts(String key, String value, String refusal) throws Exception {
    announce(text("a", "alpha"));

    Exception failure = assertThrows(Exception.class, () -> round(prepOnly(key, value)));

    IllegalArgumentException refused = EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure)));
    assertTrue(refused.getMessage() != null && refused.getMessage().contains(refusal), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing is written");
    assertEquals(0, TextPipePreprocessor.OPENED.get());
  }

  @Test
  void removalEventWritesNothingAndTheRowsStay() throws Exception {
    FixtureFile a = text("a", "alpha", "beta");
    announce(a);
    round(prepOnly());

    FixtureFile b = text("b", "gamma");
    BlobJobHarness objects = objects();
    S3EventsTableFixture.Event created = objects.put(keyOf(b), b.bytes);
    objects.announce(spark(), Arrays.asList(S3EventsTableFixture.removed(BlobJobHarness.BUCKET, keyOf(a), BlobJobHarness.EVENT_TIME),
        created));
    Files.delete(objects.file(keyOf(a)));
    Round round = round(prepOnly());

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(1, round.getRowsWritten(), "only the created object: " + round);
    assertEquals(3, rows(table()).size(), "deletes are not handled: the removed object's rows stay");
    assertEquals(new TreeSet<>(Arrays.asList("a." + TEXT_EXTENSION, "b." + TEXT_EXTENSION)), committedObjects());
  }

  @Test
  void anInlineObjectThatCannotBeReadFailsTheRoundInTheSource() throws Exception {
    FixtureFile a = text("a", "alpha");
    announce(a);
    SourceOpenHooks.beforeOpen(local(a), () -> {
      throw new IOException("injected read failure of an inline object");
    });

    Exception failure = assertThrows(Exception.class, () -> round(prepOnly()));

    assertTrue(EmbeddingStreamers.causes(failure).contains("injected read failure"), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing is committed");
    SourceOpenHooks.clear();
    Round rerun = round(prepOnly());
    assertEquals(1, rerun.getRowsWritten(), "the rerun reads the same events: " + rerun);
  }

  @Test
  void anOutOfLineObjectsTransientOpenFailureIsRetriedInTheRound() throws Exception {
    FixtureFile large = text("large", "a long first chunk of text", "a long second chunk of text");
    assertTrue(large.bytes.length > INLINE_CEILING);
    announce(large);
    AtomicInteger failures = new AtomicInteger();
    SourceOpenHooks.beforeOpen(local(large), () -> {
      if (failures.getAndIncrement() == 0) {
        throw new IOException("injected transient open failure");
      }
    });

    Round round = round(prepOnly(EmbeddingTableConfig.PREP_SOURCE_RETRY_BACKOFF_MS.key(), "0"));

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertEquals(2, round.getRowsWritten(), round.toString());
    assertEquals(2, failures.get(), "one failed open, one retried open");
  }

  @Test
  void anInlineObjectGoneBeforeTheReadIsQuarantinedAsGoneSinceAnnounced() throws Exception {
    FixtureFile a = text("a", "alpha");
    FixtureFile b = text("b", "beta");
    announce(a, b);
    String path = local(a);
    long observedTime = Files.getLastModifiedTime(java.nio.file.Paths.get(path)).toMillis();
    // the object disappears after the source's status call and before its inline read
    SourceOpenHooks.beforeOpen(path, () -> Files.deleteIfExists(java.nio.file.Paths.get(path)));

    Round round = round(prepOnly());

    assertEquals(Collections.singleton("b." + TEXT_EXTENSION), committedObjects());
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    assertEquals(a.sha, error.get("source_path_sha"));
    assertEquals("prep", error.get("stage"));
    assertEquals("gone since announced", error.get("error"));
    assertEquals(observedTime, ((Number) error.get("source_modification_time")).longValue(), "the version the status call observed");
    assertEquals((long) a.bytes.length, ((Number) error.get("source_file_size")).longValue());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()),
        "the round commits past the gone object");
  }

  /** A1 on the embedding job: gone before the source's status call, the error record carries no observed version (0, the announced size). */
  @Test
  void anInlineObjectGoneBeforeTheStatusCallIsQuarantinedWithTheAnnouncedSize() throws Exception {
    FixtureFile a = text("a", "alpha");
    FixtureFile b = text("b", "beta");
    announce(a, b);
    Files.delete(java.nio.file.Paths.get(local(a)));

    Round round = round(prepOnly());

    assertEquals(Collections.singleton("b." + TEXT_EXTENSION), committedObjects());
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    assertEquals(a.sha, error.get("source_path_sha"));
    assertEquals("gone since announced", error.get("error"));
    assertEquals(0L, ((Number) error.get("source_modification_time")).longValue(), "nothing was observed");
    assertEquals((long) a.bytes.length, ((Number) error.get("source_file_size")).longValue(), "the announced size");
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
  }

  /** With the error table off nothing can record the gone object, so the round fails as every failed file does. */
  @Test
  void goneObjectWithoutAnErrorTableFailsTheRound() throws Exception {
    FixtureFile a = text("a", "alpha");
    announce(a);
    Files.delete(java.nio.file.Paths.get(local(a)));

    Exception failure = assertThrows(Exception.class, () -> round(prepOnly(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false")));

    assertTrue(EmbeddingStreamers.causes(failure).contains("gone since announced"), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing is committed");
  }

  @Test
  void anOutOfLineObjectGoneBeforePrepIsAFailedFileAndTheOtherObjectsCommit() throws Exception {
    FixtureFile gone = text("gone", "a long first chunk of text", "a long second chunk of text");
    FixtureFile kept = text("kept", "delta");
    assertTrue(gone.bytes.length > INLINE_CEILING);
    announce(gone, kept);
    String path = local(gone);
    // the object disappears after the source stated it and before prep opens it
    SourceOpenHooks.beforeOpen(path, () -> Files.deleteIfExists(java.nio.file.Paths.get(path)));

    Round round = round(prepOnly());

    assertEquals(Collections.singleton("kept." + TEXT_EXTENSION), committedObjects());
    assertEquals(1, round.getErrors().size(), round.toString());
    assertEquals(gone.sha, round.getErrors().get(0).get("source_path_sha"));
    assertEquals("prep", round.getErrors().get(0).get("stage"));
    assertFalse(Files.exists(java.nio.file.Paths.get(path)));
    assertTrue(String.valueOf(round.getErrors().get(0).get("error")).contains("does not exist"), round.toString());
  }
}
