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
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.testutils.ParquetFooters;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The embedding job over the unstructured data source on S3, with no blob table anywhere: an inline object comes in the source
 * row, an object above the inline ceiling is streamed from the bucket at prep, and one commit holds every row with its embedding
 * under the stock S3 events checkpoint.
 */
@Tag("functional")
public class TestEmbeddingFromS3EventsNoBlobTable extends EmbeddingRoundTestBase {

  /** Objects above this many bytes are out of line. */
  private static final long INLINE_CEILING = 64;

  private TypedProperties prepAndCompute() {
    return props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, INLINE_CEILING,
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "3");
  }

  @Test
  public void inlineAndOutOfLineObjectsAreEmbeddedInOneCommitWithNoBlobTable() throws Exception {
    FixtureFile small = text("docs/small", "alpha", "beta");
    FixtureFile large = text("docs/large", "gamma gamma gamma gamma gamma", "delta delta delta delta delta",
        "epsilon epsilon epsilon epsilon");
    FixtureFile pdf = object("docs/c.pdf", PdfFixtures.text("Omicron pi rho sigma.", "Tau upsilon phi chi."));
    assertTrue(small.bytes.length <= INLINE_CEILING && large.bytes.length > INLINE_CEILING && pdf.bytes.length > INLINE_CEILING);
    announce(small, large, pdf);

    Round round = round(prepAndCompute());

    HoodieInstant commit = round.getCommit().get();
    assertTrue(round.getErrors().isEmpty(), round.toString());
    Map<String, Row> rows = rows(table());
    assertEquals(rows.size(), round.getRowsWritten());
    assertEquals(Arrays.asList("alpha", "beta"), chunks(rows, small));
    assertEquals(Arrays.asList("gamma gamma gamma gamma gamma", "delta delta delta delta delta", "epsilon epsilon epsilon epsilon"),
        chunks(rows, large), "the out-of-line object is read from the bucket");
    String pdfText = String.join(" ", chunks(rows, pdf));
    assertTrue(pdfText.contains("Omicron") && pdfText.contains("chi"), pdfText);
    for (Row row : rows.values()) {
      String sha = row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA);
      FixtureFile file = Arrays.asList(small, large, pdf).stream().filter(f -> f.sha.equals(sha)).findFirst().get();
      long chunk = row.getAs(EmbeddingTableSchema.CHUNK_ID);
      assertTrue(rows.containsKey(key(file, "text", chunk)), rows.keySet().toString());
      assertEquals(versionOf(file).modificationTime, row.<Long>getAs(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME));
      assertEquals(versionOf(file).size, row.<Long>getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE));
      assertEquals(file.sourcePath, row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
      assertEquals(file.fileName(), row.getAs(EmbeddingTableSchema.SOURCE_FILE_NAME));
      assertEquals(file.extension, row.getAs(EmbeddingTableSchema.SOURCE_FILE_EXTENSION));
      assertArrayEquals(DummyEmbeddingClient.vectorFor(sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row));
    }

    // the embedding table keeps the writer's stock Parquet defaults (compression codec, a record-key bloom filter): the blob table's
    // uncompressed, filter-less defaults belong to the blob job, the one that runs the quarantine transformer
    String defaultCodec = HoodieWriteConfig.newBuilder().withPath(table()).build().getParquetCompressionCodec();
    ParquetFooters.assertBaseFiles(jsc().hadoopConfiguration(), table(), CompressionCodecName.fromConf(defaultCodec), true);

    // the stock S3 events checkpoint, "<events commit>#<key>" under the V1 key, and no other checkpoint key
    String eventsCommit = EmbeddingStreamers.commits(objects().eventsPath(), storage()).get(0).requestedTime();
    String lastKey = Collections.max(Arrays.asList(keyOf(small), keyOf(large), keyOf(pdf)));
    assertEquals(eventsCommit + "#" + lastKey, EmbeddingStreamers.metadata(table(), storage(), commit)
        .getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1));
    assertEquals(Collections.singletonList(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1),
        EmbeddingStreamers.checkpointKeys(table(), storage(), commit));
    // no blob table: nothing beside the embedding table, its error table and the source; nothing copied under the table
    try (Stream<Path> tables = Files.list(localPath(basePath()))) {
      assertEquals(Arrays.asList("embedding_table", "source"), tables.map(path -> path.getFileName().toString())
          .filter(name -> !name.equals("embedding_table" + EmbeddingStreamers.ERRORS_SUFFIX)).sorted().collect(Collectors.toList()),
          "no blob table: only the embedding table, its error table and the source objects");
    }
    assertFalse(Files.exists(localPath(table()).resolve(".hoodie/.blob")), "no copy under the table");
  }

  @Test
  public void roundWithoutNewEventsCommitsNothingAndTheNextEventsCommitIsReadOnce() throws Exception {
    announce(text("a", "alpha", "beta"));
    assertTrue(round(prepAndCompute()).getCommit().isPresent());

    Round idle = round(prepAndCompute());
    assertFalse(idle.getCommit().isPresent(), idle.toString());

    FixtureFile c = text("c", "gamma", "delta", "epsilon");
    announce(c);
    Round next = round(prepAndCompute());
    assertEquals(3, next.getRowsWritten(), "only the new events commit's object is read: " + next);
    List<HoodieInstant> events = EmbeddingStreamers.commits(objects().eventsPath(), storage());
    assertEquals(events.get(1).requestedTime() + "#" + keyOf(c), EmbeddingStreamers.metadata(table(), storage(),
        next.getCommit().get()).getMetadata(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1));
    assertEquals(5, rows(table()).size());
    assertEquals(2, TextPipePreprocessor.OPENED.get(), "each object is prepared once");
  }

  private static List<String> chunks(Map<String, Row> rows, FixtureFile file) {
    return rows.values().stream().filter(row -> file.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)))
        .sorted((x, y) -> Long.compare(x.getAs(EmbeddingTableSchema.CHUNK_ID), y.getAs(EmbeddingTableSchema.CHUNK_ID)))
        .map(row -> row.<String>getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)).collect(Collectors.toList());
  }
}
