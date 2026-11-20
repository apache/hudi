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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.PrepRoundCounters;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.balance.BalancingAssignment;
import org.apache.hudi.utilities.unstructured.embedding.balance.HashBalancingStrategy;
import org.apache.hudi.utilities.unstructured.embedding.balance.SizeTypeAwareBalancingStrategy;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Whole PDF objects balanced inside the single-writer prep round, with the same committed chunks as the hash strategy. */
@Tag("functional")
public class TestEmbeddingPrepBalancing extends EmbeddingRoundTestBase {

  private static final long PDF_BUDGET = 4096L;

  /** Captures the real driver's assignment for assertions after the stream commits; no production inspection API is needed. */
  public static class RecordingStrategy extends SizeTypeAwareBalancingStrategy {
    private static final long serialVersionUID = 1L;
    private static List<Row> assignment = Collections.emptyList();
    private static int partitions;

    @Override
    public BalancingAssignment assign(Dataset<Row> metadata, TypedProperties props) {
      assertFalse(Arrays.asList(metadata.columns()).contains(BlobColumns.BLOB), "only metadata reaches balancing");
      BalancingAssignment plan = super.assign(metadata, props);
      assignment = plan.getAssignment().orderBy(BalancingAssignment.PARTITION, BalancingAssignment.SOURCE_PATH_SHA).collectAsList();
      partitions = plan.getNumPartitions();
      return plan;
    }
  }

  @Test
  public void mixedPdfSizesHaveARecordedTaskPlanAndIdenticalCommittedChunks() throws Exception {
    List<FixtureFile> files = new ArrayList<>();
    for (int pages : new int[] {1, 2, 3, 4, 10, 20}) {
      files.add(object("pages-" + pages + ".pdf", PdfFixtures.text(Collections.nCopies(pages,
          "Alpha beta gamma delta epsilon zeta. Eta theta iota kappa lambda mu."))));
    }
    assertTrue(files.get(files.size() - 1).bytes.length >= PDF_BUDGET, "the fixture includes a solo large PDF");
    announce(files.toArray(new FixtureFile[0]));
    String sizedTable = table("size_balanced");
    String equalTable = table("equal_balanced");
    TypedProperties sized = props(sizedTable, EmbeddingTableConfig.MODE_PREP_ONLY,
        EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.key(), RecordingStrategy.class.getName(),
        EmbeddingTableConfig.BALANCE_PREFIX + "pdf.max.bytes.per.task", String.valueOf(PDF_BUDGET),
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    TypedProperties equal = props(equalTable, EmbeddingTableConfig.MODE_PREP_ONLY,
        EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.key(), HashBalancingStrategy.class.getName(),
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    try {
      PrepRoundCounters sizedPrep = PrepRoundCounters.listen(spark());
      EmbeddingStreamers.Round sizedRound;
      try {
        sizedRound = round(sized);
      } finally {
        sizedPrep.stop();
      }
      assertTrue(sizedRound.getCommit().isPresent(), sizedRound.toString());
      assertTrue(sizedRound.getErrors().isEmpty(), sizedRound.toString());
      List<Row> assignment = RecordingStrategy.assignment;
      assertEquals(files.size(), assignment.size());
      assertTrue(RecordingStrategy.partitions >= 2, "small PDFs still spread over the configured two cores");
      Set<String> expectedShas = files.stream().map(file -> file.sha).collect(Collectors.toSet());
      assertEquals(expectedShas, assignment.stream().map(row -> row.<String>getAs(BlobColumns.SOURCE_PATH_SHA))
          .collect(Collectors.toSet()));
      Map<Integer, List<Row>> tasks = assignment.stream().collect(Collectors.groupingBy(
          row -> row.<Integer>getAs(BalancingAssignment.PARTITION)));
      assertEquals(RecordingStrategy.partitions, tasks.size(), "no planned PDF task is empty");
      for (List<Row> task : tasks.values()) {
        long total = task.stream().mapToLong(row -> row.<Long>getAs(BlobColumns.SOURCE_FILE_SIZE)).sum();
        long largest = task.stream().mapToLong(row -> row.<Long>getAs(BlobColumns.SOURCE_FILE_SIZE)).max().getAsLong();
        assertTrue(total <= PDF_BUDGET + largest, task.toString());
        if (largest >= PDF_BUDGET) {
          assertEquals(1, task.size(), "a PDF at or above the budget owns its task");
        }
      }
      System.out.println("PREP_BALANCE_PLAN partitions=" + RecordingStrategy.partitions + " tasks=" + tasks);
      Map<String, Row> sizedRows = rows(sizedTable);
      assertEquals(expectedShas, sizedRows.values().stream()
          .map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)).collect(Collectors.toSet()));
      // the write collapses duplicate keys, so a file balanced into two tasks shows only in what prep did
      assertEquals(files.size(), sizedPrep.filesOk(), "every file prepared exactly once: " + sizedPrep);
      assertEquals(0, sizedPrep.filesFailed(), sizedPrep.toString());
      assertEquals(sizedRows.size(), sizedPrep.chunks(), "every committed row prepared exactly once: " + sizedPrep);
      assertTrue(sizedRows.values().stream().allMatch(row -> row.<String>getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)
          .contains("Alpha")), sizedRows.toString());
      assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(sizedTable, storage(), sizedRound.getCommit().get()));

      PrepRoundCounters equalPrep = PrepRoundCounters.listen(spark());
      EmbeddingStreamers.Round equalRound;
      try {
        equalRound = round(equal);
      } finally {
        equalPrep.stop();
      }
      assertTrue(equalRound.getCommit().isPresent(), equalRound.toString());
      assertEquals(files.size(), equalPrep.filesOk(), "every file prepared exactly once: " + equalPrep);
      assertEquals(sizedRows.size(), equalPrep.chunks(), "every committed row prepared exactly once: " + equalPrep);
      assertTrue(equalRound.getErrors().isEmpty(), equalRound.toString());
      assertEquals(rows(equalTable), sizedRows, "balancing changes tasks, never the committed content or version keys");
      assertEquals(1, EmbeddingStreamers.commits(sizedTable, storage()).size());
      assertEquals(1, EmbeddingStreamers.commits(equalTable, storage()).size());
    } finally {
      EmbeddingTableTransformer.releaseRound(sizedTable);
      EmbeddingTableTransformer.releaseRound(equalTable);
      RecordingStrategy.assignment = Collections.emptyList();
    }
  }

  @Test
  public void prepRoundKeepsBothSourceVersionsOfAPath() throws Exception {
    String target = table("versions");
    // the round's table exists before the transformer runs (the streamer starts the instant first); prep writes large payloads
    // under it
    TypedProperties props = props(target, EmbeddingTableConfig.MODE_PREP_ONLY,
        EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.key(), RecordingStrategy.class.getName());
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.embedding(props)).setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("versions")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS)).initTable(storage(), target);
    byte[] first = PdfFixtures.text("Alpha from the first version.");
    byte[] second = PdfFixtures.text("Beta from the second version.");
    Row v1 = sourceVersion(100L, first);
    Row v2 = sourceVersion(200L, second);
    Dataset<Row> batch = spark().createDataFrame(Arrays.asList(v1, v2), BlobTableSchema.schema());
    try {
      List<Row> prepared = new EmbeddingTableTransformer().apply(jsc(), spark(), batch, props, "20261031000000000")
          .orderBy(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME).collectAsList();
      assertEquals(2, prepared.size(), "one result per version, including both versions of a path");
      assertEquals(Arrays.asList(100L, 200L), prepared.stream()
          .map(row -> row.<Long>getAs(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)).collect(Collectors.toList()));
      assertEquals(Arrays.asList("Alpha from the first version.", "Beta from the second version."), prepared.stream()
          .map(row -> row.<String>getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)).collect(Collectors.toList()));
      assertEquals(2, RecordingStrategy.assignment.size());
    } finally {
      EmbeddingTableTransformer.releaseRound(target);
      RecordingStrategy.assignment = Collections.emptyList();
    }
  }

  private static Row sourceVersion(long modificationTime, byte[] bytes) {
    return RowFactory.create("same-path", modificationTime, "file:///source.pdf", "source.pdf", "pdf", (long) bytes.length,
        "application/pdf", RowFactory.create(HoodieSchema.Blob.INLINE, bytes, null));
  }

}
