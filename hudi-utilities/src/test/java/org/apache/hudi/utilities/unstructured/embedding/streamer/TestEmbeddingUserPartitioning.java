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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A path partition column follows each prepared chunk into the user's embedding table partition. */
@Tag("functional")
public class TestEmbeddingUserPartitioning extends EmbeddingRoundTestBase {

  @Test
  public void userPathPartitionColumnsPartitionTheEmbeddingTable() throws Exception {
    FixtureFile blue = text("team=blue/a", "alpha", "beta");
    FixtureFile red = text("team=red/b", "gamma");
    announce(blue, red);
    Instant blueTime = Instant.parse("2026-10-30T01:00:00Z");
    Instant redTime = Instant.parse("2026-10-30T02:00:00Z");
    touch(blue, blueTime);
    touch(red, redTime);
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_ONLY,
        "hoodie.streamer.source.cloud.data.partition.fields.from.path", "team",
        "hoodie.datasource.write.partitionpath.field", "team",
        "hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator");

    Round round = round(props);

    assertTrue(round.getErrors().isEmpty(), round.toString());
    assertTrue(round.getCommit().isPresent(), round.toString());
    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(3, round.getRowsWritten(), round.toString());
    String blueKey = "source_path_sha:" + blue.sha + ",source_modification_time:" + blueTime.toEpochMilli()
        + ",source_file_size:10,modality:text,chunk_id:";
    String redKey = "source_path_sha:" + red.sha + ",source_modification_time:" + redTime.toEpochMilli()
        + ",source_file_size:5,modality:text,chunk_id:";
    Map<String, Row> rows = rows(table());
    assertEquals(new HashSet<>(Arrays.asList(blueKey + "0", blueKey + "1", redKey + "0")), rows.keySet(),
        "the stored key contains exactly five fields, independently of the team partition");

    List<Row> stored = spark().read().format("hudi").load(table()).select(
        HoodieRecord.RECORD_KEY_METADATA_FIELD, EmbeddingTableSchema.SOURCE_PATH_SHA,
        EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, EmbeddingTableSchema.SOURCE_FILE_SIZE,
        EmbeddingTableSchema.MODALITY, EmbeddingTableSchema.CHUNK_ID, EmbeddingTableSchema.SOURCE_FILE_PATH,
        EmbeddingTableSchema.SOURCE_FILE_NAME, EmbeddingTableSchema.SOURCE_FILE_EXTENSION,
        EmbeddingTableSchema.PREPARED_TEXT_CHUNK, EmbeddingTableSchema.CHUNK_TOKEN_COUNT,
        "team", HoodieRecord.PARTITION_PATH_METADATA_FIELD).collectAsList();
    assertEquals(new HashSet<>(Arrays.asList(
        RowFactory.create(blueKey + "0", blue.sha, blueTime.toEpochMilli(), 10L, "text", 0L,
            blue.sourcePath, "a." + TEXT_EXTENSION, TEXT_EXTENSION, "alpha", 1, "blue", "blue"),
        RowFactory.create(blueKey + "1", blue.sha, blueTime.toEpochMilli(), 10L, "text", 1L,
            blue.sourcePath, "a." + TEXT_EXTENSION, TEXT_EXTENSION, "beta", 1, "blue", "blue"),
        RowFactory.create(redKey + "0", red.sha, redTime.toEpochMilli(), 5L, "text", 0L,
            red.sourcePath, "b." + TEXT_EXTENSION, TEXT_EXTENSION, "gamma", 1, "red", "red"))),
        new HashSet<>(stored));
    for (Row row : rows.values()) {
      assertTrue(row.getJavaMap(row.fieldIndex(EmbeddingTableSchema.AUX)).isEmpty(), row.toString());
    }
    for (Row row : spark().read().format("hudi").load(table()).select(
        HoodieRecord.PARTITION_PATH_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD).collectAsList()) {
      assertTrue(Files.isRegularFile(localPath(table()).resolve(row.getString(0)).resolve(row.getString(1))),
          "the stored partition path names the actual data file: " + row);
    }
    assertEquals("team", EmbeddingStreamers.metaClient(table(), storage()).getTableConfig().getPartitionFieldProp());
    assertEquals(Collections.singletonList(round.getCommit().get()), EmbeddingStreamers.commits(table(), storage()));
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
  }
}
