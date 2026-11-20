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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.UnstructuredFileSourceConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the unstructured DFS source end to end at the source level: per-record
 * inline vs out-of-line blob placement, parse/chunk columns, BLOB schema metadata
 * propagation to Avro, and checkpoint advancement.
 */
public class TestUnstructuredFileDFSSource extends UtilitiesTestBase {

  @TempDir
  static Path tempDir;

  @BeforeAll
  public static void setupOnce() throws Exception {
    initTestServices();
  }

  private TypedProperties props(long inlineMaxBytes) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.source.dfs.root", tempDir.toString());
    props.setProperty(UnstructuredFileSourceConfig.BLOB_INLINE_MAX_BYTES.key(), String.valueOf(inlineMaxBytes));
    return props;
  }

  @Test
  public void testInlineVsOutOfLineParseColumnsAndCheckpoint() throws IOException {
    Files.write(tempDir.resolve("small.txt"),
        "hudi unstructured ingest smoke text".getBytes(StandardCharsets.UTF_8));
    byte[] big = new byte[256];
    Arrays.fill(big, (byte) 'x');
    Files.write(tempDir.resolve("big.txt"), big);

    // threshold between the two files: 35-byte file inlines, 256-byte file goes out-of-line
    UnstructuredFileDFSSource source =
        new UnstructuredFileDFSSource(props(100), jsc, sparkSession, null);
    Pair<Option<Dataset<Row>>, Checkpoint> batch = source.fetchNextBatch(Option.empty(), Long.MAX_VALUE);
    assertTrue(batch.getLeft().isPresent());
    Dataset<Row> df = batch.getLeft().get();
    List<Row> rows = df.collectAsList();
    assertEquals(2, rows.size());

    Row small = rows.stream().filter(r -> r.getString(df.schema().fieldIndex("file_name"))
        .equals("small.txt")).findFirst().get();
    Row smallBlob = small.getStruct(df.schema().fieldIndex("content"));
    assertEquals(HoodieSchema.Blob.INLINE, smallBlob.getString(0));
    assertEquals(35, ((byte[]) smallBlob.get(1)).length);
    assertNull(smallBlob.get(2));
    assertEquals("SUCCESS", small.getString(df.schema().fieldIndex("parse_status")));
    assertTrue(small.getString(df.schema().fieldIndex("extracted_text")).contains("smoke"));
    assertFalse(small.getList(df.schema().fieldIndex("chunks")).isEmpty());
    assertEquals("txt", small.getString(df.schema().fieldIndex("extension")));

    Row bigRow = rows.stream().filter(r -> r.getString(df.schema().fieldIndex("file_name"))
        .equals("big.txt")).findFirst().get();
    Row bigBlob = bigRow.getStruct(df.schema().fieldIndex("content"));
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, bigBlob.getString(0));
    assertNull(bigBlob.get(1));
    Row reference = bigBlob.getStruct(2);
    assertTrue(reference.getString(0).endsWith("big.txt"));
    assertFalse(reference.getBoolean(3)); // managed=false: points at the original file in place
    assertNull(reference.get(1), "offset is unset, meaning the blob starts at byte 0");
    // the ingested size, so a reference that stops matching the file is detectable without
    // reading the blob - the only signal when a replacement preserves the modification time
    assertEquals(bigRow.getLong(df.schema().fieldIndex("size")), reference.getLong(2));
    // out-of-line files are still parsed (streamed from the source file)
    assertEquals("SUCCESS", bigRow.getString(df.schema().fieldIndex("parse_status")));

    // checkpoint advanced; an immediate re-fetch returns empty
    Checkpoint checkpoint = batch.getRight();
    assertNotNull(checkpoint);
    Pair<Option<Dataset<Row>>, Checkpoint> next = source.fetchNextBatch(Option.of(checkpoint), Long.MAX_VALUE);
    assertFalse(next.getLeft().isPresent());
  }

  @Test
  public void testBlobLogicalTypeSurvivesAvroConversion() {
    // The schema seam the whole design rests on: the BLOB metadata on the source's
    // StructType must convert into the Avro/Hoodie blob logical type.
    HoodieSchema hoodieSchema = org.apache.hudi.HoodieSchemaConversionUtils
        .convertStructTypeToHoodieSchema(UnstructuredFileDFSSource.SOURCE_SCHEMA, "hoodie_source", "hoodie.source");
    Schema avro = hoodieSchema.toAvroSchema();
    Schema content = resolveNullable(avro.getField("content").schema());
    LogicalType logicalType = content.getLogicalType();
    assertNotNull(logicalType, "content field lost the blob logical type");
    assertEquals(HoodieSchemaType.BLOB.name().toLowerCase(), logicalType.getName().toLowerCase());
  }

  private static Schema resolveNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst().get();
    }
    return schema;
  }
}
