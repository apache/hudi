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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepCounters;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepSideOutput;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.prep.TestPreprocessingIterator;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PrepRound#outputRows}: the side outputs of files that yield no chunk reach the partition's output as they are met, so
 * the queue between the prep iterator and the output holds a bounded number of them however long such a run of files is.
 */
public class TestPrepRoundOutputRows {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType BLOB_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, true)});
  /** Files of the run that yield no chunk: far more than the bound the output may hold back. */
  private static final int NO_CHUNK_FILES = 200;

  @TempDir
  Path temp;

  private String embeddingTable;

  @BeforeEach
  void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
  }

  @Test
  void runOfFilesWithoutChunksIsHandedOnAsItIsMet() throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        TestPreprocessingIterator.PipeSplitPreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    List<Row> source = new ArrayList<>();
    for (int i = 0; i < NO_CHUNK_FILES; i++) {
      // an extension no preprocessor prepares: an empty "no preprocessor" side output, no row
      source.add(blobRow("none-" + i, "bin", "unprepared"));
    }
    source.add(blobRow("text", "txt", "alpha"));
    AtomicInteger read = new AtomicInteger();
    Iterator<Row> counted = new Iterator<Row>() {
      private final Iterator<Row> rows = source.iterator();

      @Override
      public boolean hasNext() {
        return rows.hasNext();
      }

      @Override
      public Row next() {
        read.incrementAndGet();
        return rows.next();
      }
    };
    StructType iteratorSchema = EmbeddingTableSchema.build(config);
    StructType outputSchema = iteratorSchema.add(DataTypes.createStructField(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true));
    Iterator<Row> output = outputRows(counted, config, props, outputSchema, iteratorSchema);

    int corrupt = outputSchema.fieldIndex(PrepRound.CORRUPT_RECORD);
    List<String> errorShas = new ArrayList<>();
    int errorRows = 0;
    int maxReadAhead = 0;
    List<Row> rows = new ArrayList<>();
    while (output.hasNext()) {
      maxReadAhead = Math.max(maxReadAhead, read.get() - (errorRows + rows.size()));
      Row row = output.next();
      if (row.get(corrupt) != null) {
        JsonNode error = JSON.readTree(row.getString(corrupt));
        assertEquals(PrepSideOutput.NO_PREPROCESSOR, error.get("error").textValue(), row.getString(corrupt));
        assertTrue(rows.isEmpty(), "every error row comes before the chunk row of the later file");
        errorShas.add(error.get("source_path_sha").textValue());
        errorRows++;
      } else {
        rows.add(row);
      }
    }
    // the same output as before: one error row per file without chunks, then the text file's chunk row
    assertEquals(NO_CHUNK_FILES, errorRows);
    List<String> expectedShas = new ArrayList<>();
    for (int i = 0; i < NO_CHUNK_FILES; i++) {
      expectedShas.add("none-" + i);
    }
    assertEquals(expectedShas, errorShas, "one error row per file without chunks, in source order");
    assertEquals(1, rows.size());
    Row chunk = rows.get(0);
    assertNull(chunk.get(corrupt));
    assertEquals("text", chunk.get(outputSchema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)));
    assertEquals("alpha", chunk.get(outputSchema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)));
    assertFalse(output.hasNext());
    // Every held side output belongs to a source row read but not yet output, so this also bounds the queue.
    assertTrue(maxReadAhead <= 1, "source rows read ahead of the output: " + maxReadAhead);
  }

  /** Ordinary text rows preserve their data and ordering when next() is called without a preceding hasNext(). */
  @Test
  void ordinaryTextRowsSupportDirectNextWithoutHasNext() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        TestPreprocessingIterator.PipeSplitPreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType iteratorSchema = EmbeddingTableSchema.build(config);
    StructType outputSchema = iteratorSchema.add(DataTypes.createStructField(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true));
    Iterator<Row> output = outputRows(Arrays.asList(blobRow("first", "txt", "alpha|beta"),
        blobRow("second", "txt", "gamma")).iterator(), config, props, outputSchema, iteratorSchema);

    int corrupt = outputSchema.fieldIndex(PrepRound.CORRUPT_RECORD);
    int sha = outputSchema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA);
    int chunkId = outputSchema.fieldIndex(EmbeddingTableSchema.CHUNK_ID);
    int text = outputSchema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
    List<List<Object>> actual = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Row row = output.next();
      assertNull(row.get(corrupt));
      actual.add(Arrays.asList(row.get(sha), row.get(chunkId), row.get(text)));
    }
    assertEquals(Arrays.asList(Arrays.asList("first", 0L, "alpha"), Arrays.asList("first", 1L, "beta"),
        Arrays.asList("second", 0L, "gamma")), actual);
    assertFalse(output.hasNext());
    assertThrows(NoSuchElementException.class, output::next);
  }

  private static Iterator<Row> outputRows(Iterator<Row> source, EmbeddingTableConfig config, TypedProperties props,
                                          StructType outputSchema, StructType iteratorSchema) {
    return PrepRound.outputRows(source, config, new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())),
        new PreprocessorRegistry(props), new WordCountTokenizer(), PrepCounters.local(), "001", outputSchema,
        PrepRound.layoutOf(outputSchema, iteratorSchema), null);
  }

  private static Row blobRow(String sha, String extension, String text) {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, (long) bytes.length, 1234L, blob,
        "s3a://bucket/dir/" + sha + "." + extension, sha + "." + extension}, BLOB_ROW);
  }
}
