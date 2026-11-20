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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Video frame rows in {@link PreprocessingIterator}: a chunk carrying a {@link Chunk#getFrameIndex() frame index} is an image row
 * pinned at chunk_id = the index; a frame index at or below the file's last image chunk_id is a duplicate record key and fails
 * the file.
 */
public class TestVideoFrameRows {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType SOURCE_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false)});
  private static final byte[] FRAME = "jpeg-frame".getBytes(StandardCharsets.UTF_8);

  /** A video preprocessor scripted by the file's text: comma-separated frame indexes, one image frame per {@code <index>}. */
  public static class ScriptedFramePreprocessor implements FilePreprocessor {
    @Override
    public String name() {
      return "scripted-frames-v1";
    }

    @Override
    public SourceKind sourceKind() {
      return SourceKind.VIDEO;
    }

    @Override
    public boolean storesInputs() {
      return true;
    }

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
        throws IOException {
      String script;
      try (InputStream in = bytes.open()) {
        script = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
      }
      List<Chunk> chunks = new ArrayList<>();
      for (String frame : script.split(",")) {
        chunks.add(Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).frameIndex(Long.parseLong(frame))
            .payload(() -> new ByteArrayInputStream(FRAME), FRAME.length, "image/jpeg").build());
      }
      return ClosableIterator.wrap(chunks.iterator());
    }
  }

  @TempDir
  Path temp;

  private String embeddingTable;

  @BeforeEach
  public void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
  }

  private PreprocessingIterator iterator(String script) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), ScriptedFramePreprocessor.class.getName());
    byte[] bytes = script.getBytes(StandardCharsets.UTF_8);
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    Row source = new GenericRowWithSchema(new Object[] {"sha-video", "mp4", (long) bytes.length, 1L, blob, "s3://bucket/sha-video.mp4", "sha-video.mp4"}, SOURCE_ROW);
    return new PreprocessingIterator(Collections.singletonList(source).iterator(), EmbeddingTableConfig.from(props),
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), new PreprocessorRegistry(props),
        new WordCountTokenizer(), counters(), null);
  }

  private static PrepCounters counters() {
    return new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator());
  }

  private static List<Row> drain(PreprocessingIterator iterator) {
    List<Row> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }
    return rows;
  }

  private static Object column(Row row, String name) {
    return row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(name));
  }

  private static String errors(PreprocessingIterator iterator) {
    StringBuilder all = new StringBuilder();
    iterator.getSideOutputs().forEach(output -> all.append(output.getError()).append('\n'));
    return all.toString();
  }

  @Test
  public void framesArePinnedAtTheirIndexAndStoreTheirImageInline() {
    PreprocessingIterator iterator = iterator("0,1,2");
    List<Row> rows = drain(iterator);

    assertEquals(3, rows.size(), errors(iterator));
    for (int i = 0; i < 3; i++) {
      assertEquals((long) i, column(rows.get(i), EmbeddingTableSchema.CHUNK_ID));
      assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, column(rows.get(i), EmbeddingTableSchema.MODALITY));
      Row prepared = (Row) column(rows.get(i), EmbeddingTableSchema.PREPARED_IMAGE);
      assertEquals(INLINE, prepared.getString(0));
      assertArrayEquals(FRAME, (byte[]) prepared.get(1));
    }
    assertTrue(iterator.getSideOutputs().isEmpty(), errors(iterator));
  }

  /** A frame index that does not increase is a duplicate record key: the file fails, and its frames before it are kept. */
  @Test
  public void repeatedFrameIndexFailsTheFileAsADuplicateRecordKey() {
    PreprocessingIterator iterator = iterator("0,1,1");
    List<Row> rows = drain(iterator);

    assertEquals(2, rows.size(), errors(iterator));
    assertEquals(0L, column(rows.get(0), EmbeddingTableSchema.CHUNK_ID));
    assertEquals(1L, column(rows.get(1), EmbeddingTableSchema.CHUNK_ID));
    assertEquals(1, iterator.getSideOutputs().size(), errors(iterator));
    assertEquals(PrepSideOutput.STAGE_PREP, iterator.getSideOutputs().get(0).getStage());
    assertTrue(errors(iterator).contains("Duplicate record key"), errors(iterator));
  }

  @Test
  public void negativeFrameIndexIsRefusedByTheBuilder() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).frameIndex(-1));
    assertTrue(e.getMessage().contains("frame index must be >= 0"), e.getMessage());
    assertEquals(Long.valueOf(4L), Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).frameIndex(4).build().getFrameIndex());
    assertNull(Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).build().getFrameIndex());
  }

  /** Every video extension, in any letter case, resolves to the configured video preprocessor. */
  @Test
  public void everyVideoExtensionResolvesToTheVideoPreprocessor() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), ScriptedFramePreprocessor.class.getName());
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    for (String extension : Arrays.asList("mp4", "MOV", "mkv", "webm", "avi")) {
      assertTrue(registry.forExtension(extension).get() instanceof ScriptedFramePreprocessor, extension);
    }
  }
}
