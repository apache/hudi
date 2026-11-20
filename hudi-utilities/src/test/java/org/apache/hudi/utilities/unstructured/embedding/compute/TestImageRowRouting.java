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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The compute side of the media input contract in {@link MicroBatchEmbeddingIterator}: an image row reaches a
 * {@link MultimodalEmbeddingClient} as its stored input ({@code prepared_image}), and a client that embeds text only is never
 * called for one: the row gets a PERMANENT failure naming the provider, while its text batch-mates are embedded.
 */
public class TestImageRowRouting {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add(EmbeddingTableSchema.preparedImageField())
      .add("emb_m", DataTypes.createArrayType(DataTypes.FloatType), true)
      .add(ComputeRound.ERROR_WORKING_COLUMN, DataTypes.StringType, true);
  private static final int VECTOR = 7;
  private static final int ERROR = 8;
  private static final long MODIFICATION_TIME = 1000L;
  private static final long FILE_SIZE = 7L;
  private static final List<EmbeddingInput> SEEN = new ArrayList<>();

  /** A text-only client: it records every input it is called with. */
  public static class TextOnlyClient implements EmbeddingClient {
    private EmbeddingModelSpec model;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      model = spec;
    }

    @Override
    public EmbeddingModelSpec spec() {
      return model;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      List<EmbeddingResult> results = new ArrayList<>();
      for (EmbeddingInput input : inputs) {
        SEEN.add(input);
        results.add(EmbeddingResult.ok(new float[] {1, 2}));
      }
      return results;
    }
  }

  /** A multimodal client: an image input's vector is its stored input's length and first byte. */
  public static class ImageClient extends TextOnlyClient implements MultimodalEmbeddingClient {
    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      List<EmbeddingResult> results = new ArrayList<>();
      for (EmbeddingInput input : inputs) {
        SEEN.add(input);
        if (input.storedImage() == null) {
          results.add(EmbeddingResult.ok(new float[] {1, 2}));
          continue;
        }
        try (InputStream in = input.storedImage().open()) {
          byte[] bytes = in.readAllBytes();
          results.add(EmbeddingResult.ok(new float[] {bytes.length, bytes[0]}));
        } catch (Exception e) {
          results.add(EmbeddingResult.permanent(e.toString()));
        }
      }
      return results;
    }
  }

  private static Row textRow(String sha) {
    return new GenericRowWithSchema(new Object[] {sha, MODIFICATION_TIME, FILE_SIZE, 0L, EmbeddingTableSchema.MODALITY_TEXT, "words", null, null, null}, SCHEMA);
  }

  private static Row imageRow(String sha, byte[] bytes) {
    Row prepared = bytes == null ? null : new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, MODIFICATION_TIME, FILE_SIZE, 0L, EmbeddingTableSchema.MODALITY_IMAGE, null, prepared, null, null}, SCHEMA);
  }

  private static List<Row> drain(Class<?> client, BlobBytesSource storedInputs, Row... rows) {
    SEEN.clear();
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "2");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX, client.getName());
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    props.setProperty(EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), "0");
    EmbeddingModelSpec model = EmbeddingTableConfig.from(props).model("m");
    ComputeCounters counters = new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    List<Row> out = new ArrayList<>();
    try (MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(Arrays.asList(rows).iterator(), model, props,
        SCHEMA, 8, counters, storedInputs)) {
      iterator.forEachRemaining(out::add);
    }
    return out;
  }

  private static BlobBytesSource storedInputs() {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
  }

  @Test
  public void textOnlyClientIsNeverCalledForAnImageRowAndTheRowFailsForGood() {
    List<Row> rows = drain(TextOnlyClient.class, storedInputs(), textRow("a"), imageRow("b", new byte[] {9}), textRow("c"));

    assertEquals(3, rows.size());
    assertEquals(Arrays.asList("a", "c"), Arrays.asList(SEEN.get(0).getSourcePathSha(), SEEN.get(1).getSourcePathSha()),
        "only the text rows reach the client");
    assertEquals(2, SEEN.size());
    assertNotNull(rows.get(0).get(VECTOR));
    assertNull(rows.get(0).get(ERROR));
    assertNull(rows.get(1).get(VECTOR), "the image row has no vector");
    String error = rows.get(1).getString(ERROR);
    assertTrue(error.startsWith("EMBED-COMPUTE-IMAGE-NOT-SUPPORTED model=m provider=dummy"), error);
    assertTrue(error.contains("embeds text only"), error);
    assertNotNull(rows.get(2).get(VECTOR));
  }

  @Test
  public void batchOfImageRowsOnlyNeverCallsATextOnlyClient() {
    List<Row> rows = drain(TextOnlyClient.class, storedInputs(), imageRow("a", new byte[] {1}), imageRow("b", new byte[] {2}));

    assertTrue(SEEN.isEmpty());
    rows.forEach(row -> assertTrue(row.getString(ERROR).startsWith("EMBED-COMPUTE-IMAGE-NOT-SUPPORTED"), row.getString(ERROR)));
  }

  @Test
  public void multimodalClientReadsTheStoredInput() {
    List<Row> rows = drain(ImageClient.class, storedInputs(), imageRow("a", new byte[] {7, 8, 9}), textRow("b"));

    assertEquals(2, SEEN.size());
    EmbeddingInput image = SEEN.get(0);
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, image.getModality());
    assertEquals(MODIFICATION_TIME, image.getSourceModificationTime(), "the image input carries its source version");
    assertEquals(FILE_SIZE, image.getSourceFileSize(), "the image input carries its source version");
    assertNull(image.getText());
    assertNotNull(image.storedImage());
    assertNull(SEEN.get(1).storedImage(), "a text row has no stored input");
    assertArrayEquals(new float[] {3, 7}, (float[]) rows.get(0).get(VECTOR));
    assertNull(rows.get(0).get(ERROR));
  }

  @Test
  public void imageRowWithoutAPreparedImageBreaksTheInvariant() {
    EmbeddingComputePlatformException failure = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(ImageClient.class, storedInputs(), imageRow("a", null)));
    assertTrue(failure.getMessage().startsWith("EMBED-COMPUTE-NO-STORED-IMAGE model=m "), failure.getMessage());
    assertTrue(failure.getMessage().contains("source_path_sha=a source_modification_time=" + MODIFICATION_TIME + " source_file_size="
        + FILE_SIZE + " modality=" + EmbeddingTableSchema.MODALITY_IMAGE + " chunk_id=0:"), "the full key names the row: " + failure.getMessage());
  }
}
