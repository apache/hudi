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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.image.PreparedImage;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepCounters;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Embedding-compute outcomes of image rows read from their STORED input ({@code prepared_image}) by a multimodal client, through
 * {@link MicroBatchEmbeddingIterator}: a readable stored input is decoded, resized and embedded (image -> resized -> embedding);
 * a row whose {@code prepared_image} is NULL fails the step (prep never writes an image row without its stored input, so the
 * table's invariant is broken, not the row's input); a stored input whose file is confirmed gone is PERMANENT for its row
 * while its batch-mates embed; a stored input whose storage read fails transiently is RETRYABLE, retried in the step and,
 * still failing after {@code compute.max.attempts} tries, fails the task. Without Spark: the iterator over hand-built rows in
 * the table schema.
 */
class TestStoredImageOutcomes {

  private static final String MODEL = "m1";
  private static final int DIM = 4;
  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
  private static final String EMBEDDED = "EMBEDDED";
  private static final String SKIPPED = "SKIPPED ";
  private static final String PENDING = "PENDING";

  @TempDir
  Path base;

  @ParameterizedTest
  @CsvSource({"dummy-multimodal,'text,image',true", "dummy,text,false"})
  void builtInProviderAdmitsOnlyItsSupportedImageRows(String provider, String modalities, boolean imageSupported) {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, provider);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, modalities);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    ComputeCounters counters = counters();

    Row result = drain(config, props, schema, counters, storedInputs(),
        row(schema, "image", inline(ImageFixtures.image("png", 8, 6)))).get(0);

    if (imageSupported) {
      assertEquals(EMBEDDED, outcome(result, schema, config));
      assertEquals(DIM, ((float[]) result.get(schema.fieldIndex(config.model(MODEL).getColumn()))).length);
      assertEquals(1L, counters.getRowsEmbedded().value());
      assertEquals(0L, counters.getRowsPermanent().value());
    } else {
      assertTrue(outcome(result, schema, config).startsWith(SKIPPED));
      assertEquals(0L, counters.getRowsEmbedded().value());
      assertEquals(1L, counters.getRowsPermanent().value());
    }
  }

  @Test
  void sourceImageChangedBetweenComputeHeaderAndDecodeIsSkipped() throws Exception {
    TypedProperties props = sourceImageProps(false);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    Path file = base.resolve("source.png");
    Files.write(file, png);
    Files.setLastModifiedTime(file, FileTime.fromMillis(1000L));
    Row prepared = prepareSourceImage(props, schema, file, png.length);
    AtomicInteger opens = new AtomicInteger();
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())) {
      @Override
      public SeekableBlob openSeekable(Row blob) throws IOException {
        SeekableBlob input = super.openSeekable(blob);
        if (opens.incrementAndGet() > 1) {
          return input;
        }
        return new SeekableBlob() {
          @Override
          public long length() {
            return input.length();
          }

          @Override
          public long getPos() throws IOException {
            return input.getPos();
          }

          @Override
          public void seek(long position) throws IOException {
            input.seek(position);
          }

          @Override
          public int read(byte[] bytes, int offset, int length) throws IOException {
            return input.read(bytes, offset, length);
          }

          @Override
          public void close() throws IOException {
            input.close();
            Files.setLastModifiedTime(file, FileTime.fromMillis(2000L));
          }
        };
      }
    };
    ComputeCounters counters = counters();

    Row out = drain(config, props, schema, counters, source, prepared).get(0);

    assertTrue(outcome(out, schema, config).startsWith(SKIPPED), outcome(out, schema, config));
    assertTrue(outcome(out, schema, config).contains("BlobSourceChangedException"), outcome(out, schema, config));
    assertEquals(1, opens.get(), "the second open must check the listed version before decoding");
    assertEquals(0L, counters.getRowsEmbedded().value());
    assertEquals(1L, counters.getRowsPermanent().value());
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void sourceImageChangedAfterPrepIsSkippedUnderItsOriginalKey(boolean rawInput, boolean sizeChanged) throws Exception {
    TypedProperties props = sourceImageProps(rawInput);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    Path file = base.resolve("source.png");
    Files.write(file, png);
    Files.setLastModifiedTime(file, FileTime.fromMillis(1000L));
    Row prepared = prepareSourceImage(props, schema, file, png.length);
    // Prep has finished successfully; compute still holds the raw reference and opens it lazily.
    Files.write(file, sizeChanged ? ImageFixtures.image("png", 80, 60) : png);
    Files.setLastModifiedTime(file, FileTime.fromMillis(sizeChanged ? 1000L : 2000L));
    ComputeCounters counters = counters();

    Row out = drain(config, props, schema, counters, storedInputs(), prepared).get(0);

    String outcome = outcome(out, schema, config);
    assertTrue(outcome.startsWith(SKIPPED), outcome);
    assertTrue(outcome.contains("BlobSourceChangedException") && outcome.contains("changed since it was listed"), outcome);
    assertTrue(outcome.contains("source_path_sha=source source_modification_time=1000 source_file_size=" + png.length
        + " modality=image chunk_id=0"), outcome);
    assertEquals(0L, counters.getRowsEmbedded().value());
    assertEquals(1L, counters.getRowsPermanent().value());
    assertEquals(0L, counters.getRowsRetried().value());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unchangedSourceImageAfterPrepStillEmbeds(boolean rawInput) throws Exception {
    TypedProperties props = sourceImageProps(rawInput);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    Path file = base.resolve("source.png");
    Files.write(file, png);
    Files.setLastModifiedTime(file, FileTime.fromMillis(1000L));
    Row prepared = prepareSourceImage(props, schema, file, png.length);
    ComputeCounters counters = counters();

    Row out = drain(config, props, schema, counters, storedInputs(), prepared).get(0);

    byte[] expected = rawInput ? png : new ImagePreparer(config.imagePreprocessing()).prepare(ImageFixtures.source(png)).getEncoded();
    assertEquals(EMBEDDED, outcome(out, schema, config));
    assertArrayEquals(DummyMultimodalEmbeddingClient.vectorForImage(expected, DIM),
        (float[]) out.get(schema.fieldIndex(config.model(MODEL).getColumn())));
    assertEquals(1L, counters.getRowsEmbedded().value());
    assertEquals(0L, counters.getRowsPermanent().value());
    assertEquals(0L, counters.getRowsRetried().value());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unmanagedRangeKeepsItsOwnBytesRegardlessOfSourceVersion(boolean rawInput) throws Exception {
    TypedProperties props = sourceImageProps(rawInput);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    byte[] padded = new byte[png.length + 10];
    System.arraycopy(png, 0, padded, 7, png.length);
    Path file = base.resolve("container.bin");
    Files.write(file, padded);
    Files.setLastModifiedTime(file, FileTime.fromMillis(9000L));
    ComputeCounters counters = counters();
    // This row's source version belongs to the original input, not to the ranged payload's container.
    Row input = row(schema, "range", reference(file, 7L, (long) png.length), 1000L, 1L);

    Row out = drain(config, props, schema, counters, storedInputs(), input).get(0);

    byte[] expected = rawInput ? png : new ImagePreparer(config.imagePreprocessing()).prepare(ImageFixtures.source(png)).getEncoded();
    assertEquals(EMBEDDED, outcome(out, schema, config));
    assertArrayEquals(DummyMultimodalEmbeddingClient.vectorForImage(expected, DIM),
        (float[]) out.get(schema.fieldIndex(config.model(MODEL).getColumn())));
    assertEquals(1L, counters.getRowsEmbedded().value());
    assertEquals(0L, counters.getRowsPermanent().value());
  }

  /** A client using the public raw stored-input supplier instead of the resized image supplier. */
  public static class RawStoredImageClient extends DummyMultimodalEmbeddingClient {
    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      List<EmbeddingResult> results = new ArrayList<>();
      for (EmbeddingInput input : inputs) {
        try {
          byte[] bytes;
          try (InputStream in = input.storedImage().open()) {
            bytes = in.readAllBytes();
          }
          results.add(EmbeddingResult.ok(vectorForImage(bytes, DIM)));
        } catch (IOException | RuntimeException failure) {
          results.add(ImageFailures.result(input, failure));
        }
      }
      return results;
    }
  }

  private TypedProperties sourceImageProps(boolean rawInput) {
    TypedProperties props = props();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), base.resolve("table").toUri().toString());
    if (rawInput) {
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
          RawStoredImageClient.class.getName());
    }
    return props;
  }

  private Row prepareSourceImage(TypedProperties props, StructType schema, Path file, int length) {
    StructType sourceSchema = new StructType()
        .add(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType).add(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType)
        .add(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType).add(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType)
        .add(BlobColumns.BLOB, BLOB).add(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType)
        .add(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType);
    Row source = new GenericRowWithSchema(new Object[] {"source", "png", (long) length, 1000L, reference(file),
        file.toUri().toString(), "source.png"}, sourceSchema);
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    try (PreprocessingIterator prep = PreprocessingIterator.builder(Collections.singletonList(source).iterator(),
        EmbeddingTableConfig.from(props), "001").bytesSource(storedInputs()).registry(new PreprocessorRegistry(props))
        .tokenizer(new WordCountTokenizer()).counters(counters).build()) {
      List<Row> rows = new ArrayList<>();
      prep.forEachRemaining(rows::add);
      assertEquals(1, rows.size(), prep.getSideOutputs().toString());
      assertTrue(prep.getSideOutputs().isEmpty(), prep.getSideOutputs().toString());
      Object[] values = new Object[schema.size()];
      for (int index = 0; index < rows.get(0).size(); index++) {
        values[index] = rows.get(0).get(index);
      }
      return new GenericRowWithSchema(values, schema);
    }
  }

  @Test
  void storedImageRowIsEmbeddedFromItsPreparedImageAndANullStoredInputFailsTheStep() throws Exception {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 1200, 900);
    Path file = base.resolve("a.png");
    Files.write(file, png);
    ComputeCounters counters = counters();
    List<Row> out = drain(config, props, schema, counters, storedInputs(), row(schema, "a0", inline(png)),
        row(schema, "a1", reference(file), Files.getLastModifiedTime(file).toMillis(), png.length));
    assertEquals(2, out.size());
    PreparedImage expected = new ImagePreparer(config.imagePreprocessing()).prepare(ImageFixtures.source(png));
    assertEquals(512, expected.getWidth(), "the model input is the resized image");
    float[] vector = DummyMultimodalEmbeddingClient.vectorForImage(expected.getEncoded(), DIM);
    for (Row row : out) {
      assertEquals(EMBEDDED, outcome(row, schema, config));
      assertArrayEquals(vector, (float[]) row.get(schema.fieldIndex(config.model(MODEL).getColumn())),
          "inline and referenced stored inputs of the same image give the same vector");
    }
    assertEquals(2L, counters.getRowsEmbedded().value());

    // NULL prepared_image: the step fails before any call, no row is written as an error
    ComputeCounters nullCounters = counters();
    BlobBytesSource storedInputs = storedInputs();
    Row a0 = row(schema, "a0", inline(png));
    Row b0 = row(schema, "b0", null);
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(config, props, schema, nullCounters, storedInputs, a0, b0));
    assertTrue(e.getMessage().startsWith("EMBED-COMPUTE-NO-STORED-IMAGE"), e.getMessage());
    assertTrue(e.getMessage().contains("source_path_sha=b0 source_modification_time=1000 source_file_size=1 modality=image chunk_id=0"),
        e.getMessage());
    assertEquals(0L, nullCounters.getRowsPermanent().value());
    assertEquals(0L, nullCounters.getBatchesOk().value() + nullCounters.getBatchesFailed().value(), "no client call");
  }

  /**
   * A stored input the input store wrote into the embedding table's own {@code .hoodie/.blob} folder is a managed reference
   * RELATIVE to the table base: compute, reading {@code prepared_image} with the table's base path, opens it there (in its range of
   * a larger file) and embeds the image it holds, giving the vector of the same image stored inline.
   */
  @Test
  void managedRelativeStoredInputIsReadUnderTheTableBaseAndEmbedded() throws Exception {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 1200, 900);
    byte[] padded = new byte[png.length + 7];
    System.arraycopy(png, 0, padded, 5, png.length);
    String relative = ".hoodie/.blob/p1/frames.bin";
    Path table = base.resolve("table");
    Files.createDirectories(table.resolve(".hoodie/.blob/p1"));
    Files.write(table.resolve(relative), padded);
    ComputeCounters counters = counters();
    List<Row> out = drain(config, props, schema, counters,
        new BlobBytesSource(table.toUri().toString(), HadoopFSUtils.getStorageConf(new Configuration())),
        row(schema, "r0", managedRelative(relative, 5L, png.length)), row(schema, "i0", inline(png)));
    PreparedImage expected = new ImagePreparer(config.imagePreprocessing()).prepare(ImageFixtures.source(png));
    float[] vector = DummyMultimodalEmbeddingClient.vectorForImage(expected.getEncoded(), DIM);
    assertEquals(2, out.size());
    for (Row row : out) {
      assertEquals(EMBEDDED, outcome(row, schema, config), row.getString(schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)));
      assertArrayEquals(vector, (float[]) row.get(schema.fieldIndex(config.model(MODEL).getColumn())),
          "the relative stored input embeds as the same image stored inline");
    }
    assertEquals(2L, counters.getRowsEmbedded().value());
    assertEquals(0L, counters.getRowsPermanent().value());
  }

  /**
   * A stored input whose file is gone (the source file was deleted after prep) is the row's own fault, the same on every read:
   * PERMANENT, its error naming the row, while the readable batch-mate and a refused image's batch-mate embed in the same call.
   */
  @Test
  void goneStoredFileAndUndecodableImageArePermanentForTheirRowOnly() {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    ComputeCounters counters = counters();
    Map<String, String> outcomes = outcomes(schema, config, drain(config, props, schema, counters, storedInputs(),
        row(schema, "a0", inline(ImageFixtures.image("png", 40, 30))),
        row(schema, "gone", reference(base.resolve("deleted.png"))),
        row(schema, "garbage", inline(ImageFixtures.garbage()))));
    assertEquals(EMBEDDED, outcomes.get("a0"));
    assertTrue(outcomes.get("gone").startsWith(SKIPPED + "stored input of source_path_sha=gone source_modification_time=1000 "
        + "source_file_size=1 modality=image chunk_id=0 cannot be read: FileNotFoundException"), outcomes.get("gone"));
    assertTrue(outcomes.get("garbage").startsWith(SKIPPED), outcomes.get("garbage"));
    assertEquals(1L, counters.getBatchesOk().value(), "one call for the whole batch");
    assertEquals(0L, counters.getRowsRetried().value());
    assertEquals(2L, counters.getRowsPermanent().value());
  }

  /**
   * A stored input whose storage read fails transiently is RETRYABLE: tried again alone until its {@code compute.max.attempts}
   * tries are spent, then the task fails with {@link EmbeddingComputeTransientException}. No row is ever yielded pending, nor
   * skipped for good.
   */
  @Test
  void storedInputWhoseStorageFailsIsRetriedAndThenFailsTheTask() throws Exception {
    TypedProperties props = props();
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    Path file = base.resolve("flaky.png");
    Files.write(file, ImageFixtures.image("png", 40, 30));
    ComputeCounters counters = counters();
    List<Row> yielded = new ArrayList<>();
    FlakyStoredInputs flaky = new FlakyStoredInputs();
    Row a0 = row(schema, "a0", inline(ImageFixtures.image("png", 40, 30)));
    Row d0 = row(schema, "d0", reference(file), Files.getLastModifiedTime(file).toMillis(), Files.size(file));
    EmbeddingComputeTransientException thrown = assertThrows(EmbeddingComputeTransientException.class,
        () -> drainInto(yielded, config, props, schema, counters, flaky, a0, d0));
    assertTrue(thrown.getMessage().contains("EMBED-COMPUTE-RETRIES-EXHAUSTED") && thrown.getMessage().contains("first source_path_sha=d0")
        && thrown.getMessage().contains("after " + EmbeddingTableConfig.DEFAULT_COMPUTE_MAX_ATTEMPTS + " tries")
        && thrown.getMessage().contains(FlakyStoredInputs.FAILURE), thrown.getMessage());
    assertEquals(Collections.emptyList(), yielded, "no row of the failing batch is yielded");
    assertEquals(1L, counters.getBatchesOk().value(), "the batch call answered d0 RETRYABLE, a0 OK");
    assertEquals(EmbeddingTableConfig.DEFAULT_COMPUTE_MAX_ATTEMPTS - 1L, counters.getRowsRetried().value(), "d0 alone, until its "
        + "tries were spent");
    assertEquals(0L, counters.getRowsEmbedded().value() + counters.getRowsPermanent().value(),
        "no outcome is counted for a batch that failed the task");
  }

  /**
   * A platform failure a client raises inside its call (a broken prep/compute invariant) is not retried and not reported as a
   * transient: the iterator rethrows it at once, as it does a configuration rejection, so the compute commit fails.
   */
  @Test
  void platformFailureOfTheClientIsNotRetried() {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        PlatformFailingClient.class.getName());
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    ComputeCounters counters = counters();
    BlobBytesSource storedInputs = storedInputs();
    Row a0 = row(schema, "a0", inline(png));
    Row b0 = row(schema, "b0", inline(png));
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(config, props, schema, counters, storedInputs, a0, b0));
    assertEquals(PlatformFailingClient.MESSAGE, e.getMessage());
    assertEquals(0L, counters.getRowsRetried().value(), "a platform failure is never retried");
    assertEquals(0L, counters.getRowsPermanent().value());
  }

  /**
   * The same platform failure raised on the single-input retry (the batch call failed generically, so each input is tried
   * alone) is rethrown at once too: the run fails with it after that one retry, never as a transient.
   */
  @Test
  void platformFailureOnTheSingleInputRetryIsNotRetriedFurther() {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX,
        PlatformFailingAloneClient.class.getName());
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType schema = schema(config);
    byte[] png = ImageFixtures.image("png", 40, 30);
    ComputeCounters counters = counters();
    BlobBytesSource storedInputs = storedInputs();
    Row a0 = row(schema, "a0", inline(png));
    Row b0 = row(schema, "b0", inline(png));
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
        () -> drain(config, props, schema, counters, storedInputs, a0, b0));
    assertEquals(PlatformFailingClient.MESSAGE, e.getMessage());
    assertEquals(1L, counters.getRowsRetried().value(), "only the first input's single retry ran");
  }

  /** A multimodal client whose batch call fails generically and whose single-input call fails on a broken invariant. */
  public static final class PlatformFailingAloneClient extends PlatformFailingClient {
    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      if (inputs.size() > 1) {
        throw new IllegalStateException("batch call failed (simulated)");
      }
      return super.embed(inputs);
    }
  }

  /** A multimodal client whose every call fails on a broken invariant. */
  public static class PlatformFailingClient implements EmbeddingClient {
    static final String MESSAGE = "image input without a prepared image (model " + MODEL + ")";
    private EmbeddingModelSpec spec;

    @Override
    public Set<String> supportedModalities(String modelName) {
      return TEXT_AND_IMAGE;
    }

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      this.spec = spec;
    }

    @Override
    public EmbeddingModelSpec spec() {
      return spec;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      throw new EmbeddingComputePlatformException(MESSAGE);
    }
  }

  /** Stored inputs whose out-of-line opens fail in storage on every try (a connection reset); inline ones read as stored. */
  private static final class FlakyStoredInputs extends BlobBytesSource {
    static final String FAILURE = "Connection reset (simulated)";

    FlakyStoredInputs() {
      super(HadoopFSUtils.getStorageConf(new Configuration()));
    }

    @Override
    public SeekableBlob openSeekable(Row blob) throws IOException {
      if (HoodieSchema.Blob.OUT_OF_LINE.equals(blob.getString(0))) {
        throw new IOException(FAILURE);
      }
      return super.openSeekable(blob);
    }
  }

  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), MODEL);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, String.valueOf(DIM));
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX,
        DummyMultimodalEmbeddingClient.PROVIDER);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, "text,image");
    props.setProperty(EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), "0");
    return props;
  }

  private static StructType schema(EmbeddingTableConfig config) {
    return EmbeddingTableSchema.build(config).add(ComputeRound.errorWorkingField());
  }

  private static BlobBytesSource storedInputs() {
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
  }

  private static List<Row> drain(EmbeddingTableConfig config, TypedProperties props, StructType schema, ComputeCounters counters,
                                 BlobBytesSource storedInputs, Row... rows) {
    List<Row> out = new ArrayList<>();
    try (MicroBatchEmbeddingIterator it = new MicroBatchEmbeddingIterator(Arrays.asList(rows).iterator(), config.model(MODEL),
        props, schema, 4, counters, storedInputs)) {
      it.forEachRemaining(out::add);
    }
    return out;
  }

  private static void drainInto(List<Row> yielded, EmbeddingTableConfig config, TypedProperties props, StructType schema,
                                ComputeCounters counters, BlobBytesSource storedInputs, Row... rows) {
    yielded.addAll(drain(config, props, schema, counters, storedInputs, rows));
  }

  private static ComputeCounters counters() {
    return new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator());
  }

  /** An inline stored input: the bytes as the source row carried them. */
  private static Row inline(byte[] bytes) {
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, bytes, null}, BLOB);
  }

  /** An out-of-line stored input: the unmanaged reference to the source file, with no range, as the source row carries it. */
  private static Row reference(Path file) {
    return reference(file, null, null);
  }

  private static Row reference(Path file, Long offset, Long length) {
    Row reference = new GenericRowWithSchema(new Object[] {file.toUri().toString(), offset, length, false}, REFERENCE);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
  }

  /** A managed out-of-line stored input relative to the embedding table's base, with its range, as the input store writes it. */
  private static Row managedRelative(String relativePath, long offset, long length) {
    Row reference = new GenericRowWithSchema(new Object[] {relativePath, offset, length, true}, REFERENCE);
    return new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
  }

  private static Map<String, String> outcomes(StructType schema, EmbeddingTableConfig config, List<Row> rows) {
    Map<String, String> outcomes = new TreeMap<>();
    for (Row row : rows) {
      outcomes.put(row.getString(schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)), outcome(row, schema, config));
    }
    return outcomes;
  }

  /** A processed row's outcome: EMBEDDED (vector), SKIPPED with its error, or PENDING (neither; the iterator never yields it). */
  private static String outcome(Row out, StructType schema, EmbeddingTableConfig config) {
    int vector = schema.fieldIndex(config.model(MODEL).getColumn());
    int error = schema.fieldIndex(ComputeRound.ERROR_WORKING_COLUMN);
    return out.isNullAt(vector) ? (out.isNullAt(error) ? PENDING : SKIPPED + out.getString(error)) : EMBEDDED;
  }

  /** An image row of the table schema, as compute reads it from the embedding table. */
  private static Row row(StructType schema, String sourcePathSha, Row preparedImage) {
    return row(schema, sourcePathSha, preparedImage, 1000L, 1L);
  }

  private static Row row(StructType schema, String sourcePathSha, Row preparedImage, long modificationTime, long size) {
    Object[] values = new Object[schema.size()];
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)] = sourcePathSha;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)] = modificationTime;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE)] = size;
    values[schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID)] = 0L;
    values[schema.fieldIndex(EmbeddingTableSchema.MODALITY)] = EmbeddingTableSchema.MODALITY_IMAGE;
    values[schema.fieldIndex(EmbeddingTableSchema.PREPARED_IMAGE)] = preparedImage;
    return new GenericRowWithSchema(values, schema);
  }
}
