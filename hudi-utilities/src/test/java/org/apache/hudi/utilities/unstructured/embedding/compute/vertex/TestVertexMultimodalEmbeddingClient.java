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

package org.apache.hudi.utilities.unstructured.embedding.compute.vertex;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingComputePlatformException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.PreparedImageSupplier;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreprocessingSpec;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.image.InProcessImageDecoder;
import org.apache.hudi.utilities.unstructured.embedding.image.SeekableBlobImageInputStream;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.awt.image.BufferedImage;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The Vertex multimodalembedding@001 client against recorded image and text responses (no live call). */
class TestVertexMultimodalEmbeddingClient {

  private static final String ALIAS = "mm";
  private static final String TOKEN = "recorded-test-token-0001";
  private static final ImagePreprocessingSpec IMAGE_SPEC = new ImagePreprocessingSpec(512,
      ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, ImagePreprocessingSpec.DEFAULT_MAX_PIXELS,
      InProcessImageDecoder.class.getName());
  private static final StructType BLOB = (StructType) BlobType.dataType();
  @TempDir
  Path tempDir;
  private VertexStubServer stub;
  private TypedProperties props;

  @BeforeEach
  void setUp() throws IOException {
    stub = new VertexStubServer();
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, (TOKEN + "\n").getBytes(StandardCharsets.UTF_8));
    props = new TypedProperties();
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), stub.endpoint());
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tokenFile.toString());
  }

  @AfterEach
  void tearDown() {
    stub.close();
  }

  @Test
  void imageRequestShapeAndRecordedVector() throws Exception {
    byte[] jpeg = ImageFixtures.image("jpeg", 1200, 900);
    List<EmbeddingResult> results = client(128).embed(Collections.singletonList(imageInput("f1", jpeg)));
    assertEquals(1, results.size());
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertArrayEquals(recorded("predict-image-response.json", "imageEmbedding"), results.get(0).getVector());

    assertEquals(1, stub.requests().size());
    VertexStubServer.Recorded request = stub.requests().get(0);
    assertEquals("Bearer " + TOKEN, request.authorization);
    JsonNode image = request.body.path("instances").path(0).path("image");
    // the default model input: JPEG at quality 90
    assertEquals("image/jpeg", image.path("mimeType").asText());
    assertEquals(128, request.body.path("parameters").path("dimension").asInt());
    assertFalse(request.body.path("instances").path(0).has("text"));
    BufferedImage sent = ImageFixtures.decode(Base64.getDecoder().decode(image.path("bytesBase64Encoded").asText()));
    assertEquals(512, sent.getWidth());
    assertEquals(384, sent.getHeight());
  }

  @Test
  void textRequestShape() throws Exception {
    EmbeddingInput text = new EmbeddingInput("f2", 3, EmbeddingTableSchema.MODALITY_TEXT, "a cat on a mat");
    List<EmbeddingResult> results = client(128).embed(Collections.singletonList(text));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertArrayEquals(recorded("predict-text-response.json", "textEmbedding"), results.get(0).getVector());
    JsonNode instance = stub.requests().get(0).body.path("instances").path(0);
    assertEquals("a cat on a mat", instance.path("text").asText());
    assertFalse(instance.has("image"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"null", "true", "\"invalid\"", "\"0.5\"", "{}", "[]", "1e100", "NaN", "1e300", "1e400"})
  void malformedVectorElementsAreRetryable(String element) throws Exception {
    props.setProperty(key(".retry.max.attempts"), "1");
    String values = element + ",0.25".repeat(127);
    stub.enqueueBody("{\"predictions\":[{\"textEmbedding\":[" + values + "]}]}");
    stub.enqueueBody("{\"predictions\":[{\"textEmbedding\":[" + values + "]}]}");
    List<EmbeddingResult> results = client(128).embed(Arrays.asList(
        new EmbeddingInput("text", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat"),
        new EmbeddingInput("second", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat")));
    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
      assertTrue(result.getError().startsWith("malformed answer:"), result.getError());
      assertNull(result.getVector());
    }
    assertEquals(2, stub.requests().size());
  }

  @Test
  void oneRequestPerInputAndMixedOutcomesStayPerInput() throws Exception {
    // one attempt per request, so the 429 ends its input RETRYABLE (retries: TestVertexRetryLoop)
    props.setProperty(key(".retry.max.attempts"), "1");
    stub.enqueue(200, "predict-text-response.json");
    stub.enqueue(400, "error-400.json");
    stub.enqueue(429, "error-429.json");
    List<EmbeddingResult> results = client(128).embed(Arrays.asList(
        new EmbeddingInput("a", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat"),
        new EmbeddingInput("b", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat"),
        new EmbeddingInput("c", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat")));
    assertEquals(3, stub.requests().size());
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(1).getStatus());
    assertTrue(results.get(1).getError().startsWith("Vertex rejected the input with HTTP 400"), results.get(1).getError());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(2).getStatus());
    assertTrue(results.get(2).getError().startsWith("Vertex answered HTTP 429"), results.get(2).getError());
  }

  @Test
  void http401IsAConfigFailureAndNeverEchoesTheToken() {
    stub.enqueueStatus(401, "{\"error\":{\"message\":\"rejected " + TOKEN + "\"}}");
    VertexMultimodalEmbeddingClient client = client(128);
    List<EmbeddingInput> inputs = Collections.singletonList(new EmbeddingInput("a", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
    assertTrue(e.getMessage().contains("file "), e.getMessage());
    assertFalse(e.getMessage().contains(TOKEN));
  }

  /**
   * 400 FAILED_PRECONDITION ("you tried to access a model that requires allowlisting or is disallowed by the organization's
   * policy"; "the client should not retry until the system state has been explicitly fixed",
   * https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/api-errors, read 2026-10-27) is the configuration, never
   * the input: the run fails instead of writing the row as an error.
   */
  @Test
  void failedPreconditionIsAConfigFailure() {
    stub.enqueue(400, "error-400-failed-precondition.json");
    VertexMultimodalEmbeddingClient client = client(128);
    List<EmbeddingInput> inputs = Collections.singletonList(new EmbeddingInput("f", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("FAILED_PRECONDITION"), e.getMessage());
    assertEquals(1, stub.requests().size());
  }

  @Test
  void refusedImageIsPermanentWithoutARequest() throws Exception {
    ImagePreprocessingSpec guarded = new ImagePreprocessingSpec(512, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, 100,
        InProcessImageDecoder.class.getName());
    ImagePreparer preparer = new ImagePreparer(guarded);
    byte[] png = ImageFixtures.image("png", 50, 50);
    EmbeddingInput input = image("big",
        () -> preparer.prepare(ImageFixtures.source(png)));
    EmbeddingResult result = client(128).embed(Collections.singletonList(input)).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus());
    assertTrue(result.getError().startsWith("source_path_sha="), result.getError());
    assertTrue(result.getError().contains(": " + ImageRefusedException.REFUSED_BEFORE_DECODE), result.getError());
    assertEquals(0, stub.requests().size());
  }

  @Test
  void imageReadFailureIsRetryable() throws Exception {
    EmbeddingInput input = image("io",
        () -> {
          throw new IOException("storage hiccup");
        });
    EmbeddingResult result = client(128).embed(Collections.singletonList(input)).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertEquals(0, stub.requests().size());
  }

  @Test
  void imagesOfAMicroBatchArePreparedOneAtATimeBetweenRequests() throws Exception {
    // no container accumulates the batch: each image is prepared only after the previous input's
    // request went out, so at most one prepared image is held at any time; nothing is prepared before embed() runs
    List<String> preparedAfterRequests = new ArrayList<>();
    ImagePreparer preparer = new ImagePreparer(IMAGE_SPEC);
    List<EmbeddingInput> batch = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      byte[] png = ImageFixtures.image("png", 60 + i, 40);
      String fid = "f" + i;
      batch.add(image(fid, () -> {
        preparedAfterRequests.add(fid + "@" + stub.requests().size());
        return preparer.prepare(ImageFixtures.source(png));
      }));
    }
    assertTrue(preparedAfterRequests.isEmpty(), "building the inputs prepares nothing");
    List<EmbeddingResult> results = client(128).embed(batch);
    assertEquals(Arrays.asList("f0@0", "f1@1", "f2@2", "f3@3"), preparedAfterRequests);
    assertEquals(4, results.size());
    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.OK, result.getStatus());
    }
  }

  @Test
  void storageFailureMidDecodeIsRetryable() throws Exception {
    // the PNG reader wraps the stream's IOException in an IIOException; a blob read failure is still transient
    byte[] png = ImageFixtures.image("png", 300, 200);
    ImagePreparer preparer = new ImagePreparer(IMAGE_SPEC);
    EmbeddingInput input = image("flaky",
        () -> preparer.prepare(() -> SeekableBlobImageInputStream.forStoredInput(new FailingBlob(png, 200))));
    EmbeddingResult result = client(128).embed(Collections.singletonList(input)).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus(), result.getError());
    assertEquals(0, stub.requests().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"png", "jpeg", "bmp"})
  void outOfLineFileShorterThanItsRecordedLengthIsPermanentAtCompute(String format) throws Exception {
    // a stored file that ends before external_reference.length cannot be read to the end on any attempt, whichever
    // reader meets the end (PNG wraps it in an IIOException, the JPEG native callback and the BMP readFully let it out bare): the
    // row's fault, PERMANENT, its error naming the row and the file
    byte[] image = ImageFixtures.image(format, 300, 200);
    Path file = tempDir.resolve("short." + format);
    Files.write(file, Arrays.copyOf(image, image.length * 6 / 10));
    StructType referenceType = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
    Row reference = new GenericRowWithSchema(new Object[] {file.toUri().toString(), 0L, (long) image.length, false}, referenceType);
    Row blob = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null, reference}, BLOB);
    BlobBytesSource source = new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration()));
    ImagePreparer preparer = new ImagePreparer(IMAGE_SPEC);
    EmbeddingInput input = image("short",
        () -> preparer.prepare(() -> SeekableBlobImageInputStream.forStoredInput(source.openSeekable(blob))));
    EmbeddingResult result = client(128).embed(Collections.singletonList(input)).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus(), format + ": " + result.getError());
    assertTrue(result.getError().startsWith("stored input of " + input.describeIdentity() + " cannot be read: BlobTruncatedException: ")
        && result.getError().contains("short." + format) && result.getError().contains("ended early"), format + ": " + result.getError());
    assertEquals(0, stub.requests().size());
  }

  @Test
  void uncheckedWrapperOfATruncatedRangeIsPermanent() throws Exception {
    // An EOF that is not under a BlobReadException means the reader ran out of intact bytes, so under an unchecked wrapper
    // it is the data's failure. A storage failure under the same wrapper stays RETRYABLE, as the unchecked storage test shows.
    EmbeddingInput truncated = image("t", () -> {
      throw new UncheckedIOException(new EOFException("Blob file x ended early: expected 10 more bytes"));
    });
    EmbeddingResult result = client(128).embed(Collections.singletonList(truncated)).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus(), result.getError());
  }

  @Test
  void uncheckedStorageFailureIsRetryable() throws Exception {
    // storage layers also fail with unchecked wrappers of an IOException (UncheckedIOException, HoodieIOException)
    EmbeddingInput unchecked = image("u", () -> {
      throw new UncheckedIOException(new IOException("Connection reset by peer (simulated)"));
    });
    EmbeddingInput hoodie = image("h", () -> {
      throw new HoodieIOException("open failed", new IOException("timeout (simulated)"));
    });
    List<EmbeddingResult> results = client(128).embed(Arrays.asList(unchecked, hoodie));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(0).getStatus(), results.get(0).getError());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus(), results.get(1).getError());
    assertEquals(0, stub.requests().size());
  }

  @Test
  void corruptPixelDataIsPermanent() throws Exception {
    // the same IIOException without a blob read failure underneath is a property of the bytes
    byte[] png = ImageFixtures.image("png", 300, 200);
    byte[] truncated = Arrays.copyOf(png, png.length * 6 / 10);
    ImagePreparer preparer = new ImagePreparer(IMAGE_SPEC);
    EmbeddingInput input = image("cut",
        () -> preparer.prepare(ImageFixtures.source(truncated)));
    EmbeddingResult result = client(128).embed(Collections.singletonList(input)).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus(), result.getError());
  }

  @Test
  void unexpectedPreparationRuntimeFailureFailsTheCall() throws Exception {
    EmbeddingInput broken = image("broken", () -> {
      throw new IllegalStateException("Image raster budget assertion failed");
    });
    EmbeddingComputePlatformException failure = assertThrows(EmbeddingComputePlatformException.class,
        () -> client(128).embed(Arrays.asList(broken, imageInput("ok", ImageFixtures.image("png", 20, 20)))));
    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertEquals(0, stub.requests().size());
  }

  /** A blob whose storage read fails once {@code failAt} bytes have been served. */
  private static final class FailingBlob implements SeekableBlob {
    private final byte[] data;
    private final int failAt;
    private int pos;

    FailingBlob(byte[] data, int failAt) {
      this.data = data;
      this.failAt = failAt;
    }

    @Override
    public long length() {
      return data.length;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) {
      pos = (int) newPos;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
      if (pos >= failAt) {
        throw new IOException("Connection reset by peer (simulated storage failure)");
      }
      int n = Math.min(len, Math.min(failAt, data.length) - pos);
      System.arraycopy(data, pos, buffer, offset, n);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      // nothing to release: the bytes are an in-memory array
    }
  }

  /**
   * An image input that reaches the client without a prepared image is a broken prep/compute invariant (every multimodal
   * input is built with its preparation by the iterator), never the user's configuration: a platform failure, not
   * {@link EmbeddingClientConfigException}.
   */
  @Test
  void imageWithoutPreparedInputIsAPlatformFailure() {
    EmbeddingInput input = EmbeddingInput.storedImage("x", 0, EmbeddingTableSchema.MODALITY_IMAGE, null, null);
    VertexMultimodalEmbeddingClient client = client(128);
    List<EmbeddingInput> inputs = Collections.singletonList(input);
    EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().startsWith(VertexMultimodalEmbeddingClient.IMAGE_WITHOUT_PREPARED_INPUT), e.getMessage());
    assertEquals(0, stub.requests().size());
  }

  /** A 404 (a model or endpoint that does not exist) or another 4xx is the configuration (class P): never retried. */
  @Test
  void http404IsAConfigFailure() {
    stub.enqueueStatus(404, "{\"error\":{\"code\":404,\"message\":\"Publisher Model was not found\"}}");
    VertexMultimodalEmbeddingClient client = client(128);
    List<EmbeddingInput> inputs = Collections.singletonList(new EmbeddingInput("a", 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
    assertFalse(e.getMessage().contains(TOKEN));
  }

  @Test
  void configIsValidatedOnTheDriver() {
    EmbeddingModelSpec badDimension = spec(100);
    IllegalArgumentException dimension = assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.checkConfig(badDimension, props));
    assertTrue(dimension.getMessage().contains("[128, 256, 512, 1408]"), dimension.getMessage());
    TypedProperties noEndpoint = new TypedProperties();
    EmbeddingModelSpec validSpec = spec(128);
    IllegalArgumentException endpoint = assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.checkConfig(validSpec, noEndpoint));
    assertTrue(endpoint.getMessage().contains(key(VertexClientConfig.PROJECT_SUFFIX)), endpoint.getMessage());
    noEndpoint.setProperty(key(VertexClientConfig.PROJECT_SUFFIX), "proj");
    assertEquals("https://us-central1-aiplatform.googleapis.com/v1/projects/proj/locations/us-central1/publishers/google/models/"
        + "multimodalembedding@001:predict", VertexClientConfig.from(spec(128), noEndpoint).endpoint());
    assertEquals(VertexMultimodalEmbeddingClient.class.getName(), EmbeddingClientFactory.clientClassName(spec(128)));
    assertEquals(EmbeddingClient.TEXT_AND_IMAGE, client(128).supportedModalities("multimodalembedding@001"));
  }

  @Test
  void missingTokenFailsInit() {
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tempDir.resolve("absent").toString());
    assertThrows(EmbeddingClientConfigException.class, () -> client(128));
  }

  @ParameterizedTest
  @ValueSource(ints = {33, 256})
  void textModalityRefusesChunksBeyondTheEndpointLimit(int maxTokens) {
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), Integer.toString(maxTokens));
    EmbeddingModelSpec textModel = new EmbeddingModelSpec(ALIAS, "emb_mm", 128, null,
        VertexMultimodalEmbeddingClient.PROVIDER, "multimodalembedding@001", EmbeddingClient.TEXT_AND_IMAGE);
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient();
    EmbeddingClientConfigException failure = assertThrows(EmbeddingClientConfigException.class, () -> client.init(textModel, props));
    assertTrue(failure.getMessage().contains(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()), failure.getMessage());
    assertEquals(0, stub.requests().size());
  }

  @Test
  void textModalityAccepts32TokensAndImageOnlyKeepsItsChunkSetting() {
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "32");
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient();
    client.init(new EmbeddingModelSpec(ALIAS, "emb_mm", 128, null, VertexMultimodalEmbeddingClient.PROVIDER,
        "multimodalembedding@001", EmbeddingClient.TEXT_AND_IMAGE), props);
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "256");
    client.init(new EmbeddingModelSpec(ALIAS, "emb_mm", 128, null, VertexMultimodalEmbeddingClient.PROVIDER,
        "multimodalembedding@001", Collections.singleton(EmbeddingTableSchema.MODALITY_IMAGE)), props);
  }

  @ParameterizedTest
  @ValueSource(strings = {"multimodalembedding@002:predict", "other-model:predict", "multimodalembedding@001:other"})
  void explicitEndpointMustNameThePersistedModel(String endpointModel) {
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), stub.endpoint().replace("multimodalembedding@001:predict", endpointModel));
    EmbeddingClientConfigException failure = assertThrows(EmbeddingClientConfigException.class, () -> client(128));
    assertTrue(failure.getMessage().contains("model"), failure.getMessage());
    assertEquals(0, stub.requests().size());
  }

  @Test
  void explicitProxyEndpointWithoutAModelIsRefused() {
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), "http://127.0.0.1:12345/predict");
    assertThrows(EmbeddingClientConfigException.class, () -> client(128));
  }

  @Test
  void singleInputRequestsKeepTheWholeCallRejectionGuard() {
    assertFalse(client(128).answersEachInputAlone());
  }

  private VertexMultimodalEmbeddingClient client(int dimension) {
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient();
    client.init(spec(dimension), props);
    return client;
  }

  private static EmbeddingModelSpec spec(int dimension) {
    return new EmbeddingModelSpec(ALIAS, "emb_mm", dimension, null, VertexMultimodalEmbeddingClient.PROVIDER,
        "multimodalembedding@001");
  }

  /** An image input whose model input {@code image} prepares. */
  private static EmbeddingInput image(String fid, PreparedImageSupplier image) {
    return EmbeddingInput.storedImage(fid, 0, EmbeddingTableSchema.MODALITY_IMAGE, null, image);
  }

  private static EmbeddingInput imageInput(String fid, byte[] bytes) {
    ImagePreparer preparer = new ImagePreparer(IMAGE_SPEC);
    return image(fid, () -> preparer.prepare(ImageFixtures.source(bytes)));
  }

  private static float[] recorded(String resource, String field) throws IOException {
    JsonNode vector = VertexStubServer.MAPPER.readTree(VertexStubServer.resource(resource)).path("predictions").path(0).path(field);
    float[] values = new float[vector.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = (float) vector.get(i).asDouble();
    }
    return values;
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
