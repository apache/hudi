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

package org.apache.hudi.utilities.unstructured.embedding.compute.bedrock;

import org.apache.hudi.common.config.TypedProperties;
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
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderClientSettings;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockInvoker.BedrockReply;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.image.PreparedImage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Image requests of the Bedrock families that embed images, against a stub {@link BedrockInvoker} that records each InvokeModel
 * body and answers from a script: Cohere Embed v4 packs the call's images like its texts, as an {@code images} list of data URIs
 * at the model's {@code output_dimension}, never beside {@code texts}, within its input, payload and token limits; Titan Multimodal Embeddings G1 sends each text as
 * {@code inputText} and each image as {@code inputImage} with {@code embeddingConfig.outputEmbeddingLength}. An image a family
 * cannot take (type, encoded size, side) and a failed preparation fail the input for good without a request; a text and an
 * image vector land at the one configured length.
 */
class TestBedrockImageRequests {

  private static final String ALIAS = "br";
  private static final String COHERE = "cohere.embed-v4:0";
  private static final String TITAN_MULTIMODAL = "amazon.titan-embed-image-v1";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final List<String> modelIds = new ArrayList<>();
  private final List<JsonNode> bodies = new ArrayList<>();
  /** Scripted replies, one per request; when empty, every request is answered with vectors of the requested length. */
  private final Deque<BedrockReply> script = new LinkedList<>();
  private TypedProperties props;

  @BeforeEach
  void setUp() {
    props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + BedrockClientConfig.REGION_SUFFIX, "us-west-2");
  }

  @Test
  void familiesReportTheirModalities() {
    assertEquals(EmbeddingClient.TEXT_ONLY, new BedrockTitanTextEmbeddingClient().supportedModalities("amazon.titan-embed-text-v2:0"));
    assertEquals(EmbeddingClient.TEXT_AND_IMAGE, new BedrockCohereEmbeddingClient().supportedModalities(COHERE));
    assertEquals(EmbeddingClient.TEXT_AND_IMAGE, new BedrockTitanMultimodalEmbeddingClient().supportedModalities(TITAN_MULTIMODAL));
    for (String id : Arrays.asList(TITAN_MULTIMODAL, "amazon.titan-embed-image-v1:0", "us.amazon.titan-embed-image-v1:0",
        "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-image-v1:0")) {
      assertEquals(BedrockTitanMultimodalEmbeddingClient.class.getName(),
          EmbeddingClientFactory.clientClassName(spec(id, 1024, EmbeddingClient.TEXT_AND_IMAGE)), id);
      assertEquals(BedrockModelFamily.TITAN_EMBED_IMAGE_V1, BedrockModelFamily.fromModelId(id).get(), id);
    }
  }

  @Test
  void coherePacksTheCallsImagesIntoOneRequestOfDataUrisApartFromItsTexts() throws InterruptedException {
    byte[] first = "first-jpeg".getBytes(StandardCharsets.UTF_8);
    byte[] second = "second-png".getBytes(StandardCharsets.UTF_8);
    BedrockEmbeddingClient client = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 1024);
    List<EmbeddingResult> results = client.embed(Arrays.asList(
        image("a", new PreparedImage(first, "image/jpeg", 40, 30, 1)),
        text("b", "a cat on a mat"),
        image("c", new PreparedImage(second, "image/png", 10, 20, 1))));

    assertEquals(2, bodies.size(), "one request for the text, one for both images");
    JsonNode texts = bodies.get(0);
    assertEquals(Collections.singletonList("a cat on a mat"), strings(texts.get("texts")));
    assertFalse(texts.has("images"));
    JsonNode images = bodies.get(1);
    assertFalse(images.has("texts"), "texts and images are never sent together");
    assertEquals("search_document", images.get("input_type").asText());
    assertEquals("NONE", images.get("truncate").asText());
    assertEquals(Collections.singletonList("float"), strings(images.get("embedding_types")));
    assertEquals(1024, images.get("output_dimension").asInt());
    assertEquals(Arrays.asList("data:image/jpeg;base64," + Base64.getEncoder().encodeToString(first),
        "data:image/png;base64," + Base64.getEncoder().encodeToString(second)), strings(images.get("images")), "in input order");
    assertEquals(Arrays.asList(COHERE, COHERE), modelIds);
    // every input gets its own vector, in input order, all at the one configured length
    assertArrayEquals(vector(1, 1024), results.get(0).getVector());
    assertArrayEquals(vector(0, 1024), results.get(1).getVector());
    assertArrayEquals(vector(2, 1024), results.get(2).getVector());
  }

  @Test
  void cohereImageRequestsRespectMaxInputs() throws InterruptedException {
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ProviderClientSettings.REQUEST_MAX_INPUTS_SUFFIX, "2");
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      inputs.add(image("i" + i, new PreparedImage(new byte[] {(byte) i}, "image/jpeg", 8, 8, 1)));
    }
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(inputs);

    assertEquals(Arrays.asList(2, 2, 1), imageCounts());
    results.forEach(result -> assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError()));
  }

  @Test
  void cohereImageRequestsStayWithinTheRequestPayload() throws InterruptedException {
    // each data URI is 5,333,359 characters: three fit in 20,000,000, a fourth does not
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      inputs.add(image("p" + i, new PreparedImage(new byte[4_000_000], "image/jpeg", 8, 8, 1)));
    }
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(inputs);

    assertEquals(Arrays.asList(3, 3), imageCounts());
    results.forEach(result -> assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError()));
  }

  @Test
  void cohereImageRequestsStayWithinTheRequestTokenBudget() throws InterruptedException {
    // 2000 x 2000 is resampled to 2,458,624 pixels, 12,544 tokens: ten fit in 128,000, an eleventh does not
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      inputs.add(image("t" + i, new PreparedImage(new byte[] {1}, "image/png", 2000, 2000, 1)));
    }
    client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(inputs);

    assertEquals(Arrays.asList(10, 2), imageCounts());
  }

  @Test
  void rejectedImageRequestIsIsolatedImageByImage() throws InterruptedException {
    script.add(BedrockReply.error(400, "ValidationException", "one image is malformed", null));
    script.add(null);
    script.add(BedrockReply.error(400, "ValidationException", "image b is malformed", null));
    BedrockEmbeddingClient client = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(
        image("a", new PreparedImage(new byte[] {1}, "image/jpeg", 1, 1, 1)),
        image("b", new PreparedImage(new byte[] {2}, "image/jpeg", 1, 1, 1))));

    assertEquals(Arrays.asList(2, 1, 1), imageCounts(), "the packed request, then each image alone");
    assertArrayEquals(vector(1, 256), results.get(0).getVector());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(1).getStatus());
    assertEquals("Bedrock HTTP 400 ValidationException: image b is malformed", results.get(1).getError());
    assertEquals(2, client.lastCallStats().getBisectRequests());
  }

  @Test
  void cohereImageOverItsByteCapIsPermanentWithoutARequest() throws InterruptedException {
    BedrockEmbeddingClient client = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(
        image("big", new PreparedImage(new byte[5_000_001], "image/jpeg", 10, 10, 1)),
        image("cap", new PreparedImage(new byte[5_000_000], "image/jpeg", 10, 10, 1))));

    assertEquals(1, bodies.size(), "only the image at the cap is sent");
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus());
    assertEquals(identity("big") + ": the prepared image is 5000001 bytes, over the 5000000 bytes COHERE_EMBED_V4 accepts; lower "
        + EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), results.get(0).getError());
    assertEquals(EmbeddingResult.Status.OK, results.get(1).getStatus(), results.get(1).getError());
  }

  @Test
  void imageOfATypeTheFamilyDoesNotAcceptIsPermanentWithoutARequest() throws InterruptedException {
    EmbeddingResult result = client(BedrockModelFamily.TITAN_EMBED_IMAGE_V1, TITAN_MULTIMODAL, 1024)
        .embed(Collections.singletonList(image("gif", new PreparedImage(new byte[] {1}, "image/gif", 1, 1, 1)))).get(0);

    assertTrue(bodies.isEmpty());
    assertEquals(identity("gif") + ": the prepared image is image/gif, which TITAN_EMBED_IMAGE_V1 does not accept (it accepts "
        + "[image/jpeg, image/png])", result.getError());
  }

  @Test
  void imagePreparationFailureIsTheInputsOwnOutcome() throws InterruptedException {
    PreparedImageSupplier refused = () -> {
      throw new ImageRefusedException("declares 200000000 pixels, over the limit");
    };
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(Arrays.asList(
        EmbeddingInput.storedImage("bad", 0L, EmbeddingTableSchema.MODALITY_IMAGE, () -> null, refused),
        image("good", new PreparedImage(new byte[] {7}, "image/png", 1, 1, 1))));

    assertEquals(1, bodies.size(), "the refused image is never sent; its batch-mate is");
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus());
    assertEquals(identity("bad") + ": declares 200000000 pixels, over the limit", results.get(0).getError());
    assertEquals(EmbeddingResult.Status.OK, results.get(1).getStatus());
  }

  @Test
  void imageWithoutAPreparedInputBreaksTheComputeInvariant() {
    BedrockEmbeddingClient client = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256);
    List<EmbeddingInput> inputs = Collections.singletonList(EmbeddingInput.storedImage("a", 0L, EmbeddingTableSchema.MODALITY_IMAGE, () -> null, null));
    EmbeddingComputePlatformException failure = assertThrows(EmbeddingComputePlatformException.class, () -> client.embed(inputs));
    assertEquals(BedrockEmbeddingClient.IMAGE_WITHOUT_PREPARED_INPUT + " (model " + ALIAS + ")", failure.getMessage());
    assertTrue(bodies.isEmpty());
  }

  @Test
  void imageRejectedByBedrockIsPermanentForThatInput() throws InterruptedException {
    script.add(BedrockReply.error(400, "ValidationException", "Malformed input image", null));
    EmbeddingResult result = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256)
        .embed(Collections.singletonList(image("a", new PreparedImage(new byte[] {1}, "image/jpeg", 1, 1, 1)))).get(0);

    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus());
    assertEquals("Bedrock HTTP 400 ValidationException: Malformed input image", result.getError());
  }

  @Test
  void titanMultimodalSendsTextAndImageInItsOwnFormatOneInputPerRequest() throws InterruptedException {
    byte[] pixels = "jpeg-bytes".getBytes(StandardCharsets.UTF_8);
    List<EmbeddingResult> results = client(BedrockModelFamily.TITAN_EMBED_IMAGE_V1, TITAN_MULTIMODAL, 384).embed(Arrays.asList(
        text("a", "one"), text("b", "two"), image("c", new PreparedImage(pixels, "image/jpeg", 2048, 1024, 1))));

    assertEquals(3, bodies.size());
    for (int i = 0; i < 2; i++) {
      assertEquals(Arrays.asList("embeddingConfig", "inputText"), fields(bodies.get(i)));
      assertEquals(384, bodies.get(i).get("embeddingConfig").get("outputEmbeddingLength").asInt());
    }
    assertEquals("one", bodies.get(0).get("inputText").asText());
    assertEquals("two", bodies.get(1).get("inputText").asText());
    JsonNode image = bodies.get(2);
    assertEquals(Arrays.asList("embeddingConfig", "inputImage"), fields(image), "an image is sent alone, without inputText");
    assertEquals(Base64.getEncoder().encodeToString(pixels), image.get("inputImage").asText(), "bare base64, no data URI");
    assertEquals(384, image.get("embeddingConfig").get("outputEmbeddingLength").asInt());
    assertEquals(Arrays.asList(TITAN_MULTIMODAL, TITAN_MULTIMODAL, TITAN_MULTIMODAL), modelIds);
    for (int i = 0; i < 3; i++) {
      assertArrayEquals(vector(i, 384), results.get(i).getVector(), "input " + i);
    }
  }

  @Test
  void titanMultimodalImageBeyondItsSideOrByteLimitIsPermanentWithoutARequest() throws InterruptedException {
    List<EmbeddingResult> results = client(BedrockModelFamily.TITAN_EMBED_IMAGE_V1, TITAN_MULTIMODAL, 1024).embed(Arrays.asList(
        image("wide", new PreparedImage(new byte[] {1}, "image/png", 2049, 10, 1)),
        image("heavy", new PreparedImage(new byte[25_000_001], "image/jpeg", 100, 100, 1))));

    assertTrue(bodies.isEmpty());
    assertEquals(identity("wide") + ": the prepared image is 2049x10 pixels, over the 2048 pixels per side TITAN_EMBED_IMAGE_V1 "
        + "accepts; lower " + EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), results.get(0).getError());
    assertEquals(identity("heavy") + ": the prepared image is 25000001 bytes, over the 25000000 bytes TITAN_EMBED_IMAGE_V1 "
        + "accepts; lower " + EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), results.get(1).getError());
  }

  @Test
  void titanMultimodalAcceptsItsOwnOutputLengthsOnly() {
    for (int dimension : Arrays.asList(256, 384, 1024)) {
      BedrockClientConfig.from(spec(TITAN_MULTIMODAL, dimension, EmbeddingClient.TEXT_AND_IMAGE), props,
          BedrockModelFamily.TITAN_EMBED_IMAGE_V1);
    }
    EmbeddingModelSpec spec = spec(TITAN_MULTIMODAL, 512, EmbeddingClient.TEXT_AND_IMAGE);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> BedrockClientConfig.from(spec, props, BedrockModelFamily.TITAN_EMBED_IMAGE_V1));
    assertEquals("hoodie.unstructured.embedding.model.br.dimensions=512 is not an output dimension TITAN_EMBED_IMAGE_V1 supports: "
        + "[256, 384, 1024]", refused.getMessage());
  }

  @Test
  void titanMultimodalRefusesAnImageLongSideBeyondItsLimitOnlyWhenImagesAreDeclared() {
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), "2049");
    EmbeddingModelSpec spec = spec(TITAN_MULTIMODAL, 1024, EmbeddingClient.TEXT_AND_IMAGE);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> BedrockClientConfig.from(spec, props, BedrockModelFamily.TITAN_EMBED_IMAGE_V1));
    assertEquals(EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key() + "=2049 resizes images beyond the 2048 pixels per side "
        + "TITAN_EMBED_IMAGE_V1 accepts, and model br declares image (hoodie.unstructured.embedding.model.br.modalities); set it "
        + "to at most 2048", refused.getMessage());
    // a text-only declaration sends no image
    BedrockClientConfig.from(spec(TITAN_MULTIMODAL, 1024, EmbeddingClient.TEXT_ONLY), props, BedrockModelFamily.TITAN_EMBED_IMAGE_V1);
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key(), "2048");
    BedrockClientConfig.from(spec(TITAN_MULTIMODAL, 1024, EmbeddingClient.TEXT_AND_IMAGE), props, BedrockModelFamily.TITAN_EMBED_IMAGE_V1);
  }

  @Test
  void titanMultimodalClientInitRefusesAWrongDimensionAsConfiguration() {
    EmbeddingClientConfigException refused = assertThrows(EmbeddingClientConfigException.class,
        () -> client(BedrockModelFamily.TITAN_EMBED_IMAGE_V1, TITAN_MULTIMODAL, 1536));
    assertTrue(refused.getMessage().contains("[256, 384, 1024]"), refused.getMessage());
  }

  @Test
  void firstCallOfAFailedPreparationAndOneRejectedImageIsPermanentNotConfiguration() throws InterruptedException {
    // one input reached Bedrock: its rejection cannot tell the row from the binding, so it stays a row outcome
    script.add(BedrockReply.error(400, "ValidationException", "Malformed input image", null));
    PreparedImageSupplier refused = () -> {
      throw new ImageRefusedException("declares 200000000 pixels, over the limit");
    };
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(Arrays.asList(
        EmbeddingInput.storedImage("bad", 0L, EmbeddingTableSchema.MODALITY_IMAGE, () -> null, refused),
        image("rejected", new PreparedImage(new byte[] {1}, "image/jpeg", 1, 1, 1))));

    assertEquals(1, bodies.size());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(1).getStatus());
    assertEquals("Bedrock HTTP 400 ValidationException: Malformed input image", results.get(1).getError());
  }

  @Test
  void firstCallWhoseEverySentImageIsRejectedFailsAsConfiguration() {
    for (int i = 0; i < 3; i++) {
      script.add(BedrockReply.error(400, "ValidationException", "The provided model identifier is invalid", null));
    }
    BedrockEmbeddingClient client = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256);
    List<EmbeddingInput> inputs = Arrays.asList(
        image("a", new PreparedImage(new byte[] {1}, "image/jpeg", 1, 1, 1)),
        image("b", new PreparedImage(new byte[] {2}, "image/jpeg", 1, 1, 1)));
    EmbeddingClientConfigException refused = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(refused.getMessage().contains("2 input(s) rejected"), refused.getMessage());
    assertEquals(Arrays.asList(2, 1, 1), imageCounts(), "the packed request, then each image alone");
  }

  @Test
  void throttledTextsBesideARejectedImageStayRetryable() throws InterruptedException {
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ProviderClientSettings.RETRY_MAX_ATTEMPTS_SUFFIX, "3");
    for (int i = 0; i < 3; i++) {
      script.add(BedrockReply.error(429, "ThrottlingException", "Too many requests", null));
    }
    script.add(BedrockReply.error(400, "ValidationException", "Malformed input image", null));
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(Arrays.asList(
        text("t", "words"), image("i", new PreparedImage(new byte[] {1}, "image/jpeg", 1, 1, 1))));

    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(0).getStatus(), results.get(0).getError());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus(), results.get(1).getError());
    assertTrue(results.get(1).getError().contains("retried"), results.get(1).getError());
  }

  @Test
  void imagesAfterAnOpenOutageAreRetryableAndNeverPrepared() throws InterruptedException {
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ProviderClientSettings.REQUEST_MAX_INPUTS_SUFFIX, "1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ProviderClientSettings.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX, "1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ProviderClientSettings.RETRY_MAX_ATTEMPTS_SUFFIX, "3");
    for (int i = 0; i < 3; i++) {
      script.add(BedrockReply.error(503, "ServiceUnavailableException", "unavailable", null));
    }
    int[] prepared = {0};
    PreparedImageSupplier counted = () -> {
      prepared[0]++;
      return new PreparedImage(new byte[] {1}, "image/jpeg", 1, 1, 1);
    };
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      inputs.add(EmbeddingInput.storedImage("o" + i, 0L, EmbeddingTableSchema.MODALITY_IMAGE, () -> null, counted));
    }
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(inputs);

    results.forEach(result -> assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus(), result.getError()));
    assertEquals(1, prepared[0], "only the first image is prepared; the outage stops the rest before preparation");
  }

  @Test
  void coherePayloadCountsTheEnvelopeAndEachUrisQuotesAndSeparator() throws InterruptedException {
    // four data URIs of 4,999,999 characters each (3,749,982-byte images, under the 5 MB image cap) sum to 19,999,996: within
    // 20,000,000 by their lengths alone, over it once the JSON envelope and each URI's quotes and separator are counted
    long prefix = "data:image/jpeg;base64,".length();
    int bytes = (int) ((5_000_000L - prefix) / 4L * 3L);
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      inputs.add(image("q" + i, new PreparedImage(new byte[bytes], "image/jpeg", 8, 8, 1)));
    }
    List<EmbeddingResult> results = client(BedrockModelFamily.COHERE_EMBED_V4, COHERE, 256).embed(inputs);

    assertEquals(Arrays.asList(3, 1), imageCounts());
    bodies.forEach(body -> assertTrue(body.toString().length() <= 20_000_000, "each request body stays within the limit"));
    results.forEach(result -> assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError()));
  }

  @Test
  void titanMultimodalRefusesAChunkSizeBeyondItsTextTokensOnlyWhenTextIsDeclared() {
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "257");
    EmbeddingModelSpec spec = spec(TITAN_MULTIMODAL, 1024, EmbeddingClient.TEXT_AND_IMAGE);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> BedrockClientConfig.from(spec, props, BedrockModelFamily.TITAN_EMBED_IMAGE_V1));
    assertEquals(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key() + "=257 cuts text chunks beyond the 256 text tokens "
        + "TITAN_EMBED_IMAGE_V1 accepts, and model br declares text (hoodie.unstructured.embedding.model.br.modalities); set it "
        + "to at most 256", refused.getMessage());
    // an image-only declaration sends no text; the default chunk size fits
    BedrockClientConfig.from(spec(TITAN_MULTIMODAL, 1024, Collections.singleton(EmbeddingTableSchema.MODALITY_IMAGE)), props,
        BedrockModelFamily.TITAN_EMBED_IMAGE_V1);
    props.remove(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key());
    BedrockClientConfig.from(spec(TITAN_MULTIMODAL, 1024, EmbeddingClient.TEXT_AND_IMAGE), props, BedrockModelFamily.TITAN_EMBED_IMAGE_V1);
  }

  /** A client of {@code family} whose requests go to the stub invoker. */
  private BedrockEmbeddingClient client(BedrockModelFamily family, String modelId, int dimension) {
    BedrockEmbeddingClient client = new BedrockEmbeddingClient(config -> this::invoke, System::currentTimeMillis, millis -> {
    }, new Random(7)) {
      @Override
      public BedrockModelFamily family() {
        return family;
      }
    };
    client.init(spec(modelId, dimension, EmbeddingClient.TEXT_AND_IMAGE), props);
    return client;
  }

  /**
   * Records the request; answers with the script's next reply, or, when the script is empty or holds null next, with one vector
   * per text or image, numbered from the request's position.
   */
  private BedrockReply invoke(String modelId, byte[] body) throws IOException {
    modelIds.add(modelId);
    JsonNode request = MAPPER.readTree(body);
    bodies.add(request);
    BedrockReply scripted = script.poll();
    if (scripted != null) {
      return scripted;
    }
    int seed = bodies.size() - 1;
    ObjectNode reply = MAPPER.createObjectNode();
    if (request.has("output_dimension")) {
      int count = request.has("texts") ? request.get("texts").size() : request.get("images").size();
      ArrayNode floats = reply.putObject("embeddings").putArray("float");
      for (int i = 0; i < count; i++) {
        floats.add(array(vector(seed + i, request.get("output_dimension").asInt())));
      }
    } else {
      reply.set("embedding", array(vector(seed, request.get("embeddingConfig").get("outputEmbeddingLength").asInt())));
      reply.put("inputTextTokenCount", 1);
    }
    return BedrockReply.ok(MAPPER.writeValueAsBytes(reply), -1L);
  }

  /** A distinct, deterministic vector of {@code length} floats for {@code seed}. */
  private static float[] vector(int seed, int length) {
    float[] vector = new float[length];
    for (int i = 0; i < length; i++) {
      vector[i] = seed + i / 10_000f;
    }
    return vector;
  }

  /** The number of images each recorded request carried. */
  private List<Integer> imageCounts() {
    List<Integer> counts = new ArrayList<>();
    bodies.forEach(body -> counts.add(body.get("images").size()));
    return counts;
  }

  private static ArrayNode array(float[] vector) {
    ArrayNode array = MAPPER.createArrayNode();
    for (float value : vector) {
      array.add(value);
    }
    return array;
  }

  private static List<String> strings(JsonNode array) {
    List<String> values = new ArrayList<>();
    array.forEach(node -> values.add(node.asText()));
    return values;
  }

  private static List<String> fields(JsonNode object) {
    Set<String> names = new TreeSet<>();
    object.fieldNames().forEachRemaining(names::add);
    return new ArrayList<>(names);
  }

  private static EmbeddingModelSpec spec(String model, int dimension, Set<String> modalities) {
    return new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, dimension, null, BedrockEmbeddingClient.PROVIDER, model, modalities);
  }

  private static EmbeddingInput text(String sha, String text) {
    return new EmbeddingInput(sha, 0, EmbeddingTableSchema.MODALITY_TEXT, text);
  }

  private static EmbeddingInput image(String sha, PreparedImage prepared) {
    return EmbeddingInput.storedImage(sha, 0L, EmbeddingTableSchema.MODALITY_IMAGE, () -> null, () -> prepared);
  }

  private static String identity(String sha) {
    return image(sha, null).describeIdentity();
  }
}
