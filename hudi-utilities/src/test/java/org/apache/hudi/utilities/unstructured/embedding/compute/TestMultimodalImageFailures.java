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
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.vertex.VertexClientConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.vertex.VertexMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageHeaderException;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReferenceException;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobTruncatedException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.imageio.IIOException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The multimodal clients (Vertex and the dummy multimodal client) classify an image whose preparation failed alike: for the same
 * exception each gives the same result kind and the same error text, all through {@link ImageFailures#result}. No request is
 * sent.
 */
class TestMultimodalImageFailures {

  private static final String ALIAS = "mm";
  @TempDir
  Path tempDir;
  private ScriptedHttpStub stub;

  @BeforeEach
  void setUp() throws IOException {
    stub = new ScriptedHttpStub();
  }

  @AfterEach
  void tearDown() {
    stub.close();
  }

  static Stream<Arguments> failures() {
    return Stream.of(
        failure("storage IOException", EmbeddingResult.Status.RETRYABLE_FAILURE, () -> new IOException("storage hiccup")),
        failure("IOException without a message", EmbeddingResult.Status.RETRYABLE_FAILURE, IOException::new),
        failure("blob read failure", EmbeddingResult.Status.RETRYABLE_FAILURE,
            () -> new BlobReadException(new IOException("Connection reset (simulated)"))),
        failure("confirmed-missing stored file", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new FileNotFoundException("File /t/a.bin does not exist")),
        failure("missing stored file under the storage wrapper", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new HoodieIOException("Exception creating input stream from file: /t/a.bin", new FileNotFoundException("File /t/a.bin does not exist"))),
        failure("short stored copy under the PNG reader's wrapper", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new IIOException("Error reading PNG image data", new BlobTruncatedException("Blob file /t/a.bin ended early: expected 10 more bytes"))),
        failure("blob read EOF under the PNG reader's wrapper", EmbeddingResult.Status.RETRYABLE_FAILURE,
            () -> new IIOException("Error reading PNG image data",
                new BlobReadException(new EOFException("Blob file x ended early: expected 10 more bytes")))),
        failure("bare EOF", EmbeddingResult.Status.PERMANENT_FAILURE, () -> new EOFException("ran out of intact bytes")),
        failure("EOF without a message", EmbeddingResult.Status.PERMANENT_FAILURE, EOFException::new),
        failure("EOF under an unchecked wrapper", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new UncheckedIOException(new EOFException("Blob file x ended early: expected 10 more bytes"))),
        failure("EOF under a plain IOException", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new IOException("read failed", new EOFException("ran out of intact bytes"))),
        failure("refusal", EmbeddingResult.Status.PERMANENT_FAILURE, () -> new ImageRefusedException("refused: too many pixels")),
        failure("refusal under an unchecked wrapper", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new UncheckedIOException(new ImageRefusedException("refused: too many pixels"))),
        failure("unreadable header", EmbeddingResult.Status.PERMANENT_FAILURE, () -> new ImageHeaderException("no reader")),
        failure("corrupt pixel data", EmbeddingResult.Status.PERMANENT_FAILURE,
            () -> new IIOException("Error reading PNG image data")),
        failure("IOException under UncheckedIOException", EmbeddingResult.Status.RETRYABLE_FAILURE,
            () -> new UncheckedIOException(new IOException("Connection reset by peer (simulated)"))),
        failure("IOException under HoodieIOException", EmbeddingResult.Status.RETRYABLE_FAILURE,
            () -> new HoodieIOException("open failed", new IOException("timeout (simulated)"))),
        failure("IOException that is its own cause", EmbeddingResult.Status.RETRYABLE_FAILURE, SelfCausedIOException::new));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("failures")
  void sameFailureSameResultOnEveryProvider(String name, EmbeddingResult.Status expected, Supplier<Exception> failure)
      throws Exception {
    EmbeddingResult vertex = embedFailing(vertex(), failure);
    EmbeddingResult dummy = embedFailing(dummy(), failure);
    assertEquals(expected, vertex.getStatus(), name + ": " + vertex.getError());
    assertEquals(vertex.getStatus(), dummy.getStatus(), name + " (dummy multimodal)");
    assertEquals(vertex.getError(), dummy.getError(), name + " (dummy multimodal)");
    assertEquals(ImageFailures.result(input(), failure.get()).getError(), vertex.getError(), name);
    if (expected == EmbeddingResult.Status.PERMANENT_FAILURE) {
      assertTrue(vertex.getError().contains(input().describeIdentity()), name + ": " + vertex.getError());
    }
    assertEquals(0, stub.requests().size(), "an image that failed preparation sends no request");
  }

  @Test
  void unexpectedRuntimeFailureFailsTheRoundOnEveryProvider() throws Exception {
    for (EmbeddingClient client : Arrays.asList(vertex(), dummy())) {
      EmbeddingComputePlatformException failure = assertThrows(EmbeddingComputePlatformException.class,
          () -> embedFailing(client, () -> new IllegalStateException("Image raster budget assertion failed")));
      assertTrue(failure.getMessage().contains(input().describeIdentity()), failure.getMessage());
      assertTrue(failure.getCause() instanceof IllegalStateException);
    }
    assertEquals(0, stub.requests().size(), "a preparation invariant failure sends no request");
  }

  /** A malformed stored-input reference fails the run on every provider, naming the row; no request is sent. */
  @Test
  void referenceFaultFailsTheRunOnEveryProvider() throws Exception {
    for (EmbeddingClient client : Arrays.asList(vertex(), dummy())) {
      EmbeddingComputePlatformException e = assertThrows(EmbeddingComputePlatformException.class,
          () -> embedFailing(client, () -> new BlobReferenceException("OUT_OF_LINE blob has null reference")), client.getClass().getSimpleName());
      assertTrue(e.getMessage().contains(input().describeIdentity()) && e.getMessage().contains("OUT_OF_LINE blob has null reference"), e.getMessage());
    }
    assertEquals(0, stub.requests().size(), "a malformed reference sends no request");
  }

  private static EmbeddingInput input() {
    return new EmbeddingInput("f", 0, EmbeddingTableSchema.MODALITY_IMAGE, null);
  }

  /** A failure whose cause chain loops on itself; the classification must still end. */
  private static final class SelfCausedIOException extends IOException {
    SelfCausedIOException() {
      super("self-caused (simulated)");
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }

  private static Arguments failure(String name, EmbeddingResult.Status expected, Supplier<Exception> failure) {
    return Arguments.of(name, expected, failure);
  }

  /** Embeds one image input whose preparation throws a fresh {@code failure}. */
  private static EmbeddingResult embedFailing(EmbeddingClient client, Supplier<Exception> failure) throws Exception {
    EmbeddingInput input = EmbeddingInput.storedImage("f", 0, EmbeddingTableSchema.MODALITY_IMAGE, null, () -> {
      Exception e = failure.get();
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw (RuntimeException) e;
    });
    return client.embed(Collections.singletonList(input)).get(0);
  }

  private EmbeddingClient vertex() throws IOException {
    Path tokenFile = tempDir.resolve("vertex.token");
    Files.write(tokenFile, "stub-token-0002\n".getBytes(StandardCharsets.UTF_8));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), stub.url() + "/publishers/google/models/multimodalembedding@001:predict");
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tokenFile.toString());
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient();
    client.init(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, 128, null, VertexMultimodalEmbeddingClient.PROVIDER,
        "multimodalembedding@001"), props);
    return client;
  }

  private static EmbeddingClient dummy() {
    DummyMultimodalEmbeddingClient client = new DummyMultimodalEmbeddingClient();
    client.init(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, 8, null, DummyMultimodalEmbeddingClient.PROVIDER, ALIAS),
        new TypedProperties());
    return client;
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
