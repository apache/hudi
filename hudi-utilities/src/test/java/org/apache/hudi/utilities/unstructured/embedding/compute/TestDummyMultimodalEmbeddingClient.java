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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreprocessingSpec;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageRefusedException;
import org.apache.hudi.utilities.unstructured.embedding.image.InProcessImageDecoder;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dummy multimodal client: provider {@value DummyMultimodalEmbeddingClient#PROVIDER} resolves to it; a text row gets the
 * dummy client's vector; an image row's vector depends on its prepared image's bytes only, deterministically; a failed
 * preparation is that input's outcome.
 */
public class TestDummyMultimodalEmbeddingClient {

  private static final int DIMENSION = 8;
  private static final ImagePreparer PREPARER = new ImagePreparer(new ImagePreprocessingSpec(
      ImagePreprocessingSpec.DEFAULT_LONG_SIDE, ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, 1_000_000,
      InProcessImageDecoder.class.getName()));

  @Test
  public void providerResolvesToTheMultimodalDummy() {
    EmbeddingModelSpec spec = spec();
    assertEquals(DummyMultimodalEmbeddingClient.class.getName(), EmbeddingClientFactory.clientClassName(spec));
    assertTrue(EmbeddingClientNames.PROVIDERS.contains(DummyMultimodalEmbeddingClient.PROVIDER));
    assertEquals(EmbeddingClient.TEXT_ONLY, new DummyEmbeddingClient().supportedModalities("any"), "the dummy client stays text only");
    assertEquals(EmbeddingClient.TEXT_AND_IMAGE, new DummyMultimodalEmbeddingClient().supportedModalities("any"));
  }

  @Test
  public void textRowsGetTheDummyVectorAndImageRowsAVectorOfTheirPreparedImage() throws IOException {
    byte[] first = ImageFixtures.image("png", 900, 600);
    byte[] other = ImageFixtures.image("jpeg", 300, 200);
    List<EmbeddingResult> results = client().embed(Arrays.asList(
        new EmbeddingInput("t", 2, EmbeddingTableSchema.MODALITY_TEXT, "words"),
        image("a", first), image("b", first), image("c", other)));
    assertArrayEquals(DummyEmbeddingClient.vectorFor("t", 2, DIMENSION), results.get(0).getVector());
    float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(PREPARER.prepare(ImageFixtures.source(first)).getEncoded(),
        DIMENSION);
    assertArrayEquals(expected, results.get(1).getVector(), "the vector of the prepared image");
    assertArrayEquals(expected, results.get(2).getVector(), "the same image gives the same vector, whatever its row");
    assertFalse(Arrays.equals(expected, results.get(3).getVector()), "another image gives another vector");
    double squaredNorm = 0;
    for (float value : expected) {
      squaredNorm += (double) value * value;
    }
    assertEquals(1.0, squaredNorm, 1e-6);
  }

  @Test
  public void failedPreparationIsThatInputsOutcome() {
    EmbeddingInput refused = EmbeddingInput.storedImage("r", 0, EmbeddingTableSchema.MODALITY_IMAGE, null, () -> {
      throw new ImageRefusedException(ImageRefusedException.REFUSED_BEFORE_DECODE + "too many pixels");
    });
    EmbeddingInput storage = EmbeddingInput.storedImage("s", 0, EmbeddingTableSchema.MODALITY_IMAGE, null, () -> {
      throw new BlobReadException(new IOException("Connection reset (simulated)"));
    });
    List<EmbeddingResult> results = client().embed(Arrays.asList(refused, storage, image("ok", ImageFixtures.image("png", 20, 20))));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus());
    assertTrue(results.get(0).getError().startsWith("source_path_sha="), results.get(0).getError());
    assertTrue(results.get(0).getError().contains(": " + ImageRefusedException.REFUSED_BEFORE_DECODE), results.get(0).getError());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus());
    assertEquals(EmbeddingResult.Status.OK, results.get(2).getStatus());
  }

  @Test
  public void imageInputWithoutItsPreparationBreaksTheInvariant() {
    EmbeddingInput bare = EmbeddingInput.storedImage("x", 0, EmbeddingTableSchema.MODALITY_IMAGE, null, null);
    assertThrows(EmbeddingComputePlatformException.class, () -> client().embed(Collections.singletonList(bare)));
  }

  private static EmbeddingInput image(String sourcePathSha, byte[] bytes) {
    return EmbeddingInput.storedImage(sourcePathSha, 0, EmbeddingTableSchema.MODALITY_IMAGE, null,
        () -> PREPARER.prepare(ImageFixtures.source(bytes)));
  }

  private static EmbeddingModelSpec spec() {
    return new EmbeddingModelSpec("mm", "emb_mm", DIMENSION, null, DummyMultimodalEmbeddingClient.PROVIDER, "mm");
  }

  private static DummyMultimodalEmbeddingClient client() {
    DummyMultimodalEmbeddingClient client = new DummyMultimodalEmbeddingClient();
    client.init(spec(), new TypedProperties());
    return client;
  }
}
