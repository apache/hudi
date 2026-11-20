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
import org.apache.hudi.utilities.unstructured.embedding.image.PreparedImage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * DUMMY multimodal embedding client, used only when a model names provider {@value #PROVIDER}: a text row gets
 * {@link DummyEmbeddingClient}'s vector, and an image row is prepared like any multimodal client's ({@link EmbeddingInput#image()},
 * decode and resize in memory) and gets a deterministic unit vector seeded by the prepared image's encoded bytes
 * ({@link #vectorForImage}), so a test can tell which image a vector was computed from. A failure to prepare the image is that
 * input's outcome ({@link ImageFailures}). It reads no configuration besides the model's.
 */
public class DummyMultimodalEmbeddingClient implements MultimodalEmbeddingClient {

  /** Provider id of this client ({@code hoodie.unstructured.embedding.model.<alias>.provider}); used only when named. */
  public static final String PROVIDER = "dummy-multimodal";

  private EmbeddingModelSpec spec;

  /** Text and images, whatever the model name. */
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
    List<EmbeddingResult> results = new ArrayList<>(inputs.size());
    for (EmbeddingInput input : inputs) {
      results.add(embedOne(input));
    }
    return results;
  }

  private EmbeddingResult embedOne(EmbeddingInput input) {
    if (!EmbeddingTableSchema.MODALITY_IMAGE.equals(input.getModality())) {
      return EmbeddingResult.ok(DummyEmbeddingClient.vectorFor(input.getSourcePathSha(), input.getChunkId(), spec.getDimension()));
    }
    if (input.image() == null) {
      // compute builds every image input of a multimodal client with its preparation: a missing one is a broken invariant
      throw new EmbeddingComputePlatformException("image input without a prepared image (model " + spec.getName() + ")");
    }
    PreparedImage image;
    try {
      image = input.image().get();
    } catch (IOException | RuntimeException e) {
      return ImageFailures.result(input, e);
    }
    return EmbeddingResult.ok(vectorForImage(image.getEncoded(), spec.getDimension()));
  }

  /** The vector of a prepared image: a unit vector seeded by its encoded bytes. */
  public static float[] vectorForImage(byte[] encoded, int dimension) {
    return DummyEmbeddingClient.unitVector(Arrays.hashCode(encoded), dimension);
  }
}
