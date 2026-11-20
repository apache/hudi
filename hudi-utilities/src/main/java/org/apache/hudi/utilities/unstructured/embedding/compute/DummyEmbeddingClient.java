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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A dummy embedding client for tests and pipeline checks, not for real embeddings: it returns deterministic random unit
 * vectors seeded by source_path_sha and chunk_id, whatever the input text.
 */
public class DummyEmbeddingClient implements EmbeddingClient {

  /** Provider id of this client ({@code hoodie.unstructured.embedding.model.<alias>.provider}); used only when named. */
  public static final String PROVIDER = "dummy";

  private EmbeddingModelSpec spec;

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
    return EmbeddingResult.ok(vectorFor(input.getSourcePathSha(), input.getChunkId(), spec.getDimension()));
  }

  public static float[] vectorFor(String sourcePathSha, long chunkId, int dimension) {
    Random random = new Random((sourcePathSha + "#" + chunkId).hashCode());
    float[] vector = new float[dimension];
    double norm = 0;
    for (int i = 0; i < dimension; i++) {
      vector[i] = (float) random.nextGaussian();
      norm += vector[i] * vector[i];
    }
    float scale = (float) (1.0 / Math.sqrt(norm));
    for (int i = 0; i < dimension; i++) {
      vector[i] *= scale;
    }
    return vector;
  }
}
