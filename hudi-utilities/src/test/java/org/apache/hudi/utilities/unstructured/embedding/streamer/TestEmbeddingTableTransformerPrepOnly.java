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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link EmbeddingTableTransformer} refuses the instant-less {@code apply}: it needs the commit started before the transformer.
 */
@Tag("functional")
public class TestEmbeddingTableTransformerPrepOnly extends EmbeddingRoundTestBase {

  private TypedProperties prepOnly() {
    return props(table(), EmbeddingTableConfig.MODE_PREP_ONLY,
        EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
  }

  @Test
  public void theTransformerNeedsTheInstantStartedBeforeIt() {
    assertThrows(HoodieException.class, () -> new EmbeddingTableTransformer().apply(jsc(), spark(), null, prepOnly()));
  }

}
