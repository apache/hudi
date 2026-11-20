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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The PDF chunker's configuration and its tokenizer selection. */
public class TestPdfChunkerConfig {

  private static final Tokenizer BPE = new OpenAiBpeTokenizer();

  @Test
  public void maxTokensUpperBound() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "8191");
    assertEquals(8191, PdfChunkerConfig.from(props, BPE).maxTokens());
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16");
    assertEquals(16, PdfChunkerConfig.from(props, BPE).maxTokens());
  }

}
