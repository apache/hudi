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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A test preprocessor for the extension a test maps to it ({@code EmbeddingRoundTestBase} maps {@code tif}): one text chunk per
 * {@code |}-separated part.
 */
public class TextPipePreprocessor implements FilePreprocessor {

  @Override
  public String name() {
    return "text-pipe-v1";
  }

  @Override
  public SourceKind sourceKind() {
    return SourceKind.DOCUMENT;
  }

  @Override
  public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
      throws IOException {
    String text;
    try (InputStream in = bytes.open()) {
      text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<Chunk> chunks = new ArrayList<>();
    for (String part : text.split("\\|")) {
      chunks.add(Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).text(part).tokenCount(tokenizer.countTokens(part)).build());
    }
    Iterator<Chunk> it = chunks.iterator();
    return new ClosableIterator<Chunk>() {
      @Override
      public void close() {
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Chunk next() {
        return it.next();
      }
    };
  }
}
