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

package org.apache.hudi.utilities.unstructured.embedding.text;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chunker side of the shared {@link Tokenizer} contract: type selection with
 * {@code openai-bpe} as the default, the word-count definition and the default cut helpers.
 * The OpenAI counts and the inclusive 181→182 index are pinned in {@link TestOpenAiBpeTokenizer}.
 */
class TestTokenizers {

  /** Counts characters; a deliberately non-word unit for the default cut helpers. */
  static final class CharTokenizer implements Tokenizer {
    @Override
    public int countTokens(String text) {
      return text.codePointCount(0, text.length());
    }

    @Override
    public String id() {
      return "chars";
    }

    @Override
    public TokenUnit unit() {
      return TokenUnit.MODEL_TOKENS;
    }
  }

  @Test
  void selectsByType() {
    TypedProperties props = new TypedProperties();
    Tokenizer byDefault = TokenizerFactory.create(props);
    assertTrue(byDefault instanceof OpenAiBpeTokenizer, "openai-bpe is the default");
    assertEquals("openai-bpe:cl100k_base", byDefault.id());
    assertEquals(TokenizerFactory.OPENAI_BPE, TokenizerFactory.DEFAULT_TYPE);

    assertEquals("hoodie.unstructured.chunk.tokenizer.encoding", EmbeddingTableConfig.TOKENIZER_ENCODING.key());
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), "openai-bpe");
    props.setProperty(EmbeddingTableConfig.TOKENIZER_ENCODING.key(), "o200k_base");
    assertEquals("openai-bpe:o200k_base", TokenizerFactory.create(props).id());

    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), "word-count");
    assertEquals("word-count", TokenizerFactory.create(props).id());

    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), "no-such-type");
    IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class, () -> TokenizerFactory.create(props));
    assertEquals("Unknown hoodie.unstructured.chunk.tokenizer.type 'no-such-type'; expected word-count or openai-bpe, "
        + "or set hoodie.unstructured.chunk.tokenizer.class", unknown.getMessage());
  }

  @Test
  void wordCountDefinition() {
    Tokenizer words = new WordCountTokenizer();
    assertEquals(4, words.countTokens("a  b\tc\n\nd"));
    assertEquals(4, words.countTokens("  one two\tthree\nfour "));
    assertEquals(0, words.countTokens(""));
    assertEquals(0, words.countTokens(null));
    assertEquals(1, words.countTokens("A\uD83D\uDE00B"));  // escaped non-ASCII test data
    assertEquals(TokenUnit.WORDS, words.unit());
  }

  @Test
  void interfaceContract() {
    Tokenizer chars = new CharTokenizer();
    String text = "ab\uD83D\uDE00cd\uD83D\uDE00\uD83D\uDE00e";   // 8 code points, 11 chars
    for (int max = 0; max <= 9; max++) {
      int end = chars.prefixEndWithin(text, max);
      assertTrue(chars.countTokens(text.substring(0, end)) <= max);
      assertFalse(splitsPair(text, end), "prefix end " + end + " splits a surrogate pair");
    }
    assertEquals(text.length(), chars.prefixEndWithin(text, 8));
    assertEquals(0, chars.prefixEndWithin(text, 0));
    assertEquals(0, chars.prefixEndWithin("", 5));
  }

  @Test
  void defaultHelpersAgreeWithOpenAiOnRandomText() {
    Tokenizer bpe = new OpenAiBpeTokenizer();
    Random random = new Random(42);
    String alphabet = "ab cd. ef\n\u00E9\u4E2D\uD83D\uDE00";  // escaped non-ASCII test data
    for (int round = 0; round < 50; round++) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 200; i++) {
        sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
      }
      String text = sb.toString();
      int max = 1 + random.nextInt(60);
      int end = bpe.prefixEndWithin(text, max);
      assertTrue(bpe.countTokens(text.substring(0, end)) <= max);
    }
  }

  private static boolean splitsPair(String text, int index) {
    return index > 0 && index < text.length() && Character.isHighSurrogate(text.charAt(index - 1))
        && Character.isLowSurrogate(text.charAt(index));
  }
}
