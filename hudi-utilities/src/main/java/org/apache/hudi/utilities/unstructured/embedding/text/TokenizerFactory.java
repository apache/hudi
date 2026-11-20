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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

/**
 * Creates the {@link Tokenizer} of the data prep job. {@code hoodie.unstructured.chunk.tokenizer.class}, when set, wins
 * (loaded by reflection through its public no-arg constructor); otherwise {@code hoodie.unstructured.chunk.tokenizer.type}
 * picks a built-in tokenizer.
 */
public final class TokenizerFactory {

  /** Tokenizer type of {@link WordCountTokenizer}. */
  public static final String WORD_COUNT = WordCountTokenizer.TOKENIZER_ID;
  /** Tokenizer type of {@link OpenAiBpeTokenizer}, whose encoding is {@code hoodie.unstructured.chunk.tokenizer.encoding}. */
  public static final String OPENAI_BPE = OpenAiBpeTokenizer.TYPE;
  /**
   * Default of {@code hoodie.unstructured.chunk.tokenizer.type}: the OpenAI BPE tokenizer, at its
   * {@link OpenAiBpeTokenizer#DEFAULT_ENCODING} unless {@code tokenizer.encoding} is set.
   */
  public static final String DEFAULT_TYPE = OPENAI_BPE;

  private TokenizerFactory() {
  }

  /**
   * Resolves the tokenizer configured in {@code props}.
   *
   * @throws IllegalArgumentException for an unknown tokenizer type or {@link #OPENAI_BPE} encoding, or a configured
   *     tokenizer class that cannot be loaded, does not implement {@link Tokenizer} or has no usable no-arg constructor
   */
  public static Tokenizer create(TypedProperties props) {
    String tokenizerClass = ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.TOKENIZER_CLASS, null);
    if (!StringUtils.isNullOrEmpty(tokenizerClass)) {
      return loadTokenizerClass(tokenizerClass.trim());
    }
    String type = ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.TOKENIZER_TYPE, DEFAULT_TYPE).trim();
    switch (type) {
      case WORD_COUNT:
        return new WordCountTokenizer();
      case OPENAI_BPE:
        return new OpenAiBpeTokenizer(ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.TOKENIZER_ENCODING, OpenAiBpeTokenizer.DEFAULT_ENCODING).trim());
      default:
        throw new IllegalArgumentException("Unknown " + EmbeddingTableConfig.TOKENIZER_TYPE.key() + " '" + type
            + "'; expected " + WORD_COUNT + " or " + OPENAI_BPE + ", or set "
            + EmbeddingTableConfig.TOKENIZER_CLASS.key());
    }
  }

  private static Tokenizer loadTokenizerClass(String className) {
    String config = "Config " + EmbeddingTableConfig.TOKENIZER_CLASS.key() + "=" + className;
    Class<?> clazz;
    try {
      clazz = ReflectionUtils.getClass(className);
    } catch (HoodieException e) {
      throw new IllegalArgumentException(config + " cannot be loaded", e);
    }
    if (!Tokenizer.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException(config + " does not implement " + Tokenizer.class.getName());
    }
    try {
      return ReflectionUtils.loadClass(className);
    } catch (HoodieException e) {
      throw new IllegalArgumentException(config + " cannot be instantiated through a public no-arg constructor", e);
    }
  }
}
