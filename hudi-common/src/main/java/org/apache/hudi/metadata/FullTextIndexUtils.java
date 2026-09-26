/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.exception.HoodieIOException;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Tokenizer, key layout and bitmap helpers shared by the writer and the reader of the full-text index.
 *
 * <p>Each (term, data file) pair has two entries in the index partition, sorting in separate key ranges:
 * a presence entry {@code P$<term>$<fileId>$<unit>} and a positions entry {@code B$<term>$<fileId>$<unit>}
 * holding the row positions of the term in the file. Each indexed data file also has one coverage marker
 * {@code U$<fileId>$<unit>}, written and removed together with its postings; a reader may only prune a file
 * whose marker it can see. The unit identifies one immutable data file of the file group,
 * {@code b:<baseInstantTime>} for a base file.
 */
public final class FullTextIndexUtils {

  public static final int TOKENIZER_VERSION = 1;
  public static final int MAX_TOKEN_BYTES = 64;
  public static final double DEFAULT_DENSE_RATIO = 0.25d;

  public static final String OPTION_DENSE_RATIO = "dense_ratio";
  public static final String OPTION_TOKENIZER_VERSION = "tokenizer_version";

  private static final String SEPARATOR = "$";
  private static final String PRESENCE_FAMILY = "P";
  private static final String POSITIONS_FAMILY = "B";
  private static final String MARKER_FAMILY = "U";
  private static final String BASE_UNIT_PREFIX = "b:";

  private FullTextIndexUtils() {
  }

  /**
   * Splits on every code point that is not a Unicode letter or decimal digit, lowercases with
   * {@link Locale#ROOT}, drops tokens longer than {@link #MAX_TOKEN_BYTES} UTF-8 bytes, and removes
   * duplicates, keeping first-occurrence order. Null or empty input yields no tokens.
   */
  public static Set<String> tokenize(String text) {
    Set<String> tokens = new LinkedHashSet<>();
    if (text == null || text.isEmpty()) {
      return tokens;
    }
    int start = -1;
    int i = 0;
    while (i < text.length()) {
      int cp = text.codePointAt(i);
      boolean tokenChar = Character.isLetter(cp) || Character.isDigit(cp);
      if (tokenChar && start < 0) {
        start = i;
      } else if (!tokenChar && start >= 0) {
        addToken(tokens, text.substring(start, i));
        start = -1;
      }
      i += Character.charCount(cp);
    }
    if (start >= 0) {
      addToken(tokens, text.substring(start));
    }
    return tokens;
  }

  private static void addToken(Set<String> tokens, String raw) {
    String token = raw.toLowerCase(Locale.ROOT);
    if (token.getBytes(StandardCharsets.UTF_8).length <= MAX_TOKEN_BYTES) {
      tokens.add(token);
    }
  }

  public static String baseUnit(String baseInstantTime) {
    return BASE_UNIT_PREFIX + baseInstantTime;
  }

  public static String presenceKey(String term, String fileId, String unit) {
    return PRESENCE_FAMILY + SEPARATOR + term + SEPARATOR + fileId + SEPARATOR + unit;
  }

  public static String positionsKey(String term, String fileId, String unit) {
    return POSITIONS_FAMILY + SEPARATOR + term + SEPARATOR + fileId + SEPARATOR + unit;
  }

  public static String markerKey(String fileId, String unit) {
    return MARKER_FAMILY + SEPARATOR + fileId + SEPARATOR + unit;
  }

  public static String presencePrefix(String term) {
    return PRESENCE_FAMILY + SEPARATOR + term + SEPARATOR;
  }

  /**
   * Parses a presence or positions key into {term, fileId, unit}. Terms and file ids never contain the
   * separator: the tokenizer only keeps letters and digits, and file ids are UUID based.
   */
  public static String[] parseKey(String key) {
    String[] parts = key.split("\\$", 4);
    if (parts.length != 4 || !(PRESENCE_FAMILY.equals(parts[0]) || POSITIONS_FAMILY.equals(parts[0]))) {
      throw new IllegalArgumentException("Not a full-text index key: " + key);
    }
    return new String[] {parts[1], parts[2], parts[3]};
  }

  public static ByteBuffer serialize(RoaringBitmap bitmap) {
    bitmap.runOptimize();
    ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
    bitmap.serialize(buffer);
    buffer.flip();
    return buffer;
  }

  public static RoaringBitmap deserialize(ByteBuffer buffer) {
    RoaringBitmap bitmap = new RoaringBitmap();
    try {
      bitmap.deserialize(buffer.duplicate());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize full-text index positions", e);
    }
    return bitmap;
  }
}
