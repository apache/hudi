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

package org.apache.hudi.metadata;

import org.apache.hudi.common.util.collection.Pair;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR_CHAR;

public class SecondaryIndexKeyUtils {

  // Null character (ASCII 0) used to represent null strings
  public static final char NULL_CHAR = '\0';
  // Escape character
  public static final char ESCAPE_CHAR = '\\';

  /**
   * Use this function if you want to get both record key and secondary key.
   *
   * @returns pair of secondary key, record key.
   * */
  public static Pair<String, String> getSecondaryKeyRecordKeyPair(String secIdxRecKey) {
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(secIdxRecKey);
    return Pair.of(unescapeSpecialChars(secIdxRecKey.substring(0, delimiterIndex)), unescapeSpecialChars(secIdxRecKey.substring(delimiterIndex + 1)));
  }

  /**
   * Use this function if you want to get both record key and secondary key.
   *
   * @returns pair of secondary key, record key.
   * */
  public static Pair<String, String> getRecordKeySecondaryKeyPair(String secIdxRecKey) {
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(secIdxRecKey);
    return Pair.of(unescapeSpecialChars(secIdxRecKey.substring(delimiterIndex + 1)), unescapeSpecialChars(secIdxRecKey.substring(0, delimiterIndex)));
  }

  /**
   * Extracts the record key portion from an encoded secondary index key.
   *
   * @param secIdxRecKey the encoded key in the form "escapedSecondaryKey$escapedRecordKey"
   * @return the unescaped record key, or {@code null} if the record key was {@code null}
   * @throws IllegalStateException if the key format is invalid (i.e., no unescaped separator found)
   */
  public static String getRecordKeyFromSecondaryIndexKey(String secIdxRecKey) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    // we need to extract the primary key from the payload key
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(secIdxRecKey);
    return unescapeSpecialChars(secIdxRecKey.substring(delimiterIndex + 1));
  }

  /**
   * Extracts the secondary key portion from an encoded secondary index key.
   *
   * @param secIdxRecKey the encoded key in the form "escapedSecondaryKey$escapedRecordKey"
   * @return the unescaped secondary key, or {@code null} if the secondary key was {@code null}
   * @throws IllegalStateException if the key format is invalid (i.e., no unescaped separator found)
   */
  public static String getSecondaryKeyFromSecondaryIndexKey(String secIdxRecKey) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    // we need to extract the secondary key from the payload key
    return unescapeSpecialChars(getUnescapedSecondaryKeyFromSecondaryIndexKey(secIdxRecKey));
  }

  public static String getUnescapedSecondaryKeyFromSecondaryIndexKey(String secIdxRecKey) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    // we need to extract the secondary key from the payload key
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(secIdxRecKey);
    return secIdxRecKey.substring(0, delimiterIndex);
  }

  /**
   * Constructs an encoded secondary index key by escaping the given secondary and record keys,
   * and concatenating them with the separator {@code "$"}.
   *
   * @param unescapedSecKey the secondary key (can be {@code null})
   * @param unescapedRecordKey the record key (can be {@code null})
   * @return a string representing the encoded secondary index key
   */
  public static String constructSecondaryIndexKey(String unescapedSecKey, String unescapedRecordKey) {
    return escapeSpecialChars(unescapedSecKey) + SECONDARY_INDEX_RECORD_KEY_SEPARATOR + escapeSpecialChars(unescapedRecordKey);
  }

  /**
   * Escapes special characters in a string. If the input is null, returns a string containing only the null character.
   * For non-null strings, escapes backslash, dollar sign, and null character.
   */
  public static String escapeSpecialChars(String str) {
    if (str == null) {
      return String.valueOf(NULL_CHAR);
    }

    StringBuilder escaped = new StringBuilder();
    for (char c : str.toCharArray()) {
      if (c == ESCAPE_CHAR || c == SECONDARY_INDEX_RECORD_KEY_SEPARATOR_CHAR || c == NULL_CHAR) {
        escaped.append(ESCAPE_CHAR);  // Add escape character
      }
      escaped.append(c);  // Add the actual character
    }
    return escaped.toString();
  }

  // Find the position of the first unescaped '$' char in the string.
  private static int getSecondaryIndexKeySeparatorPosition(String key) {
    int delimiterIndex = -1;
    boolean isEscape = false;

    // Find the delimiter index while skipping escaped $
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (c == ESCAPE_CHAR && !isEscape) {
        isEscape = true;
      } else if (c == SECONDARY_INDEX_RECORD_KEY_SEPARATOR_CHAR && !isEscape) {
        delimiterIndex = i;
        break;
      } else {
        isEscape = false;
      }
    }
    checkState(delimiterIndex != -1, "Invalid encoded key format");
    return delimiterIndex;
  }

  /**
   * Unescapes special characters in a string. If the input is a single null character, returns null.
   * For other strings, unescapes backslash, dollar sign, and null character.
   */
  public static String unescapeSpecialChars(String str) {

    if (str.equals(String.valueOf(NULL_CHAR))) {
      return null;
    }

    StringBuilder unescaped = new StringBuilder();
    boolean isEscape = false;
    for (char c : str.toCharArray()) {
      if (isEscape) {
        unescaped.append(c);
        isEscape = false;
      } else if (c == ESCAPE_CHAR) {
        isEscape = true;  // Set escape flag to skip next character
      } else {
        unescaped.append(c);
      }
    }
    return unescaped.toString();
  }

}
