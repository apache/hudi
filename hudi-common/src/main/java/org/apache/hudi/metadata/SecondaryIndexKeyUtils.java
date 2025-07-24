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

public class SecondaryIndexKeyUtils {

  /**
   * Use this function if you want to get both record key and secondary key.
   *
   * @returns pair of secondary key, record key.
   * */
  public static Pair<String, String> getSecondaryKeyRecordKeyPair(String key) {
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(key);
    return Pair.of(unescapeSpecialChars(key.substring(0, delimiterIndex)), unescapeSpecialChars(key.substring(delimiterIndex + 1)));
  }

  /**
   * Use this function if you want to get both record key and secondary key.
   *
   * @returns pair of secondary key, record key.
   * */
  public static Pair<String, String> getRecordKeySecondaryKeyPair(String key) {
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(key);
    return Pair.of(unescapeSpecialChars(key.substring(delimiterIndex + 1)), unescapeSpecialChars(key.substring(0, delimiterIndex)));
  }

  public static String getRecordKeyFromSecondaryIndexKey(String key) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    // we need to extract the primary key from the payload key
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(key);
    return unescapeSpecialChars(key.substring(delimiterIndex + 1));
  }

  public static String getSecondaryKeyFromSecondaryIndexKey(String key) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    // we need to extract the secondary key from the payload key
    return unescapeSpecialChars(getUnescapedSecondaryKeyFromSecondaryIndexKey(key));
  }

  public static String getUnescapedSecondaryKeyFromSecondaryIndexKey(String key) {
    // the payload key is in the format of "secondaryKey$primaryKey"
    // we need to extract the secondary key from the payload key
    int delimiterIndex = getSecondaryIndexKeySeparatorPosition(key);
    return key.substring(0, delimiterIndex);
  }

  public static String constructSecondaryIndexKey(String secondaryKey, String recordKey) {
    return escapeSpecialChars(secondaryKey) + SECONDARY_INDEX_RECORD_KEY_SEPARATOR + escapeSpecialChars(recordKey);
  }

  public static String escapeSpecialChars(String str) {
    StringBuilder escaped = new StringBuilder();
    for (char c : str.toCharArray()) {
      if (c == '\\' || c == '$') {
        escaped.append('\\');  // Add escape character
      }
      escaped.append(c);  // Add the actual character
    }
    return escaped.toString();
  }

  private static int getSecondaryIndexKeySeparatorPosition(String key) {
    int delimiterIndex = -1;
    boolean isEscape = false;

    // Find the delimiter index while skipping escaped $
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (c == '\\' && !isEscape) {
        isEscape = true;
      } else if (c == '$' && !isEscape) {
        delimiterIndex = i;
        break;
      } else {
        isEscape = false;
      }
    }
    checkState(delimiterIndex != -1, "Invalid encoded key format");
    return delimiterIndex;
  }

  static String unescapeSpecialChars(String str) {
    StringBuilder unescaped = new StringBuilder();
    boolean isEscape = false;
    for (char c : str.toCharArray()) {
      if (isEscape) {
        unescaped.append(c);
        isEscape = false;
      } else if (c == '\\') {
        isEscape = true;  // Set escape flag to skip next character
      } else {
        unescaped.append(c);
      }
    }
    return unescaped.toString();
  }

}
