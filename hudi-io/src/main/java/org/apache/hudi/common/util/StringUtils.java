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

package org.apache.hudi.common.util;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Simple utility for operations on strings.
 */
public class StringUtils {

  public static final char[] HEX_CHAR = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  public static final String EMPTY_STRING = "";
  // Represents a failed index search
  public static final int INDEX_NOT_FOUND = -1;

  /**
   * <p>
   * Joins the elements of the provided array into a single String containing the provided list of elements.
   * </p>
   *
   * <p>
   * No separator is added to the joined String. Null objects or empty strings within the array are represented by empty
   * strings.
   * </p>
   *
   * <pre>
   * StringUtils.join(null)            = null
   * StringUtils.join([])              = ""
   * StringUtils.join([null])          = ""
   * StringUtils.join(["a", "b", "c"]) = "abc"
   * StringUtils.join([null, "", "a"]) = "a"
   * </pre>
   */
  public static <T> String join(final String... elements) {
    return join(elements, EMPTY_STRING);
  }

  public static <T> String joinUsingDelim(String delim, final String... elements) {
    return join(elements, delim);
  }

  public static String join(final String[] array, final String separator) {
    if (array == null) {
      return null;
    }
    return String.join(separator, array);
  }

  /**
   * Wrapper of {@link java.lang.String#join(CharSequence, Iterable)}.
   *
   * Allow return {@code null} when {@code Iterable} is {@code null}.
   */
  public static String join(CharSequence delimiter, Iterable<? extends CharSequence> elements) {
    if (elements == null) {
      return null;
    }
    return String.join(delimiter, elements);
  }

  public static String join(final List<String> list, final String separator) {
    if (list == null || list.size() == 0) {
      return null;
    }
    return String.join(separator, list.toArray(new String[0]));
  }

  public static <K, V> String join(final Map<K, V> map) {
    return map.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
  }

  public static String toHexString(byte[] bytes) {
    return new String(encodeHex(bytes));
  }

  public static char[] encodeHex(byte[] data) {
    int l = data.length;
    char[] out = new char[l << 1];
    int i = 0;

    for (int var4 = 0; i < l; ++i) {
      out[var4++] = HEX_CHAR[(240 & data[i]) >>> 4];
      out[var4++] = HEX_CHAR[15 & data[i]];
    }

    return out;
  }

  public static byte[] getUTF8Bytes(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  public static String fromUTF8Bytes(byte[] bytes) {
    return fromUTF8Bytes(bytes, 0, bytes.length);
  }

  public static String fromUTF8Bytes(byte[] bytes,
                                     int offset,
                                     int length) {
    if (bytes == null) {
      return null;
    }
    if (length == 0) {
      return "";
    }
    return new String(bytes, offset, length, StandardCharsets.UTF_8);
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.length() == 0;
  }

  public static boolean nonEmpty(String str) {
    return !isNullOrEmpty(str);
  }

  /**
   * Returns the given string if it is non-null; the empty string otherwise.
   *
   * @param string the string to test and possibly return
   * @return {@code string} itself if it is non-null; {@code ""} if it is null
   */
  public static String nullToEmpty(@Nullable String string) {
    return string == null ? "" : string;
  }

  public static String objToString(@Nullable Object obj) {
    if (obj == null) {
      return null;
    }
    return obj instanceof ByteBuffer ? toHexString(((ByteBuffer) obj).array()) : obj.toString();
  }

  /**
   * Returns the given string if it is nonempty; {@code null} otherwise.
   *
   * @param string the string to test and possibly return
   * @return {@code string} itself if it is nonempty; {@code null} if it is empty or null
   */
  public static @Nullable String emptyToNull(@Nullable String string) {
    return stringIsNullOrEmpty(string) ? null : string;
  }

  private static boolean stringIsNullOrEmpty(@Nullable String string) {
    return string == null || string.isEmpty();
  }

  /**
   * Splits input string, delimited {@code delimiter} into a list of non-empty strings
   * (skipping any empty string produced during splitting)
   */
  public static List<String> split(@Nullable String input, String delimiter) {
    if (isNullOrEmpty(input)) {
      return Collections.emptyList();
    }
    return Stream.of(input.split(delimiter)).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
  }

  public static String getSuffixBy(String input, int ch) {
    int i = input.lastIndexOf(ch);
    if (i == -1) {
      return input;
    }
    return input.substring(i);
  }

  public static String removeSuffixBy(String input, int ch) {
    int i = input.lastIndexOf(ch);
    if (i == -1) {
      return input;
    }
    return input.substring(0, i);
  }

  public static String truncate(String str, int headLength, int tailLength) {
    if (isNullOrEmpty(str) || str.length() <= headLength + tailLength) {
      return str;
    }
    String head = str.substring(0, headLength);
    String tail = str.substring(str.length() - tailLength);

    return head + "..." + tail;
  }

  /**
   * Compares two version name strings using maven's ComparableVersion class.
   *
   * @param version1 the first version to compare
   * @param version2 the second version to compare
   * @return a negative integer if version1 precedes version2, a positive
   * integer if version2 precedes version1, and 0 if and only if the two
   * versions are equal.
   */
  public static int compareVersions(String version1, String version2) {
    ComparableVersion v1 = new ComparableVersion(version1);
    ComparableVersion v2 = new ComparableVersion(version2);
    return v1.compareTo(v2);
  }

  /**
   * Replaces all occurrences of a String within another String.
   *
   * <p>A <code>null</code> reference passed to this method is a no-op.</p>
   *
   * <pre>
   * StringUtils.replace(null, *, *)        = null
   * StringUtils.replace("", *, *)          = ""
   * StringUtils.replace("any", null, *)    = "any"
   * StringUtils.replace("any", *, null)    = "any"
   * StringUtils.replace("any", "", *)      = "any"
   * StringUtils.replace("aba", "a", null)  = "aba"
   * StringUtils.replace("aba", "a", "")    = "b"
   * StringUtils.replace("aba", "a", "z")   = "zbz"
   * </pre>
   * <p>
   * This method is copied from hadoop StringUtils.
   *
   * @param text         text to search and replace in, may be null
   * @param searchString the String to search for, may be null
   * @param replacement  the String to replace it with, may be null
   * @return the text with any replacements processed,
   * <code>null</code> if null String input
   * @see #replace(String text, String searchString, String replacement, int max)
   */
  public static String replace(String text, String searchString, String replacement) {
    return replace(text, searchString, replacement, -1);
  }

  /**
   * Replaces a String with another String inside a larger String,
   * for the first <code>max</code> values of the search String.
   *
   * <p>A <code>null</code> reference passed to this method is a no-op.</p>
   *
   * <pre>
   * StringUtils.replace(null, *, *, *)         = null
   * StringUtils.replace("", *, *, *)           = ""
   * StringUtils.replace("any", null, *, *)     = "any"
   * StringUtils.replace("any", *, null, *)     = "any"
   * StringUtils.replace("any", "", *, *)       = "any"
   * StringUtils.replace("any", *, *, 0)        = "any"
   * StringUtils.replace("abaa", "a", null, -1) = "abaa"
   * StringUtils.replace("abaa", "a", "", -1)   = "b"
   * StringUtils.replace("abaa", "a", "z", 0)   = "abaa"
   * StringUtils.replace("abaa", "a", "z", 1)   = "zbaa"
   * StringUtils.replace("abaa", "a", "z", 2)   = "zbza"
   * StringUtils.replace("abaa", "a", "z", -1)  = "zbzz"
   * </pre>
   * <p>
   * This method is copied from hadoop StringUtils.
   *
   * @param text         text to search and replace in, may be null
   * @param searchString the String to search for, may be null
   * @param replacement  the String to replace it with, may be null
   * @param max          maximum number of values to replace, or <code>-1</code> if no maximum
   * @return the text with any replacements processed,
   * <code>null</code> if null String input
   */
  public static String replace(String text, String searchString, String replacement, int max) {
    if (isNullOrEmpty(text) || isNullOrEmpty(searchString) || replacement == null || max == 0) {
      return text;
    }
    int start = 0;
    int end = text.indexOf(searchString, start);
    if (end == INDEX_NOT_FOUND) {
      return text;
    }
    int replLength = searchString.length();
    int increase = replacement.length() - replLength;
    increase = (increase < 0 ? 0 : increase);
    increase *= (max < 0 ? 16 : (max > 64 ? 64 : max));
    StringBuilder buf = new StringBuilder(text.length() + increase);
    while (end != INDEX_NOT_FOUND) {
      buf.append(text, start, end).append(replacement);
      start = end + replLength;
      if (--max == 0) {
        break;
      }
      end = text.indexOf(searchString, start);
    }
    buf.append(text.substring(start));
    return buf.toString();
  }

  /**
   * Strips given characters from end of the string and returns the result
   * @param str - string to be stripped
   * @param stripChars - characters to strip
   */
  public static String stripEnd(final String str, final String stripChars) {
    int end;
    if (str == null || (end = str.length()) == 0) {
      return str;
    }

    if (stripChars == null) {
      while (end != 0 && Character.isWhitespace(str.charAt(end - 1))) {
        end--;
      }
    } else if (stripChars.isEmpty()) {
      return str;
    } else {
      while (end != 0 && stripChars.indexOf(str.charAt(end - 1)) != INDEX_NOT_FOUND) {
        end--;
      }
    }
    return str.substring(0, end);
  }

  /**
   * Concatenates two strings such that the total byte length does not exceed the threshold.
   * If the total byte length exceeds the threshold, the function will find the maximum length of the first string
   * that fits within the threshold and concatenate that with the second string.
   *
   * @param a         The first string
   * @param b         The second string
   * @param byteLengthThreshold The maximum byte length
   */
  public static String concatenateWithThreshold(String a, String b, int byteLengthThreshold) {
    // Convert both strings to byte arrays in UTF-8 encoding
    byte[] bytesA = getUTF8Bytes(a);
    byte[] bytesB = getUTF8Bytes(b);
    if (bytesB.length > byteLengthThreshold) {
      throw new IllegalArgumentException(String.format(
          "Length of the Second string to concatenate exceeds the threshold (%d > %d)",
          bytesB.length, byteLengthThreshold));
    }

    // Calculate total bytes
    int totalBytes = bytesA.length + bytesB.length;

    // If total bytes is within the threshold, return concatenated string
    if (totalBytes <= byteLengthThreshold) {
      return a + b;
    }

    // Calculate the maximum bytes 'a' can take
    int bestLength = getBestLength(a, byteLengthThreshold - bytesB.length);

    // Concatenate the valid substring of 'a' with 'b'
    return a.substring(0, bestLength) + b;
  }

  /**
   * Concatenates the string representation of each object in the list
   * and returns a single string. The result is capped at "lengthThreshold" characters.
   *
   * @param objectList      object list
   * @param lengthThreshold number of characters to cap the string
   * @return capped string representation of the object list
   * @param <T> type of the object
   */
  public static <T> String toStringWithThreshold(List<T> objectList, int lengthThreshold) {
    if (objectList == null || objectList.isEmpty()) {
      return "";
    }
    // For non-positive value, we will not do any truncation.
    if (lengthThreshold <= 0) {
      return objectList.toString();
    }
    StringBuilder sb = new StringBuilder();

    for (Object obj : objectList) {
      if (sb.length() >= lengthThreshold) {
        setLastThreeDots(sb);
        break;
      }

      String objString = (sb.length() > 0 ? "," : "") + obj;

      // Check if appending this string would exceed the limit
      if (sb.length() + objString.length() > lengthThreshold) {
        int remaining = lengthThreshold - sb.length();
        sb.append(objString, 0, remaining);
        setLastThreeDots(sb);
        break;
      } else {
        sb.append(objString);
      }
    }

    return sb.toString();
  }

  private static void setLastThreeDots(StringBuilder sb) {
    IntStream.range(1, 4).forEach(i -> {
      int loc = sb.length() - i;
      if (loc >= 0) {
        sb.setCharAt(loc, '.');
      }
    });
  }

  private static int getBestLength(String a, int threshold) {
    // Binary search to find the maximum length of substring of 'a' that fits within maxBytesForA
    int low = 0;
    int high = Math.min(a.length(), threshold);
    int bestLength = 0;

    while (low <= high) {
      int mid = (low + high) / 2;
      byte[] subABytes = getUTF8Bytes(a.substring(0, mid));

      if (subABytes.length <= threshold) {
        bestLength = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return bestLength;
  }
}
