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

package org.apache.hudi.common.util;

import javax.annotation.Nullable;

/**
 * Simple utility for operations on strings.
 */
public class StringUtils {

  public static final String EMPTY_STRING = "";

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
    return join(elements, "");
  }

  public static <T> String joinUsingDelim(String delim, final String... elements) {
    return join(elements, delim);
  }

  public static String join(final String[] array, final String separator) {
    if (array == null) {
      return null;
    }
    return org.apache.hadoop.util.StringUtils.join(separator, array);
  }

  public static String toHexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.length() == 0;
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
    return obj == null ? null : obj.toString();
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
}
