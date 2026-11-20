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

import org.apache.hudi.utilities.unstructured.embedding.text.Utf16;

/** Cuts error text to a column's char limit. */
public final class ErrorText {

  private ErrorText() {
  }

  /**
   * {@code text} cut to at most {@code maxChars} chars, one fewer when the cut would keep the high half of a surrogate pair
   * without its low half; {@code null} stays {@code null}, and a {@code maxChars} of 0 or less keeps nothing.
   */
  public static String truncate(String text, int maxChars) {
    if (text == null || text.length() <= maxChars) {
      return text;
    }
    if (maxChars <= 0) {
      return "";
    }
    int end = Utf16.floor(text, maxChars, true);
    return text.substring(0, end);
  }
}
