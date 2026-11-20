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

/** UTF-16 cut positions shared by token budgets and bounded text writers. */
public final class Utf16 {
  private Utf16() {
  }

  /** Floors a cut between the two halves of a surrogate pair. */
  public static int floor(CharSequence text, int end) {
    return floor(text, end, false);
  }

  /** Floors a pair boundary, also trimming an unpaired trailing high surrogate when requested by a bounded writer. */
  public static int floor(CharSequence text, int end, boolean trimUnpairedHigh) {
    return floor(end, end > 0 ? text.charAt(end - 1) : 0,
        trimUnpairedHigh || (end < text.length() && Character.isLowSurrogate(text.charAt(end))));
  }

  /** The bounded writer's cut in a char-array slice, trimming any trailing high surrogate. */
  public static int floor(char[] text, int offset, int end) {
    return floor(end, end > 0 ? text[offset + end - 1] : 0, true);
  }

  private static int floor(int end, char previous, boolean split) {
    return end > 0 && split && Character.isHighSurrogate(previous) ? end - 1 : end;
  }
}
