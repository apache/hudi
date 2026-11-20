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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** {@link ErrorText#truncate}: error text cut to a column's char limit, never inside a surrogate pair. */
public class TestErrorText {

  @Test
  void keepsTextThatFits() {
    assertNull(ErrorText.truncate(null, 5));
    assertEquals("abc", ErrorText.truncate("abc", 5));
    assertEquals("abcde", ErrorText.truncate("abcde", 5), "an exact fit");
    assertEquals("", ErrorText.truncate("", 0));
  }

  @Test
  void cutsLongerText() {
    assertEquals("abc", ErrorText.truncate("abcdef", 3));
    assertEquals("a", ErrorText.truncate("abcdef", 1));
  }

  @Test
  void limitOfZeroOrLessKeepsNothing() {
    assertEquals("", ErrorText.truncate("abc", 0));
    assertEquals("", ErrorText.truncate("abc", -1));
    assertNull(ErrorText.truncate(null, 0));
  }

  @Test
  void neverKeepsHalfASurrogatePair() {
    String text = "ab😀cd";
    assertEquals("ab", ErrorText.truncate(text, 3), "the cut falls inside the pair");
    assertEquals("ab😀", ErrorText.truncate(text, 4), "the cut falls after the pair");
    assertEquals("", ErrorText.truncate("😀x", 1));
  }
}
