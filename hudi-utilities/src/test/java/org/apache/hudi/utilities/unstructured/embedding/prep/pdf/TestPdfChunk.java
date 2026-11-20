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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The chunk record holds exactly the contract's fields. */
class TestPdfChunk {

  @Test
  void fieldSet() {
    Set<String> fields = new TreeSet<>();
    for (Field field : PdfChunk.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        fields.add(field.getName());
      }
    }
    assertEquals(new TreeSet<>(Arrays.asList("sourcePathSha", "chunkId", "text", "tokenCount", "tokenizerId", "wordCount", "charCount",
        "overlapChars", "pageStart", "pageEnd", "status", "statusDetail")), fields);
  }
}
