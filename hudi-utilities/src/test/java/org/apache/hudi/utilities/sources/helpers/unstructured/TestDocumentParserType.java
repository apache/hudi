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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.UnstructuredFileSourceConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies how {@link DocumentParserType} resolves the configured parser name to a class name.
 */
class TestDocumentParserType {

  private static final String PARSER_KEY = UnstructuredFileSourceConfig.DOCUMENT_PARSER.key();
  private static final String PARSER_CLASS_KEY = UnstructuredFileSourceConfig.PARSER_CLASS.key();

  @Test
  void tikaResolvesRegardlessOfCaseAndPadding() {
    // Unset falls back to the TIKA default.
    assertEquals(TikaDocumentParser.class.getName(),
        DocumentParserType.resolveParserClass(new TypedProperties()));

    TypedProperties props = new TypedProperties();
    props.setProperty(PARSER_KEY, " tika ");
    assertEquals(TikaDocumentParser.class.getName(), DocumentParserType.resolveParserClass(props));
  }

  @Test
  void unknownParserTypeThrows() {
    TypedProperties props = new TypedProperties();
    props.setProperty(PARSER_KEY, "docling");

    HoodieException e = assertThrows(HoodieException.class, () -> DocumentParserType.resolveParserClass(props));
    assertTrue(e.getMessage().contains("Unknown " + PARSER_KEY), e.getMessage());
    assertTrue(e.getMessage().contains("docling"), e.getMessage());
  }

  @Test
  void customWithoutParserClassThrows() {
    TypedProperties props = new TypedProperties();
    props.setProperty(PARSER_KEY, "CUSTOM");

    HoodieException unset = assertThrows(HoodieException.class, () -> DocumentParserType.resolveParserClass(props));
    assertTrue(unset.getMessage().contains(PARSER_CLASS_KEY), unset.getMessage());

    props.setProperty(PARSER_CLASS_KEY, "   ");
    HoodieException blank = assertThrows(HoodieException.class, () -> DocumentParserType.resolveParserClass(props));
    assertTrue(blank.getMessage().contains(PARSER_CLASS_KEY), blank.getMessage());
  }

  @Test
  void customReturnsTrimmedParserClass() {
    TypedProperties props = new TypedProperties();
    props.setProperty(PARSER_KEY, " custom ");
    props.setProperty(PARSER_CLASS_KEY, "  com.example.MyDocumentParser  ");

    assertEquals("com.example.MyDocumentParser", DocumentParserType.resolveParserClass(props));
  }
}
