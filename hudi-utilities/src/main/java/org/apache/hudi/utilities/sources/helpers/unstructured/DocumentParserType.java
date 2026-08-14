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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;

import java.util.Locale;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.DOCUMENT_PARSER;
import static org.apache.hudi.utilities.config.UnstructuredFileSourceConfig.PARSER_CLASS;

/**
 * Blessed {@link DocumentParser} implementations selectable by name, with CUSTOM as the
 * escape hatch for user-supplied classes (mirrors the index.type / index.class pattern).
 */
@EnumDescription("Parser used to extract text and metadata from ingested unstructured files.")
public enum DocumentParserType {

  @EnumFieldDescription("Apache Tika AutoDetectParser: in-process extraction for 1000+ formats.")
  TIKA("org.apache.hudi.utilities.sources.helpers.unstructured.TikaDocumentParser"),

  @EnumFieldDescription("Load the DocumentParser implementation named by "
      + "hoodie.streamer.source.unstructured.parser.class.")
  CUSTOM(null);

  private final String parserClassName;

  DocumentParserType(String parserClassName) {
    this.parserClassName = parserClassName;
  }

  /**
   * Resolves the configured parser type to the class name to instantiate.
   */
  public static String resolveParserClass(TypedProperties props) {
    String type = getStringWithAltKeys(props, DOCUMENT_PARSER, true);
    DocumentParserType parserType;
    try {
      parserType = DocumentParserType.valueOf(type.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new HoodieException("Unknown " + DOCUMENT_PARSER.key() + " value: " + type);
    }
    if (parserType != CUSTOM) {
      return parserType.parserClassName;
    }
    String customClass = getStringWithAltKeys(props, PARSER_CLASS, true);
    if (customClass == null || customClass.trim().isEmpty()) {
      throw new HoodieException(DOCUMENT_PARSER.key() + "=CUSTOM requires " + PARSER_CLASS.key());
    }
    return customClass.trim();
  }
}
