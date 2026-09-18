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

import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Default {@link DocumentParser} backed by Apache Tika's {@link AutoDetectParser}:
 * in-process text and metadata extraction for 1000+ formats (PDF via PDFBox, Office
 * via POI, HTML, plain text, image/video container metadata). Which formats parse
 * depends on the Tika parser modules present on the classpath; with only tika-core
 * available, non-plain-text formats yield EMPTY results rather than errors.
 */
public class TikaDocumentParser implements DocumentParser {

  private static final long serialVersionUID = 1L;

  private transient AutoDetectParser parser;

  @Override
  public ParseResult parse(InputStream in, String fileName, int maxTextChars) {
    try {
      if (parser == null) {
        parser = new AutoDetectParser();
      }
      BodyContentHandler handler = new BodyContentHandler(maxTextChars);
      Metadata tikaMetadata = new Metadata();
      tikaMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, fileName);
      boolean truncated = false;
      try {
        parser.parse(in, handler, tikaMetadata, new ParseContext());
      } catch (WriteLimitReachedException e) {
        // Hitting the char cap is a successful-but-truncated parse, not a failure.
        truncated = true;
      }
      return ParseResult.success(handler.toString().trim(), toMap(tikaMetadata), truncated);
    } catch (Exception e) {
      // A corrupt file must not fail the ingestion job. Error is deliberately not caught:
      // swallowing OutOfMemoryError would carry on with an undefined JVM state instead of
      // failing the task and letting Spark retry it elsewhere.
      return ParseResult.failed(e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()));
    }
  }

  private static Map<String, String> toMap(Metadata metadata) {
    Map<String, String> map = new HashMap<>();
    for (String name : metadata.names()) {
      map.put(name, metadata.get(name));
    }
    return map;
  }
}
