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

import org.apache.hudi.common.config.TypedProperties;

import java.io.InputStream;
import java.io.Serializable;

/**
 * Extracts text and metadata from a single file's content stream. Implementations run
 * inside Spark executors, must be embedded JVM libraries (no external service calls),
 * and must NEVER throw from {@link #parse}: any failure is reported via
 * {@link ParseResult#failed}.
 */
public interface DocumentParser extends Serializable {

  /**
   * Called once per executor instance before the first {@link #parse}.
   */
  default void init(TypedProperties props) {
  }

  /**
   * Parses one file.
   *
   * @param in           content stream; the caller owns closing it
   * @param fileName     name of the file (used for format detection hints)
   * @param maxTextChars cap on extracted text length; hitting it yields a TRUNCATED result
   */
  ParseResult parse(InputStream in, String fileName, int maxTextChars);
}
