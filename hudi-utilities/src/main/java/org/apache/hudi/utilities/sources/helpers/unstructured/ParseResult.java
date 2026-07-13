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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Outcome of parsing one file with a {@link DocumentParser}.
 */
public class ParseResult implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Row-level parse outcome. Parsing never fails the ingestion job; failures are
   * recorded on the row so consumers can filter or reprocess.
   */
  public enum ParseStatus {
    /** Text extracted in full. */
    SUCCESS,
    /** Text extracted but capped at the configured character limit. */
    TRUNCATED,
    /** Parsed without error but no text content (e.g. images, videos, unknown formats). */
    EMPTY,
    /** File exceeded the parse size cap or its extension/format is not parseable; blob still ingested. */
    SKIPPED,
    /** Parser threw; error message recorded, blob still ingested. */
    FAILED;
  }

  private final ParseStatus status;
  private final String text;
  private final Map<String, String> metadata;
  private final String error;

  private ParseResult(ParseStatus status, String text, Map<String, String> metadata, String error) {
    this.status = status;
    this.text = text;
    this.metadata = metadata == null ? Collections.emptyMap() : metadata;
    this.error = error;
  }

  public static ParseResult success(String text, Map<String, String> metadata, boolean truncated) {
    if (text == null || text.trim().isEmpty()) {
      return new ParseResult(ParseStatus.EMPTY, "", metadata, null);
    }
    return new ParseResult(truncated ? ParseStatus.TRUNCATED : ParseStatus.SUCCESS, text, metadata, null);
  }

  public static ParseResult skipped(String reason) {
    return new ParseResult(ParseStatus.SKIPPED, "", Collections.emptyMap(), reason);
  }

  public static ParseResult failed(String error) {
    return new ParseResult(ParseStatus.FAILED, "", Collections.emptyMap(), error);
  }

  public ParseStatus getStatus() {
    return status;
  }

  public String getText() {
    return text;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public String getError() {
    return error;
  }
}
