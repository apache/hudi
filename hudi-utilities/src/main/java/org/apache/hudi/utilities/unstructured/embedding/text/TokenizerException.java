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

import org.apache.hudi.exception.HoodieException;

/**
 * A {@link Tokenizer} could not count a text. Unchecked: the preprocessing framework decides what it means for the file, so
 * callers need not catch it.
 *
 * <p>Its {@link Kind} says whether the same text could count on a later try: {@link Kind#PERMANENT} (the default) for a
 * failure of the text itself, isolated to its file (a prep error-table event), {@link Kind#RETRYABLE} for a
 * failure of the tokenizer's runtime (a resource or service it depends on failed, after the tokenizer's own bounded retries), which fails
 * the round ({@code PrepFailures.roundFailure}). A tokenizer's configuration error is a
 * {@code PreprocessorConfigurationException}, never this exception. An exception built with an explicit kind prefixes its message with
 * {@code [PERMANENT] } or {@code [RETRYABLE] }, so the persisted prep error carries the classification.
 */
public class TokenizerException extends HoodieException {

  private static final long serialVersionUID = 1L;

  /** Whether a failed count could succeed on a later try. */
  public enum Kind {
    /** The configuration or the text: the same input fails again (a text it cannot count). */
    PERMANENT,
    /** The tokenizer's runtime: a later try may succeed (a timed-out or temporarily failing dependency). */
    RETRYABLE
  }

  private final Kind kind;

  public TokenizerException(String message) {
    super(message);
    this.kind = Kind.PERMANENT;
  }

  public TokenizerException(String message, Throwable cause) {
    super(message, cause);
    this.kind = Kind.PERMANENT;
  }

  /** A failure of {@code kind}; the message is prefixed with {@code [<kind>] }. */
  public TokenizerException(Kind kind, String message, Throwable cause) {
    super("[" + kind + "] " + message, cause);
    this.kind = kind;
  }

  public Kind kind() {
    return kind;
  }
}
