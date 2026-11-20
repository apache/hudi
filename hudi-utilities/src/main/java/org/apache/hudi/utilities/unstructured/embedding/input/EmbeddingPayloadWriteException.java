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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.exception.HoodieException;

/**
 * A prepared payload that could not be written to its managed file, named by its row (the record key fields: source path
 * hash, version time and size, modality and chunk id) and the step that failed. It is a storage failure of the round, never the
 * row's own content: data prep fails the round, which the streamer rolls back and retries. A payload whose own bytes are wrong
 * (shorter or longer than declared) is not this exception but the {@link IllegalArgumentException} {@link PreparedInputs} throws.
 */
public class EmbeddingPayloadWriteException extends HoodieException {

  private static final long serialVersionUID = 1L;

  private final String row;
  private final String step;

  /** @param row the row the payload belongs to, as {@link PreparedInputs#prepare} names it */
  public EmbeddingPayloadWriteException(String row, String step, String cause, Throwable throwable) {
    super("EMBED-PAYLOAD-WRITE row=" + row + " step=" + step + ": " + cause, throwable);
    this.row = row;
    this.step = step;
  }

  public String getRow() {
    return row;
  }

  public String getStep() {
    return step;
  }
}
