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

package org.apache.hudi.utilities.unstructured.embedding.source;

/**
 * A configuration error met while a file is prepared (e.g. a transcriber whose model is missing): never the file's failure, which
 * would advance the round past every file the same error hits. The framework lets it through (also from a cause or suppressed
 * chain, {@link org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures#roundFailure}), so the task fails, nothing of the round is committed and the next run resumes from the
 * same checkpoint, like a {@link org.apache.hudi.utilities.unstructured.embedding.prep.SourceReadException}.
 */
public class PreprocessorConfigurationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public PreprocessorConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}
