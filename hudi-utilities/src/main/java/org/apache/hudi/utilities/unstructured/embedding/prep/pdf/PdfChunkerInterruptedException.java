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

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;

/**
 * The Spark task or the thread was interrupted while a PDF was being chunked. Never isolated
 * as a page or file failure: it propagates so the task ends, after the staged file has been deleted.
 */
public class PdfChunkerInterruptedException extends UncheckedIOException {

  public PdfChunkerInterruptedException(String message) {
    super(message, new InterruptedIOException(message));
  }
}
