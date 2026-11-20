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

import java.io.Writer;

/** Collects all the extracted text of one PDF page, with no size bound, for the streaming chunk packer. */
final class BoundedPageWriter extends Writer {
  private final StringBuilder text = new StringBuilder();

  @Override
  public void write(char[] buffer, int offset, int length) {
    text.append(buffer, offset, length);
  }

  @Override
  public void write(String value, int offset, int length) {
    text.append(value, offset, offset + length);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  String text() {
    return text.toString();
  }
}
