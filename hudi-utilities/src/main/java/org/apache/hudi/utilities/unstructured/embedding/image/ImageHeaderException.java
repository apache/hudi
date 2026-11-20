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

package org.apache.hudi.utilities.unstructured.embedding.image;

import java.io.IOException;

/**
 * An image header that cannot be read: no reader recognises the content, the header is truncated or corrupt, or it declares
 * a non-positive dimension. Data prep turns it into a quarantine entry; compute into a PERMANENT outcome.
 */
public class ImageHeaderException extends IOException {

  private static final long serialVersionUID = 1L;

  public ImageHeaderException(String message) {
    super(message);
  }

  public ImageHeaderException(String message, Throwable cause) {
    super(message, cause);
  }
}
