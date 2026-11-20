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

import org.apache.hudi.exception.HoodieException;

/**
 * The configured {@link ImageDecoder} runs another decoding logic than this code's ({@link ImageResizeLogicV1}), so the images
 * it prepares would not be the ones the compute settings describe. Refused before any work.
 */
public class ImageDecoderLogicMismatchException extends HoodieException {

  public ImageDecoderLogicMismatchException(String message) {
    super(message);
  }
}
