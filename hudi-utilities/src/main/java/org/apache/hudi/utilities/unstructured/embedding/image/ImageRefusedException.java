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
 * An image the compute flow refuses BEFORE any pixel is decoded: its header declares more pixels than allowed, its reader
 * would need more decoder intermediates than the budget allows, no subsampling fits the raster budget, or this JVM has no
 * decoder for its format. The outcome is permanent for the row:
 * retrying the same bytes under the same settings can only be refused again.
 */
public class ImageRefusedException extends IOException {

  private static final long serialVersionUID = 1L;

  /** Message prefix of a refusal by the pixel guard, the decoder-intermediate budget or the raster budget. */
  public static final String REFUSED_BEFORE_DECODE = "IMAGE-REFUSED before decode: ";
  /** Message prefix of a refusal for a format this JVM cannot decode. */
  public static final String NO_DECODER = "IMAGE-NO-DECODER: ";

  public ImageRefusedException(String message) {
    super(message);
  }
}
