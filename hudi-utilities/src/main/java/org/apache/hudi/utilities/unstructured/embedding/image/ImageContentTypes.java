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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The content types of the five image formats an embedding row stores as its input ({@code prepared_image}): png, jpeg, gif,
 * bmp and tiff. WebP, whose header the prep image reader understands, is not one: the JDK cannot decode it.
 */
public final class ImageContentTypes {

  public static final String JPEG = "image/jpeg";
  public static final String PNG = "image/png";
  public static final String GIF = "image/gif";
  public static final String BMP = "image/bmp";
  public static final String TIFF = "image/tiff";
  private static final Set<String> STORED = Collections.unmodifiableSet(
      new LinkedHashSet<>(Arrays.asList(JPEG, PNG, GIF, BMP, TIFF)));

  private ImageContentTypes() {
  }

  /** The five stored image content types, in a fixed order. */
  public static Set<String> stored() {
    return STORED;
  }
}
