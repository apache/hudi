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

/**
 * The model input made from one image: the resized image, encoded, and how it was made. It lives only while the client
 * builds the request for that one input (image -> resized -> embedding); it is never a table column.
 */
public final class PreparedImage {

  private final byte[] encoded;
  private final String mimeType;
  private final int width;
  private final int height;
  private final int subsampling;

  public PreparedImage(byte[] encoded, String mimeType, int width, int height, int subsampling) {
    this.encoded = encoded;
    this.mimeType = mimeType;
    this.width = width;
    this.height = height;
    this.subsampling = subsampling;
  }

  /** The encoded resized image (owned by this object; not copied). */
  public byte[] getEncoded() {
    return encoded;
  }

  public String getMimeType() {
    return mimeType;
  }

  public int getWidth() {
    return width;
  }

  public int getHeight() {
    return height;
  }

  /** The source subsampling the decode used (1 = every pixel). */
  public int getSubsampling() {
    return subsampling;
  }
}
