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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.exception.HoodieException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Path-derived blob record identity, file extension and content type. */
public final class BlobFileNames {

  /** Hex characters of the sha-256 kept as source_path_sha: 128 bits. */
  private static final int SOURCE_PATH_SHA_HEX_LENGTH = 32;
  private static final char[] HEX = "0123456789abcdef".toCharArray();
  /**
   * The extension-to-MIME map behind every blob row's {@code content_type}, frozen for blob layout version 1
   * ({@link org.apache.hudi.utilities.sources.unstructured.UnstructuredTableVersion#CURRENT_BLOB_LAYOUT_VERSION}): rows already
   * written keep the type it gave them, so a change to it (a new extension or another type) needs a new blob layout version.
   */
  static final Map<String, String> CONTENT_TYPES = contentTypesOfLayoutVersion1();

  private BlobFileNames() {
  }

  private static Map<String, String> contentTypesOfLayoutVersion1() {
    Map<String, String> types = new HashMap<>();
    types.put("pdf", "application/pdf");
    types.put("png", "image/png");
    types.put("jpg", "image/jpeg");
    types.put("jpeg", "image/jpeg");
    types.put("gif", "image/gif");
    types.put("bmp", "image/bmp");
    types.put("tif", "image/tiff");
    types.put("tiff", "image/tiff");
    types.put("webp", "image/webp");
    return Collections.unmodifiableMap(types);
  }

  /**
   * Key encoding version 1: first 32 lowercase SHA-256 hex characters of UTF-8 {@code bucket/key} for cloud objects.
   * S3, S3A, S3N and GS prefixes are removed without decoding or normalizing the bucket/key text. Other paths are hashed
   * as supplied. Blob and embedding rows share this encoding; the source path used to open bytes is unchanged.
   */
  public static String sourcePathSha(String sourcePath) {
    byte[] digest = sha256().digest(keyEncodingInput(sourcePath).getBytes(StandardCharsets.UTF_8));
    return toHex(digest).substring(0, SOURCE_PATH_SHA_HEX_LENGTH);
  }

  private static String keyEncodingInput(String sourcePath) {
    int separator = sourcePath.indexOf("://");
    if (separator < 0) {
      return sourcePath;
    }
    switch (sourcePath.substring(0, separator).toLowerCase(Locale.ROOT)) {
      case "s3":
      case "s3a":
      case "s3n":
      case "gs":
        return sourcePath.substring(separator + 3);
      default:
        return sourcePath;
    }
  }

  /** Last path segment of a path or URI string. */
  public static String fileName(String path) {
    int slash = path.lastIndexOf('/');
    return slash < 0 ? path : path.substring(slash + 1);
  }

  /** Lower-case extension of a file name without the dot, or the empty string when it has none. */
  public static String extension(String fileName) {
    int dot = fileName.lastIndexOf('.');
    return dot < 0 || dot == fileName.length() - 1 ? "" : fileName.substring(dot + 1).toLowerCase(Locale.ROOT);
  }

  /**
   * MIME type of a lower-case extension: {@code application/pdf} for {@code pdf}, the image type for png, jpg, jpeg, gif,
   * bmp, tif, tiff and webp, null for any other extension, so the {@code content_type} of a video row is null.
   */
  public static String contentType(String extension) {
    return CONTENT_TYPES.get(extension);
  }

  static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new HoodieException("SHA-256 is not available", e);
    }
  }

  static String toHex(byte[] bytes) {
    char[] out = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      out[2 * i] = HEX[(bytes[i] >> 4) & 0xF];
      out[2 * i + 1] = HEX[bytes[i] & 0xF];
    }
    return String.valueOf(out);
  }
}
