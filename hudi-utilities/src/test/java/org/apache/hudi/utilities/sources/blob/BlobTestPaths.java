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

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * TEST-ONLY: the canonical path form tests name source objects by. The blob source never applies it: an object's
 * {@code source_path} and source_path_sha are its URL as built.
 */
public final class BlobTestPaths {

  private static final String DEFAULT_SCHEME = "file";
  private static final String S3A_SCHEME = "s3a";
  private static final Set<String> S3_SCHEMES = new HashSet<>(Arrays.asList("s3", "s3n", "s3a"));

  private BlobTestPaths() {
  }

  /**
   * Canonical form of a source file URI: {@code scheme://authority/normalized/absolute/path} with the path DECODED, e.g.
   * {@code file:///a/my scan.pdf} or {@code s3a://bucket/key.pdf} — the form Hadoop's {@code Path#toString} prints and
   * {@code StoragePath(String)} opens. {@code s3} and {@code s3n} are written as {@code s3a}. A URI without a scheme is taken as a
   * local file.
   *
   * @throws IllegalArgumentException for an opaque URI or a path that is not absolute
   */
  public static String canonicalPath(URI uri) {
    if (uri.isOpaque() || uri.getPath() == null) {
      throw new IllegalArgumentException("Source path must be a hierarchical URI: " + uri);
    }
    URI normalized = uri.normalize();
    String path = normalized.getPath().replaceAll("/{2,}", "/");
    if (!path.startsWith("/")) {
      throw new IllegalArgumentException("Source path must be absolute: " + uri);
    }
    String authority = normalized.getAuthority() == null ? "" : normalized.getAuthority();
    return canonicalScheme(normalized.getScheme()) + "://" + authority + path;
  }

  private static String canonicalScheme(String scheme) {
    if (scheme == null) {
      return DEFAULT_SCHEME;
    }
    String lower = scheme.toLowerCase(Locale.ROOT);
    return S3_SCHEMES.contains(lower) ? S3A_SCHEME : lower;
  }
}
