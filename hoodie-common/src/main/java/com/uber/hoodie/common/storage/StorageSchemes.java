/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.storage;

import java.util.Arrays;

/**
 * All the supported storage schemes in Hoodie.
 */
public enum StorageSchemes {
  // Local filesystem
  FILE("file", false),
  // Hadoop File System
  HDFS("hdfs", true),
  // Apache Ignite FS
  IGNITE("igfs", true),
  // AWS S3
  S3A("s3a", false),
  S3("s3", false),
  // Google Cloud Storage
  GCS("gs", false),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  VIEWFS("viewfs", true);

  private String scheme;
  private boolean supportsAppend;

  StorageSchemes(String scheme, boolean supportsAppend) {
    this.scheme = scheme;
    this.supportsAppend = supportsAppend;
  }

  public String getScheme() {
    return scheme;
  }

  public boolean supportsAppend() {
    return supportsAppend;
  }

  public static boolean isSchemeSupported(String scheme) {
    return Arrays.stream(values()).anyMatch(s -> s.getScheme().equals(scheme));
  }

  public static boolean isAppendSupported(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Arrays.stream(StorageSchemes.values()).anyMatch(s -> s.supportsAppend() && s.scheme.equals(scheme));
  }
}
