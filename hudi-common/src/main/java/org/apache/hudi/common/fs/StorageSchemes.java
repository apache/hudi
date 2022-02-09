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

package org.apache.hudi.common.fs;

import java.util.Arrays;

/**
 * All the supported storage schemes in Hoodie.
 */
public enum StorageSchemes {
  // Local filesystem
  FILE("file", false, true),
  // Hadoop File System
  HDFS("hdfs", true, false),
  // Baidu Advanced File System
  AFS("afs", true, false),
  // Mapr File System
  MAPRFS("maprfs", true, false),
  // Apache Ignite FS
  IGNITE("igfs", true, false),
  // AWS S3
  S3A("s3a", false, false), S3("s3", false, false),
  // Google Cloud Storage
  GCS("gs", false, false),
  // Azure WASB
  WASB("wasb", false, false), WASBS("wasbs", false, false),
  // Azure ADLS
  ADL("adl", false, false),
  // Azure ADLS Gen2
  ABFS("abfs", false, false), ABFSS("abfss", false, false),
  // Aliyun OSS
  OSS("oss", false, false),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  VIEWFS("viewfs", true, false),
  //ALLUXIO
  ALLUXIO("alluxio", false, false),
  // Tencent Cloud Object Storage
  COSN("cosn", false, true),
  // Tencent Cloud HDFS
  CHDFS("ofs", true, false),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", false, false),
  // Databricks file system
  DBFS("dbfs", false, false),
  // IBM Cloud Object Storage
  COS("cos", false, false),
  // Huawei Cloud Object Storage
  OBS("obs", false, false),
  // Kingsoft Standard Storage ks3
  KS3("ks3", false, false),
  // JuiceFileSystem
  JFS("jfs", true, false),
  // Baidu Object Storage
  BOS("bos", false, false);

  private String scheme;
  private boolean supportsAppend;
  private boolean supportsRetry;

  StorageSchemes(String scheme, boolean supportsAppend, boolean supportsRetry) {
    this.scheme = scheme;
    this.supportsAppend = supportsAppend;
    this.supportsRetry = supportsRetry;
  }

  public String getScheme() {
    return scheme;
  }

  public boolean supportsAppend() {
    return supportsAppend;
  }

  public boolean supportsRetry() {
    return supportsRetry;
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

  public static boolean isRetrySupported(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Arrays.stream(StorageSchemes.values()).anyMatch(s -> s.supportsRetry() && s.scheme.equals(scheme));
  }
}
