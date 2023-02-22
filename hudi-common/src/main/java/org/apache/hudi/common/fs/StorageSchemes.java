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
  FILE("file", false, false),
  // Hadoop File System
  HDFS("hdfs", true, false),
  // Baidu Advanced File System
  AFS("afs", true, null),
  // Mapr File System
  MAPRFS("maprfs", true, null),
  // Apache Ignite FS
  IGNITE("igfs", true, null),
  // AWS S3
  S3A("s3a", false, true), S3("s3", false, true),
  // Google Cloud Storage
  GCS("gs", false, true),
  // Azure WASB
  WASB("wasb", false, null), WASBS("wasbs", false, null),
  // Azure ADLS
  ADL("adl", false, null),
  // Azure ADLS Gen2
  ABFS("abfs", false, null), ABFSS("abfss", false, null),
  // Aliyun OSS
  OSS("oss", false, null),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  VIEWFS("viewfs", true, null),
  //ALLUXIO
  ALLUXIO("alluxio", false, null),
  // Tencent Cloud Object Storage
  COSN("cosn", false, null),
  // Tencent Cloud HDFS
  CHDFS("ofs", true, null),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", false, null),
  // Databricks file system
  DBFS("dbfs", false, null),
  // IBM Cloud Object Storage
  COS("cos", false, null),
  // Huawei Cloud Object Storage
  OBS("obs", false, null),
  // Kingsoft Standard Storage ks3
  KS3("ks3", false, null),
  // JuiceFileSystem
  JFS("jfs", true, null),
  // Baidu Object Storage
  BOS("bos", false, null),
  // Oracle Cloud Infrastructure Object Storage
  OCI("oci", false, null),
  // Volcengine Object Storage
  TOS("tos", false, null),
  // Volcengine Cloud HDFS
  CFS("cfs", true, null);

  private String scheme;
  private boolean supportsAppend;
  // null for uncertain if write is transactional, please update this for each FS
  private Boolean isWriteTransactional;

  StorageSchemes(String scheme, boolean supportsAppend, Boolean isWriteTransactional) {
    this.scheme = scheme;
    this.supportsAppend = supportsAppend;
    this.isWriteTransactional = isWriteTransactional;
  }

  public String getScheme() {
    return scheme;
  }

  public boolean supportsAppend() {
    return supportsAppend;
  }

  public boolean isWriteTransactional() {
    return isWriteTransactional != null && isWriteTransactional;
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

  public static boolean isWriteTransactional(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }

    return Arrays.stream(StorageSchemes.values()).anyMatch(s -> s.isWriteTransactional() && s.scheme.equals(scheme));
  }
}
