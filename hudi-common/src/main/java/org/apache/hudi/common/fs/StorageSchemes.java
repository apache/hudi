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
  FILE("file", false, false, true),
  // Hadoop File System
  HDFS("hdfs", true, false, true),
  // Baidu Advanced File System
  AFS("afs", true, null, null),
  // Mapr File System
  MAPRFS("maprfs", true, null, null),
  // Apache Ignite FS
  IGNITE("igfs", true, null, null),
  // AWS S3
  S3A("s3a", false, true, null), S3("s3", false, true, null),
  // Google Cloud Storage
  GCS("gs", false, true, null),
  // Azure WASB
  WASB("wasb", false, null, null), WASBS("wasbs", false, null, null),
  // Azure ADLS
  ADL("adl", false, null, null),
  // Azure ADLS Gen2
  ABFS("abfs", false, null, null), ABFSS("abfss", false, null, null),
  // Aliyun OSS
  OSS("oss", false, null, null),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  // View FS support atomic creation
  VIEWFS("viewfs", true, null, true),
  //ALLUXIO
  ALLUXIO("alluxio", false, null, null),
  // Tencent Cloud Object Storage
  COSN("cosn", false, null, null),
  // Tencent Cloud HDFS
  CHDFS("ofs", true, null, null),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", false, null, null),
  // Databricks file system
  DBFS("dbfs", false, null, null),
  // IBM Cloud Object Storage
  COS("cos", false, null, null),
  // Huawei Cloud Object Storage
  OBS("obs", false, null, null),
  // Kingsoft Standard Storage ks3
  KS3("ks3", false, null, null),
  // JuiceFileSystem
  JFS("jfs", true, null, null),
  // Baidu Object Storage
  BOS("bos", false, null, null),
  // Oracle Cloud Infrastructure Object Storage
  OCI("oci", false, null, null),
  // Volcengine Object Storage
  TOS("tos", false, null, null),
  // Volcengine Cloud HDFS
  CFS("cfs", true, null, null),
  // Aliyun Apsara File Storage for HDFS
  DFS("dfs", true, false, true);

  private String scheme;
  private boolean supportsAppend;
  // null for uncertain if write is transactional, please update this for each FS
  private Boolean isWriteTransactional;
  // null for uncertain if dfs support atomic create&delete, please update this for each FS
  private Boolean supportAtomicCreation;

  StorageSchemes(String scheme, boolean supportsAppend, Boolean isWriteTransactional, Boolean supportAtomicCreation) {
    this.scheme = scheme;
    this.supportsAppend = supportsAppend;
    this.isWriteTransactional = isWriteTransactional;
    this.supportAtomicCreation = supportAtomicCreation;
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

  public boolean isAtomicCreationSupported() {
    return supportAtomicCreation != null && supportAtomicCreation;
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

  public static boolean isAtomicCreationSupported(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Arrays.stream(StorageSchemes.values()).anyMatch(s -> s.isAtomicCreationSupported() && s.scheme.equals(scheme));
  }
}
