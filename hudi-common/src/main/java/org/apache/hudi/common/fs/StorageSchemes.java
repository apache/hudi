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
  FILE("file", false, false, true, true, false),
  // Hadoop File System
  HDFS("hdfs", true, false, true, false, false),
  // Baidu Advanced File System
  AFS("afs", true, null, null, null, false),
  // Mapr File System
  MAPRFS("maprfs", true, null, null, null, false),
  // Apache Ignite FS
  IGNITE("igfs", true, null, null, null, false),
  // AWS S3
  S3A("s3a", false, true, null, true, true),
  S3("s3", false, true, null, true, true),
  // Google Cloud Storage
  GCS("gs", false, true, null, true, true),
  // Azure WASB
  WASB("wasb", false, null, null, null, false), WASBS("wasbs", false, null, null, null, false),
  // Azure ADLS
  ADL("adl", false, null, null, null, true),
  // Azure ADLS Gen2
  ABFS("abfs", false, null, null, null, false), ABFSS("abfss", false, null, null, null, false),
  // Aliyun OSS
  OSS("oss", false, null, null, null, false),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  // View FS support atomic creation
  VIEWFS("viewfs", true, null, true, null, false),
  //ALLUXIO
  ALLUXIO("alluxio", false, null, null, null, false),
  // Tencent Cloud Object Storage
  COSN("cosn", false, null, null, null, false),
  // Tencent Cloud HDFS
  CHDFS("ofs", true, null, null, null, false),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", false, null, null, null, false),
  // Databricks file system
  DBFS("dbfs", false, null, null, null, false),
  // IBM Cloud Object Storage
  COS("cos", false, null, null, null, false),
  // Huawei Cloud Object Storage
  OBS("obs", false, null, null, null, false),
  // Kingsoft Standard Storage ks3
  KS3("ks3", false, null, null, null, false),
  // JuiceFileSystem
  JFS("jfs", true, null, null, null, false),
  // Baidu Object Storage
  BOS("bos", false, null, null, null, false),
  // Oracle Cloud Infrastructure Object Storage
  OCI("oci", false, null, null, null, false),
  // Volcengine Object Storage
  TOS("tos", false, null, null, null, false),
  // Volcengine Cloud HDFS
  CFS("cfs", true, null, null, null, false),
  // Aliyun Apsara File Storage for HDFS
  DFS("dfs", true, false, true, null, false);

  private String scheme;
  private boolean supportsAppend;
  // null for uncertain if write is transactional, please update this for each FS
  private Boolean isWriteTransactional;
  // null for uncertain if dfs support atomic create&delete, please update this for each FS
  private Boolean supportAtomicCreation;
  // list files may bring pressure to storage with centralized meta service like HDFS.
  // when we want to get only part of files under a directory rather than all files, use getStatus may be more friendly than listStatus.
  // here is a trade-off between rpc times and throughput of storage meta service
  private Boolean listStatusFriendly;
  private Boolean supportsConditionalWrites;

  StorageSchemes(String scheme, boolean supportsAppend, Boolean isWriteTransactional, Boolean supportAtomicCreation, Boolean listStatusFriendly, Boolean supportsConditionalWrites) {
    this.scheme = scheme;
    this.supportsAppend = supportsAppend;
    this.isWriteTransactional = isWriteTransactional;
    this.supportAtomicCreation = supportAtomicCreation;
    this.listStatusFriendly = listStatusFriendly;
    this.supportsConditionalWrites = supportsConditionalWrites;
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

  public boolean getListStatusFriendly() {
    return listStatusFriendly != null && listStatusFriendly;
  }

  public boolean supportsConditionalWrites() {
    return supportsConditionalWrites;
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

  public static boolean isListStatusFriendly(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Arrays.stream(StorageSchemes.values()).anyMatch(s -> s.getListStatusFriendly() && s.scheme.equals(scheme));
  }

  public static boolean isConditionalWritesSupported(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Arrays.stream(StorageSchemes.values()).anyMatch(s -> s.supportsConditionalWrites() && s.scheme.equals(scheme));
  }
}
