/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.storage;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

/**
 * All the supported storage schemes in Hoodie.
 */
public enum StorageSchemes {
  // Local filesystem
  FILE("file", false, true, false),
  // Hadoop File System
  HDFS("hdfs", false, true, false),
  // Baidu Advanced File System
  AFS("afs", null, null, false),
  // Mapr File System
  MAPRFS("maprfs", null, null, false),
  // Apache Ignite FS
  IGNITE("igfs", null, null, false),
  // AWS S3
  S3A("s3a", true, null, true),
  S3("s3", true, null, true),
  // Google Cloud Storage
  GCS("gs", true, null, true),
  // Azure WASB
  WASB("wasb", null, null, false),
  WASBS("wasbs", null, null, false),
  // Azure ADLS
  ADL("adl", null, null, false),
  // Azure ADLS Gen2
  ABFS("abfs", null, null, false),
  ABFSS("abfss", null, null, false),
  // Aliyun OSS
  OSS("oss", null, null, false),
  // View FS for federated setups. If federating across cloud stores, then append
  // support is false
  // View FS support atomic creation
  VIEWFS("viewfs", null, true, false),
  // ALLUXIO
  ALLUXIO("alluxio", null, null, false),
  // Tencent Cloud Object Storage
  COSN("cosn", null, null, false),
  // Tencent Cloud HDFS
  CHDFS("ofs", null, null, false),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", null, null, false),
  // Databricks file system
  DBFS("dbfs", null, null, false),
  // IBM Cloud Object Storage
  COS("cos", null, null, false),
  // Huawei Cloud Object Storage
  OBS("obs", null, null, false),
  // Kingsoft Standard Storage ks3
  KS3("ks3", null, null, false),
  // Netease Object Storage nos
  NOS("nos", null, null, false),
  // JuiceFileSystem
  JFS("jfs", null, null, false),
  // Baidu Object Storage
  BOS("bos", null, null, false),
  // Oracle Cloud Infrastructure Object Storage
  OCI("oci", null, null, false),
  // Volcengine Object Storage
  TOS("tos", null, null, false),
  // Volcengine Cloud HDFS
  CFS("cfs", null, null, false),
  // Aliyun Apsara File Storage for HDFS
  DFS("dfs", false, true, false),
  // Hopsworks File System
  HOPSFS("hopsfs", false, true, false);

  // list files may bring pressure to storage with centralized meta service like HDFS.
  // when we want to get only part of files under a directory rather than all files, use getStatus may be more friendly than listStatus.
  // here is a trade-off between rpc times and throughput of storage meta service
  private static final Set<String> LIST_STATUS_FRIENDLY_SCHEMES = new HashSet<>(Arrays.asList(FILE.scheme, S3.scheme, S3A.scheme, GCS.scheme));

  private final String scheme;
  // null for uncertain if write is transactional, please update this for each FS
  private final Boolean isWriteTransactional;
  // null for uncertain if dfs support atomic create&delete, please update this for each FS
  private final Boolean supportAtomicCreation;
  private final Boolean supportsConditionalWrite;

  StorageSchemes(
      String scheme,
      Boolean isWriteTransactional,
      Boolean supportAtomicCreation,
      Boolean supportsConditionalWrite) {
    this.scheme = scheme;
    this.isWriteTransactional = isWriteTransactional;
    this.supportAtomicCreation = supportAtomicCreation;
    this.supportsConditionalWrite = supportsConditionalWrite;
  }

  public String getScheme() {
    return scheme;
  }

  public boolean isWriteTransactional() {
    return isWriteTransactional != null && isWriteTransactional;
  }

  public boolean isAtomicCreationSupported() {
    return supportAtomicCreation != null && supportAtomicCreation;
  }

  public boolean isConditionalWriteSupported() {
    return supportsConditionalWrite != null && supportsConditionalWrite;
  }

  public static boolean isSchemeSupported(String scheme) {
    return Arrays.stream(values()).anyMatch(s -> s.getScheme().equals(scheme));
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

    return LIST_STATUS_FRIENDLY_SCHEMES.contains(scheme);
  }

  public static boolean isConditionalWriteSupported(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Arrays.stream(StorageSchemes.values())
        .anyMatch(s -> s.isConditionalWriteSupported() && s.scheme.equals(scheme));
  }
}