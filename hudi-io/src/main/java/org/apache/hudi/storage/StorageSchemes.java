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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

/**
 * All the supported storage schemes in Hoodie.
 */
public enum StorageSchemes {
  // Local filesystem
  FILE("file", false, true, null),
  // Hadoop File System
  HDFS("hdfs", false, true, null),
  // Baidu Advanced File System
  AFS("afs", null, null, null),
  // Mapr File System
  MAPRFS("maprfs", null, null, null),
  // Apache Ignite FS
  IGNITE("igfs", null, null, null),
  // AWS S3
  S3A("s3a", true, null, "org.apache.hudi.aws.transaction.lock.S3StorageLockClient"),
  S3("s3", true, null, "org.apache.hudi.aws.transaction.lock.S3StorageLockClient"),
  // Google Cloud Storage
  GCS("gs", true, null, "org.apache.hudi.gcp.transaction.lock.GCSStorageLockClient"),
  // Azure WASB
  WASB("wasb", null, null, null),
  WASBS("wasbs", null, null, null),
  // Azure ADLS
  ADL("adl", null, null, null),
  // Azure ADLS Gen2
  ABFS("abfs", null, null, null),
  ABFSS("abfss", null, null, null),
  // Aliyun OSS
  OSS("oss", null, null, null),
  // View FS for federated setups. If federating across cloud stores, then append
  // support is false
  // View FS support atomic creation
  VIEWFS("viewfs", null, true, null),
  // ALLUXIO
  ALLUXIO("alluxio", null, null, null),
  // Tencent Cloud Object Storage
  COSN("cosn", null, null, null),
  // Tencent Cloud HDFS
  CHDFS("ofs", null, null, null),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", null, null, null),
  // Databricks file system
  DBFS("dbfs", null, null, null),
  // IBM Cloud Object Storage
  COS("cos", null, null, null),
  // Huawei Cloud Object Storage
  OBS("obs", null, null, null),
  // Kingsoft Standard Storage ks3
  KS3("ks3", null, null, null),
  // Netease Object Storage nos
  NOS("nos", null, null, null),
  // JuiceFileSystem
  JFS("jfs", null, null, null),
  // Baidu Object Storage
  BOS("bos", null, null, null),
  // Oracle Cloud Infrastructure Object Storage
  OCI("oci", null, null, null),
  // Volcengine Object Storage
  TOS("tos", null, null, null),
  // Volcengine Cloud HDFS
  CFS("cfs", null, null, null),
  // Aliyun Apsara File Storage for HDFS
  DFS("dfs", false, true, null),
  // Hopsworks File System
  HOPSFS("hopsfs", false, true, null);

  // list files may bring pressure to storage with centralized meta service like HDFS.
  // when we want to get only part of files under a directory rather than all files, use getStatus may be more friendly than listStatus.
  // here is a trade-off between rpc times and throughput of storage meta service
  private static final Set<String> LIST_STATUS_FRIENDLY_SCHEMES = new HashSet<>(Arrays.asList(FILE.scheme, S3.scheme, S3A.scheme, GCS.scheme));

  private final String scheme;
  // null for uncertain if write is transactional, please update this for each FS
  private final Boolean isWriteTransactional;
  // null for uncertain if dfs support atomic create&delete, please update this for each FS
  private final Boolean supportAtomicCreation;
  private final String storageLockClass;

  StorageSchemes(
      String scheme,
      Boolean isWriteTransactional,
      Boolean supportAtomicCreation,
      String storageLockClass) {
    this.scheme = scheme;
    this.isWriteTransactional = isWriteTransactional;
    this.supportAtomicCreation = supportAtomicCreation;
    this.storageLockClass = storageLockClass;
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

  public boolean implementsStorageLock() {
    return !StringUtils.isNullOrEmpty(storageLockClass);
  }

  public String getStorageLockClass() {
    return storageLockClass;
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

  public static Option<StorageSchemes> getStorageLockImplementationIfExists(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Option.fromJavaOptional(Arrays.stream(StorageSchemes.values())
        .filter(s -> s.implementsStorageLock() && s.scheme.equals(scheme)).findFirst());
  }
}
