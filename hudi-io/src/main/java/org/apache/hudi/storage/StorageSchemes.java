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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

/**
 * All the supported storage schemes in Hoodie.
 */
public enum StorageSchemes {
  // Local filesystem
  FILE("file", false, false, true, true, null),
  // Hadoop File System
  HDFS("hdfs", true, false, true, false, null),
  // Baidu Advanced File System
  AFS("afs", true, null, null, null, null),
  // Mapr File System
  MAPRFS("maprfs", true, null, null, null, null),
  // Apache Ignite FS
  IGNITE("igfs", true, null, null, null, null),
  // AWS S3
  S3A("s3a", false, true, null, true, "org.apache.hudi.aws.transaction.lock.S3StorageLockClient"),
  S3("s3", false, true, null, true, "org.apache.hudi.aws.transaction.lock.S3StorageLockClient"),
  // Google Cloud Storage
  GCS("gs", false, true, null, true, "org.apache.hudi.gcp.transaction.lock.GCSStorageLockClient"),
  // Azure WASB
  WASB("wasb", false, null, null, null, null), WASBS("wasbs", false, null, null, null, null),
  // Azure ADLS
  ADL("adl", false, null, null, null, null),
  // Azure ADLS Gen2
  ABFS("abfs", false, null, null, null, null), ABFSS("abfss", false, null, null, null, null),
  // Aliyun OSS
  OSS("oss", false, null, null, null, null),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  // View FS support atomic creation
  VIEWFS("viewfs", true, null, true, null, null),
  //ALLUXIO
  ALLUXIO("alluxio", false, null, null, null, null),
  // Tencent Cloud Object Storage
  COSN("cosn", false, null, null, null, null),
  // Tencent Cloud HDFS
  CHDFS("ofs", true, null, null, null, null),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", false, null, null, null, null),
  // Databricks file system
  DBFS("dbfs", false, null, null, null, null),
  // IBM Cloud Object Storage
  COS("cos", false, null, null, null, null),
  // Huawei Cloud Object Storage
  OBS("obs", false, null, null, null, null),
  // Kingsoft Standard Storage ks3
  KS3("ks3", false, null, null, null, null),
  // JuiceFileSystem
  JFS("jfs", true, null, null, null, null),
  // Baidu Object Storage
  BOS("bos", false, null, null, null, null),
  // Oracle Cloud Infrastructure Object Storage
  OCI("oci", false, null, null, null, null),
  // Volcengine Object Storage
  TOS("tos", false, null, null, null, null),
  // Volcengine Cloud HDFS
  CFS("cfs", true, null, null, null, null),
  // Aliyun Apsara File Storage for HDFS
  DFS("dfs", true, false, true, null, null),
  // Hopsworks File System
  HOPSFS("hopsfs", false, false, true, null, null);

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
  private String storageLockClass;

  StorageSchemes(String scheme, boolean supportsAppend, Boolean isWriteTransactional, Boolean supportAtomicCreation, Boolean listStatusFriendly, String storageLockClass) {
    this.scheme = scheme;
    this.supportsAppend = supportsAppend;
    this.isWriteTransactional = isWriteTransactional;
    this.supportAtomicCreation = supportAtomicCreation;
    this.listStatusFriendly = listStatusFriendly;
    this.storageLockClass = storageLockClass;
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

  public boolean implementsStorageLock() {
    return !StringUtils.isNullOrEmpty(storageLockClass);
  }

  public String getStorageLockClass() {
    return storageLockClass;
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

  public static Option<StorageSchemes> getStorageLockImplementationIfExists(String scheme) {
    if (!isSchemeSupported(scheme)) {
      throw new IllegalArgumentException("Unsupported scheme :" + scheme);
    }
    return Option.fromJavaOptional(Arrays.stream(StorageSchemes.values())
        .filter(s -> s.implementsStorageLock() && s.scheme.equals(scheme)).findFirst());
  }
}
