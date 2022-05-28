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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.util.Arrays;

/**
 * All the supported storage schemes in Hoodie.
 */
public enum StorageSchemes {
  // Local filesystem
  FILE("file", false),
  // Hadoop File System
  HDFS("hdfs", true),
  // Baidu Advanced File System
  AFS("afs", true),
  // Mapr File System
  MAPRFS("maprfs", true),
  // Apache Ignite FS
  IGNITE("igfs", true),
  // AWS S3
  S3A("s3a", false), S3("s3", false),
  // Google Cloud Storage
  GCS("gs", false),
  // Azure WASB
  WASB("wasb", false), WASBS("wasbs", false),
  // Azure ADLS
  ADL("adl", false),
  // Azure ADLS Gen2
  ABFS("abfs", false), ABFSS("abfss", false),
  // Aliyun OSS
  OSS("oss", false),
  // View FS for federated setups. If federating across cloud stores, then append support is false
  VIEWFS("viewfs", true),
  //ALLUXIO
  ALLUXIO("alluxio", false),
  // Tencent Cloud Object Storage
  COSN("cosn", false),
  // Tencent Cloud HDFS
  CHDFS("ofs", true),
  // Tencent Cloud CacheFileSystem
  GOOSEFS("gfs", false),
  // Databricks file system
  DBFS("dbfs", false),
  // IBM Cloud Object Storage
  COS("cos", false),
  // Huawei Cloud Object Storage
  OBS("obs", false),
  // Kingsoft Standard Storage ks3
  KS3("ks3", false),
  // JuiceFileSystem
  JFS("jfs", true),
  // Baidu Object Storage
  BOS("bos", false);

  private static class UserDefinedScheme {
    public String scheme;
    public boolean supportsAppend;
  }

  private static UserDefinedScheme[] userDefinedSchemes = null;

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

  public static void registerAdditionalSchemes(
      String json, boolean enableOverride) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      UserDefinedScheme[] uds = mapper.readValue(json, UserDefinedScheme[].class);
      if (!enableOverride
          && Arrays.stream(uds).anyMatch(u ->
              Arrays.stream(StorageSchemes.values()).anyMatch(
                  s -> s.getScheme().equals(u.scheme)))) {
        throw new IllegalArgumentException(
            "Override schemes by mistake, natively supported schemes: "
            + mapper.writeValueAsString(values())
            + ", additional schemes: " + json);
      }
      userDefinedSchemes = uds;
    } catch (IOException e) {
      throw new HoodieException(e);
    }
  }

  public static boolean isSchemeSupported(String scheme) {
    return isSchemeSupported(scheme, true) || isSchemeSupported(scheme, false);
  }

  private static boolean isSchemeSupported(
      String scheme, boolean useUserDefinedSchemes) {
    if (useUserDefinedSchemes) {
      return userDefinedSchemes != null
          && Arrays.stream(userDefinedSchemes).anyMatch(
              s -> s.scheme.equals(scheme));
    }
    return Arrays.stream(values()).anyMatch(s -> s.getScheme().equals(scheme));
  }

  public static boolean isAppendSupported(String scheme) {
    if (isSchemeSupported(scheme, true)) {
      return isAppendSupported(scheme, true);
    }
    if (isSchemeSupported(scheme, false)) {
      return isAppendSupported(scheme, false);
    }
    throw new IllegalArgumentException("Unsupported scheme :" + scheme);
  }

  private static boolean isAppendSupported(
      String scheme, boolean useUserDefinedSchemes) {
    if (useUserDefinedSchemes) {
      return userDefinedSchemes != null
          && Arrays.stream(userDefinedSchemes).anyMatch(
              s -> s.supportsAppend && s.scheme.equals(scheme));
    }
    return Arrays.stream(StorageSchemes.values()).anyMatch(
        s -> s.supportsAppend() && s.scheme.equals(scheme));
  }
}
