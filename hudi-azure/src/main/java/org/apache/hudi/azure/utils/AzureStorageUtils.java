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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.azure.utils;

import org.apache.hudi.common.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class for Azure Blob Storage operations.
 */
public class AzureStorageUtils {

  /**
   * Parses Azure storage URI and extracts components.
   * Supports WASB(S): wasbs://container@account.blob.core.windows.net/path
   * Supports ABFS(S): abfss://container@account.dfs.core.windows.net/path
   *
   * @param uriString The Azure storage URI to parse
   * @return Parsed URI components containing container name, account name, and blob path
   * @throws IllegalArgumentException if the URI format is invalid
   */
  public static AzureStorageUriComponents parseAzureUri(String uriString) {
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();

      if (scheme == null) {
        throw new IllegalArgumentException("URI does not contain a scheme: " + uriString);
      }

      String authority = uri.getAuthority();
      if (StringUtils.isNullOrEmpty(authority)) {
        throw new IllegalArgumentException("URI does not contain authority: " + uriString);
      }

      String containerName;
      String accountName;

      // Parse based on scheme
      if (scheme.startsWith("wasb")) {
        // WASB format: wasbs://container@account.blob.core.windows.net/path
        String[] parts = authority.split("@");
        if (parts.length != 2) {
          throw new IllegalArgumentException("Invalid WASB URI format: " + uriString);
        }
        containerName = parts[0];
        accountName = parts[1].split("\\.")[0];
      } else if (scheme.startsWith("abfs")) {
        // ABFS format: abfss://container@account.dfs.core.windows.net/path
        String[] parts = authority.split("@");
        if (parts.length != 2) {
          throw new IllegalArgumentException("Invalid ABFS URI format: " + uriString);
        }
        containerName = parts[0];
        accountName = parts[1].split("\\.")[0];
      } else {
        throw new IllegalArgumentException("Unsupported Azure URI scheme: " + scheme);
      }

      String path = uri.getPath();
      if (StringUtils.isNullOrEmpty(path) || path.equals("/")) {
        throw new IllegalArgumentException("URI does not contain a valid path: " + uriString);
      }

      // Remove leading slash
      String blobPath = path.startsWith("/") ? path.substring(1) : path;

      return new AzureStorageUriComponents(containerName, accountName, blobPath);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to parse Azure URI: " + uriString, e);
    }
  }

  /**
   * Helper class to hold parsed Azure URI components.
   */
  public static class AzureStorageUriComponents {
    public final String containerName;
    public final String accountName;
    public final String blobPath;

    public AzureStorageUriComponents(String containerName, String accountName, String blobPath) {
      this.containerName = containerName;
      this.accountName = accountName;
      this.blobPath = blobPath;
    }
  }
}
