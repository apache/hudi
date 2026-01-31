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

import org.apache.hudi.azure.utils.AzureStorageUtils.AzureStorageUriComponents;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link AzureStorageUtils}.
 */
public class TestAzureStorageUtils {

  @Test
  public void testParseWasbUri() {
    String uri = "wasb://container@account.blob.core.windows.net/path/to/file";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("container", components.containerName);
    assertEquals("account", components.accountName);
    assertEquals("path/to/file", components.blobPath);
  }

  @Test
  public void testParseWasbsUri() {
    String uri = "wasbs://mycontainer@mystorageaccount.blob.core.windows.net/data/lake/table";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("mycontainer", components.containerName);
    assertEquals("mystorageaccount", components.accountName);
    assertEquals("data/lake/table", components.blobPath);
  }

  @Test
  public void testParseAbfsUri() {
    String uri = "abfs://container@account.dfs.core.windows.net/path/to/file";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("container", components.containerName);
    assertEquals("account", components.accountName);
    assertEquals("path/to/file", components.blobPath);
  }

  @Test
  public void testParseAbfssUri() {
    String uri = "abfss://filesystem@datalake.dfs.core.windows.net/folder/subfolder/file.parquet";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("filesystem", components.containerName);
    assertEquals("datalake", components.accountName);
    assertEquals("folder/subfolder/file.parquet", components.blobPath);
  }

  @Test
  public void testParseUriWithSinglePathSegment() {
    String uri = "wasbs://container@account.blob.core.windows.net/file";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("container", components.containerName);
    assertEquals("account", components.accountName);
    assertEquals("file", components.blobPath);
  }

  @Test
  public void testParseUriWithDeepPath() {
    String uri = "abfss://container@account.dfs.core.windows.net/a/b/c/d/e/f/g.txt";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("container", components.containerName);
    assertEquals("account", components.accountName);
    assertEquals("a/b/c/d/e/f/g.txt", components.blobPath);
  }

  @Test
  public void testParseUriMissingScheme() {
    String uri = "//container@account.blob.core.windows.net/path";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("scheme"));
  }

  @Test
  public void testParseUriMissingAuthority() {
    String uri = "wasbs:///path/to/file";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("authority"));
  }

  @Test
  public void testParseUriMissingPath() {
    String uri = "wasbs://container@account.blob.core.windows.net/";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("path"));
  }

  @Test
  public void testParseUriMissingPathNoSlash() {
    String uri = "wasbs://container@account.blob.core.windows.net";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("path"));
  }

  @Test
  public void testParseUriInvalidWasbFormat() {
    // Missing @ separator
    String uri = "wasbs://containeraccount.blob.core.windows.net/path";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("WASB"));
  }

  @Test
  public void testParseUriInvalidAbfsFormat() {
    // Missing @ separator
    String uri = "abfss://containeraccount.dfs.core.windows.net/path";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("ABFS"));
  }

  @Test
  public void testParseUriUnsupportedScheme() {
    String uri = "s3://bucket/path";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("Unsupported Azure URI scheme"));
  }

  @Test
  public void testParseUriInvalidUriSyntax() {
    String uri = "wasbs://container@account^invalid.blob.core.windows.net/path";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("Failed to parse Azure URI"));
  }

  @Test
  public void testParseUriMultipleAtSymbols() {
    String uri = "wasbs://container@account@extra.blob.core.windows.net/path";
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageUtils.parseAzureUri(uri));
    assertTrue(ex.getMessage().contains("WASB"));
  }

  @Test
  public void testParseUriWithSpecialCharactersInPath() {
    String uri = "wasbs://container@account.blob.core.windows.net/path/with-dash_underscore.file";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertNotNull(components);
    assertEquals("container", components.containerName);
    assertEquals("account", components.accountName);
    assertEquals("path/with-dash_underscore.file", components.blobPath);
  }

  @Test
  public void testParseUriAccountNameExtraction() {
    // Test that we correctly extract just the account name, not the full domain
    String uri = "wasbs://container@myaccount.blob.core.windows.net/path";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertEquals("myaccount", components.accountName);
  }

  @Test
  public void testParseUriContainerNameWithSpecialCharacters() {
    // Container names can contain hyphens
    String uri = "wasbs://my-container-123@account.blob.core.windows.net/path";
    AzureStorageUriComponents components = AzureStorageUtils.parseAzureUri(uri);

    assertEquals("my-container-123", components.containerName);
    assertEquals("account", components.accountName);
    assertEquals("path", components.blobPath);
  }
}
