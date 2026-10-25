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

package org.apache.hudi.azure.transaction.lock;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAzureStorageLockClientUriParsing {

  @Test
  public void testParseAbfsUri() {
    String uri = "abfs://container@account.dfs.core.windows.net/table/.hoodie/.locks/table_lock.json";

    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.getBlobEndpoint());
    assertEquals("container", l.getContainer());
    assertEquals("table/.hoodie/.locks/table_lock.json", l.getBlobPath());
  }

  @Test
  public void testParseAbfssUri() {
    String uri = "abfss://container@account.dfs.core.windows.net/lake/db/tbl/.hoodie/.locks/table_lock.json";

    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.getBlobEndpoint());
    assertEquals("container", l.getContainer());
    assertEquals("lake/db/tbl/.hoodie/.locks/table_lock.json", l.getBlobPath());
  }

  @Test
  public void testParseWasbUri() {
    String uri = "wasb://container@account.blob.core.windows.net/table/.hoodie/.locks/table_lock.json";

    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.getBlobEndpoint());
    assertEquals("container", l.getContainer());
    assertEquals("table/.hoodie/.locks/table_lock.json", l.getBlobPath());
  }

  @Test
  public void testParseWasbsUri() {
    String uri = "wasbs://container@account.blob.core.windows.net/lake/db/tbl/.hoodie/.locks/table_lock.json";

    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.getBlobEndpoint());
    assertEquals("container", l.getContainer());
    assertEquals("lake/db/tbl/.hoodie/.locks/table_lock.json", l.getBlobPath());
  }

  @Test
  public void testParseHttpsUri() {
    String uri = "https://account.blob.core.windows.net/container/table/.hoodie/.locks/table_lock.json";

    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.getBlobEndpoint());
    assertEquals("container", l.getContainer());
    assertEquals("table/.hoodie/.locks/table_lock.json", l.getBlobPath());
  }

  @Test
  public void testParseHttpUri() {
    String uri = "http://127.0.0.1:10000/container/table/.hoodie/.locks/table_lock.json";

    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("http://127.0.0.1:10000", l.getBlobEndpoint());
    assertEquals("table", l.getContainer());
    assertEquals(".hoodie/.locks/table_lock.json", l.getBlobPath());
  }

  @Test
  public void testParseUriWithSinglePathSegment() {
    String uri = "wasbs://container@account.blob.core.windows.net/file";
    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("container", l.getContainer());
    assertEquals("file", l.getBlobPath());
  }

  @Test
  public void testParseUriWithDeepPath() {
    String uri = "abfss://container@account.dfs.core.windows.net/a/b/c/d/e/f/g.txt";
    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("container", l.getContainer());
    assertEquals("a/b/c/d/e/f/g.txt", l.getBlobPath());
  }

  @Test
  public void testParseUriMissingScheme() {
    assertThrows(Exception.class,
        () -> AzureStorageLockClient.parseAzureLocation("//container@account.blob.core.windows.net/path"));
  }

  @Test
  public void testParseUriMissingAuthority() {
    Exception ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("wasbs:///path/to/file"));
    assertTrue(ex.getMessage().contains("WASB"));
  }

  @Test
  public void testParseUriMissingPath() {
    assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("wasbs://container@account.blob.core.windows.net/"));
  }

  @Test
  public void testParseUriInvalidWasbFormat() {
    // Missing @ separator
    Exception ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("wasbs://containeraccount.blob.core.windows.net/path"));
    assertTrue(ex.getMessage().contains("WASB"));
  }

  @Test
  public void testParseUriInvalidAbfsFormat() {
    // Missing @ separator
    Exception ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("abfss://containeraccount.dfs.core.windows.net/path"));
    assertTrue(ex.getMessage().contains("ABFS"));
  }

  @Test
  public void testParseUriUnsupportedScheme() {
    Exception ex = assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("s3://bucket/path"));
    assertTrue(ex.getMessage().contains("Unsupported"));
  }

  @Test
  public void testParseUriWithSpecialCharactersInPath() {
    String uri = "wasbs://container@account.blob.core.windows.net/path/with-dash_underscore.file";
    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("container", l.getContainer());
    assertEquals("path/with-dash_underscore.file", l.getBlobPath());
  }

  @Test
  public void testParseUriContainerNameWithHyphens() {
    String uri = "wasbs://my-container-123@account.blob.core.windows.net/path";
    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("my-container-123", l.getContainer());
  }

  @Test
  public void testParseDfsHostConvertedToBlobHost() {
    String uri = "abfss://container@myaccount.dfs.core.windows.net/path/file";
    AzureStorageLockClient.AzureLocation l = AzureStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://myaccount.blob.core.windows.net", l.getBlobEndpoint());
  }

  @Test
  public void testParseAbfsEmptyContainer() {
    assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("abfs://@account.dfs.core.windows.net/path"));
  }

  @Test
  public void testParseHttpsMissingBlobPath() {
    assertThrows(IllegalArgumentException.class,
        () -> AzureStorageLockClient.parseAzureLocation("https://account.blob.core.windows.net/container"));
  }
}
