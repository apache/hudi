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

public class TestADLSStorageLockClientUriParsing {

  @Test
  public void testParseAbfsUri() {
    String uri = "abfs://container@account.dfs.core.windows.net/table/.hoodie/.locks/table_lock.json";

    ADLSStorageLockClient.AzureLocation l = ADLSStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.blobEndpoint);
    assertEquals("container", l.container);
    assertEquals("table/.hoodie/.locks/table_lock.json", l.blobPath);
  }

  @Test
  public void testParseAbfssUri() {
    String uri = "abfss://container@account.dfs.core.windows.net/lake/db/tbl/.hoodie/.locks/table_lock.json";

    ADLSStorageLockClient.AzureLocation l = ADLSStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.blobEndpoint);
    assertEquals("container", l.container);
    assertEquals("lake/db/tbl/.hoodie/.locks/table_lock.json", l.blobPath);
  }

  @Test
  public void testParseWasbUri() {
    String uri = "wasb://container@account.blob.core.windows.net/table/.hoodie/.locks/table_lock.json";

    ADLSStorageLockClient.AzureLocation l = ADLSStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.blobEndpoint);
    assertEquals("container", l.container);
    assertEquals("table/.hoodie/.locks/table_lock.json", l.blobPath);
  }

  @Test
  public void testParseWasbsUri() {
    String uri = "wasbs://container@account.blob.core.windows.net/lake/db/tbl/.hoodie/.locks/table_lock.json";

    ADLSStorageLockClient.AzureLocation l = ADLSStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.blobEndpoint);
    assertEquals("container", l.container);
    assertEquals("lake/db/tbl/.hoodie/.locks/table_lock.json", l.blobPath);
  }

  @Test
  public void testParseHttpsUri() {
    String uri = "https://account.blob.core.windows.net/container/table/.hoodie/.locks/table_lock.json";

    ADLSStorageLockClient.AzureLocation l = ADLSStorageLockClient.parseAzureLocation(uri);

    assertEquals("https://account.blob.core.windows.net", l.blobEndpoint);
    assertEquals("container", l.container);
    assertEquals("table/.hoodie/.locks/table_lock.json", l.blobPath);
  }
}
