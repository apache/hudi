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

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestADLSGen2StorageLockClientUriParsing {

  @Test
  public void testParseAbfsUri() throws Exception {
    String uri = "abfs://container@account.dfs.core.windows.net/table/.hoodie/.locks/table_lock.json";

    Method m = ADLSGen2StorageLockClient.class.getDeclaredMethod("parseAzureLocation", String.class);
    m.setAccessible(true);
    Object location = m.invoke(null, uri);

    ADLSGen2StorageLockClient.AzureLocation l = (ADLSGen2StorageLockClient.AzureLocation) location;

    assertEquals("https://account.blob.core.windows.net", l.blobEndpoint);
    assertEquals("container", l.container);
    assertEquals("table/.hoodie/.locks/table_lock.json", l.blobPath);
  }
}

