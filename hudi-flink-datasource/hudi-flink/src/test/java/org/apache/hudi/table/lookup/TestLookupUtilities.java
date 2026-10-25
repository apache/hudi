/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.lookup;

import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class TestLookupUtilities {

  @Test
  void testHeapLookupCacheStoresDuplicateKeysAndClears() throws Exception {
    HeapLookupCache cache = new HeapLookupCache();
    RowData key = GenericRowData.of(1);
    RowData first = GenericRowData.of("first");
    RowData second = GenericRowData.of("second");

    assertNull(cache.getRows(key));
    cache.addRow(key, first);
    cache.addRow(key, second);
    List<RowData> rows = cache.getRows(key);
    assertEquals(2, rows.size());
    assertEquals(first, rows.get(0));
    assertEquals(second, rows.get(1));

    cache.clear();
    assertNull(cache.getRows(key));
    cache.addRow(key, first);
    cache.close();
    assertNull(cache.getRows(key));
  }

  @Test
  void testLookupRuntimeProviderFactorySelectsSyncAndAsyncProviders() {
    HoodieLookupFunction function = mock(HoodieLookupFunction.class);

    assertInstanceOf(LookupFunctionProvider.class,
        LookupRuntimeProviderFactory.create(function, false, 1));
    assertInstanceOf(AsyncLookupFunctionProvider.class,
        LookupRuntimeProviderFactory.create(function, true, 3));
  }
}
