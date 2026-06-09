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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieLookupFunction}.
 */
class TestHoodieLookupFunction {

  @TempDir
  File tempFile;

  @Test
  void testNextLoadTimeAdvancesWhenNoCompletedCommit() throws Exception {
    Configuration conf = getConf();
    StreamerUtil.initTableIfNotExists(conf);

    CountingLookupTableReader reader = new CountingLookupTableReader(Collections.emptyList(), conf);
    HoodieLookupFunction function = newLookupFunction(reader, conf);
    function.open(null);

    long beforeLoad = System.currentTimeMillis();
    try {
      function.lookup(lookupKey());

      assertEquals(0, reader.openCount, "The reader should not open when no completed commit exists");
      assertTrue(getNextLoadTime(function) >= beforeLoad + Duration.ofDays(1).toMillis(),
          "The next lookup reload check should be delayed by the configured TTL");
    } finally {
      function.close();
    }
  }

  @Test
  void testLookupCacheDoesNotReloadWhenCompletedCommitHasNotChanged() throws Exception {
    Configuration conf = getConf();
    TestData.writeData(TestData.DATA_SET_SINGLE_INSERT, conf);

    CountingLookupTableReader reader = new CountingLookupTableReader(TestData.DATA_SET_SINGLE_INSERT, conf);
    HoodieLookupFunction function = newLookupFunction(reader, conf);
    function.open(null);

    try {
      Collection<RowData> matchedRows = function.lookup(lookupKey());
      assertNotNull(matchedRows, "The first lookup should find the inserted row");
      assertEquals(1, matchedRows.size(), "The first lookup should load the table into cache");
      assertEquals(1, reader.openCount, "The first lookup should open the reader once");

      // Force the next lookup through the reload branch so the unchanged-commit guard is exercised.
      setNextLoadTime(function, 0L);
      function.lookup(lookupKey());

      assertEquals(1, reader.openCount, "The same completed commit should not reload table data");
      assertTrue(getNextLoadTime(function) > System.currentTimeMillis(),
          "The next lookup reload check should be delayed after detecting an unchanged commit");
    } finally {
      function.close();
    }
  }

  private HoodieLookupFunction newLookupFunction(CountingLookupTableReader reader, Configuration conf) {
    return new HoodieLookupFunction(
        reader,
        TestConfigurations.ROW_TYPE,
        new int[] {0},
        Duration.ofDays(1),
        conf);
  }

  private Configuration getConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.LOOKUP_JOIN_CACHE_TTL, Duration.ofDays(1));
    return conf;
  }

  private static RowData lookupKey() {
    return GenericRowData.of(StringData.fromString("id1"));
  }

  private static long getNextLoadTime(HoodieLookupFunction function) throws Exception {
    Field field = HoodieLookupFunction.class.getDeclaredField("nextLoadTime");
    field.setAccessible(true);
    return field.getLong(function);
  }

  private static void setNextLoadTime(HoodieLookupFunction function, long nextLoadTime) throws Exception {
    Field field = HoodieLookupFunction.class.getDeclaredField("nextLoadTime");
    field.setAccessible(true);
    field.setLong(function, nextLoadTime);
  }

  private static class CountingLookupTableReader extends HoodieLookupTableReader {
    private final List<RowData> rows;
    private int openCount;
    private int nextIndex;

    private CountingLookupTableReader(List<RowData> rows, Configuration conf) {
      super(() -> null, conf);
      this.rows = rows;
    }

    @Override
    public void open() {
      openCount++;
      nextIndex = 0;
    }

    @Override
    public RowData read(RowData reuse) {
      if (nextIndex >= rows.size()) {
        return null;
      }
      return rows.get(nextIndex++);
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
