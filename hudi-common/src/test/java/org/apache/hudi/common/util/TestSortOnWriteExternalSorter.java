/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class TestSortOnWriteExternalSorter {

  private static final int DATA_SIZE = 1024; // 1KB
  private static final byte[] DATA = new byte[DATA_SIZE];

  private static class Record implements Comparable<Record>, Serializable {
    private final int key;
    private final byte[] data;

    public Record(int key) {
      this.key = key;
      this.data = new byte[DATA_SIZE];
    }

    public int getKey() {
      return key;
    }

    @Override
    public int compareTo(TestSortOnWriteExternalSorter.Record o) {
      return Integer.compare(key, o.key);
    }
  }

  @TempDir
  private File tmpDir;

  private static int totalNum;
  private static int totalSize;
  private static SizeEstimator<Record> sizeEstimator = record -> 4 + 4 + 4 + DATA_SIZE;
  private static List<Record> records;
  private static List<Record> sortedRecords;

  @BeforeAll
  public static void setUp() {
    // total 100MB data
    totalNum = 1 << 19;
    totalSize = totalNum * (4 + 4 + 4 + DATA_SIZE);
    // generate random records
    Random random = new Random();
    records = random.ints(totalNum, 0, totalNum / 20).mapToObj(key -> new Record(key)).collect(Collectors.toList());
    sortedRecords = records.stream().sorted().collect(Collectors.toList());
  }

  @ParameterizedTest
  @ValueSource(doubles = {0.01, 0.10, 0.50, 0.90, 100.00})
  public void benchBuffer(double ratio) throws IOException {

  }

  public void verifySort(List<Record> records, Iterator<Record> iterator) {
    for (Record record : records) {
      Record next = iterator.next();
      assert record.getKey() == next.getKey();
    }
  }

  private static class SortStat {
    long insertAndWriteTime;
    long sortTime;
    long readTime;

    @Override
    public String toString() {
      return "SortStat{"
          + "insertAndWriteTime=" + insertAndWriteTime
          + ", sortTime=" + sortTime
          + ", readTime=" + readTime
          + '}';
    }
  }

}
