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

package org.apache.hudi.common.util.sorter;

import org.apache.hudi.common.util.SizeEstimator;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class ExternalSorterBenchmarkHarness {

  private static final int DATA_SIZE = 1024; // 1KB

  protected static class Record implements Comparable<Record>, Serializable {
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
    public int compareTo(Record o) {
      return Integer.compare(key, o.key);
    }
  }

  protected int totalNum;
  protected int totalSize;
  protected SizeEstimator<Record> sizeEstimator = record -> 4 + 4 + 4 + DATA_SIZE;
  protected List<Record> records;
  protected List<Record> sortedRecords;
  protected File tmpDir;

  protected void prepare() {
    // total 500MB data
    totalNum = 1 << 19;
    totalSize = totalNum * (4 + 4 + 4 + DATA_SIZE);
    // generate random records
    Random random = new Random();
    records = random.ints(totalNum, 0, totalNum / 20).mapToObj(key -> new Record(key)).collect(Collectors.toList());
    sortedRecords = records.stream().sorted().collect(Collectors.toList());
    tmpDir = new File(System.getProperty("java.io.tmpdir"));
  }

}
