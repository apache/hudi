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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestRecordReaderValueIterator {

  @Test
  public void testValueIterator() {
    String[] values = new String[] {"hoodie", "efficient", "new project", "realtime", "spark", "dataset",};
    List<Pair<Integer, String>> entries =
        IntStream.range(0, values.length).boxed().map(idx -> Pair.of(idx, values[idx])).collect(Collectors.toList());
    TestRecordReader reader = new TestRecordReader(entries);
    RecordReaderValueIterator<IntWritable, Text> itr = new RecordReaderValueIterator<IntWritable, Text>(reader);
    for (int i = 0; i < values.length; i++) {
      Assert.assertTrue(itr.hasNext());
      Text val = itr.next();
      Assert.assertEquals(values[i], val.toString());
    }
    Assert.assertFalse(itr.hasNext());
  }

  /**
   * Simple replay record reader for unit-testing.
   */
  private static class TestRecordReader implements RecordReader<IntWritable, Text> {

    private final List<Pair<Integer, String>> entries;
    private int currIndex = 0;

    public TestRecordReader(List<Pair<Integer, String>> entries) {
      this.entries = entries;
    }

    @Override
    public boolean next(IntWritable key, Text value) throws IOException {
      if (currIndex >= entries.size()) {
        return false;
      }
      key.set(entries.get(currIndex).getLeft());
      value.set(entries.get(currIndex).getRight());
      currIndex++;
      return true;
    }

    @Override
    public IntWritable createKey() {
      return new IntWritable();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return currIndex;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return (currIndex * 1.0F) / entries.size();
    }
  }
}
