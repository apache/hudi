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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Utils for Hadoop MapRed in testing.
 */
public class HadoopMapRedUtils {

  /**
   * Creates instance of {@link Reporter} to collect reported counters
   */
  public static Reporter createTestReporter() {
    class TestReporter implements Reporter {
      private final ConcurrentHashMap<String, Counters.Counter> counters =
          new ConcurrentHashMap<>();

      @Override
      public void setStatus(String status) {
        // not-supported
      }

      @Override
      public Counters.Counter getCounter(Enum<?> name) {
        return counters.computeIfAbsent(name.name(), (ignored) -> new Counters.Counter());
      }

      @Override
      public Counters.Counter getCounter(String group, String name) {
        return counters.computeIfAbsent(getKey(group, name), (ignored) -> new Counters.Counter());
      }

      @Override
      public void incrCounter(Enum<?> key, long amount) {
        Option.ofNullable(counters.get(key))
            .ifPresent(c -> c.increment(amount));
      }

      @Override
      public void incrCounter(String group, String counter, long amount) {
        Option.ofNullable(counters.get(getKey(group, counter)))
            .ifPresent(c -> c.increment(amount));
      }

      @Override
      public InputSplit getInputSplit() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public float getProgress() {
        return -1;
      }

      @Override
      public void progress() {
        // not-supported
      }

      private String getKey(String group, String name) {
        return String.format("%s:%s", group, name);
      }
    }

    return new TestReporter();
  }
}
