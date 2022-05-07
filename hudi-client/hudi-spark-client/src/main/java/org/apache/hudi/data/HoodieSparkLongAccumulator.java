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

package org.apache.hudi.data;

import org.apache.hudi.common.data.HoodieAccumulator;

import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

/**
 * An accumulator on counts based on Spark {@link AccumulatorV2} implementation.
 */
public class HoodieSparkLongAccumulator extends HoodieAccumulator {

  private final AccumulatorV2<Long, Long> accumulator;

  private HoodieSparkLongAccumulator() {
    accumulator = new LongAccumulator();
  }

  public static HoodieSparkLongAccumulator create() {
    return new HoodieSparkLongAccumulator();
  }

  @Override
  public long value() {
    return accumulator.value();
  }

  @Override
  public void add(long increment) {
    accumulator.add(increment);
  }

  public AccumulatorV2<Long, Long> getAccumulator() {
    return accumulator;
  }
}
