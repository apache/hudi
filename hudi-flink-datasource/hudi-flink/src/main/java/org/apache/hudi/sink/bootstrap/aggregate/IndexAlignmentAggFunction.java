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

package org.apache.hudi.sink.bootstrap.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Aggregate function that accumulates loading the index.
 */
public class IndexAlignmentAggFunction implements AggregateFunction<Tuple4<String, Integer, Integer, Long>, IndexAlignmentAccumulator, Boolean> {
  public static final String NAME = IndexAlignmentAggFunction.class.getSimpleName();

  @Override
  public IndexAlignmentAccumulator createAccumulator() {
    return new IndexAlignmentAccumulator();
  }

  @Override
  public IndexAlignmentAccumulator add(Tuple4<String, Integer, Integer, Long> taskDetails, IndexAlignmentAccumulator indexAlignmentAccumulator) {
    indexAlignmentAccumulator.update(taskDetails);
    return indexAlignmentAccumulator;
  }

  @Override
  public Boolean getResult(IndexAlignmentAccumulator indexAlignmentAccumulator) {
    return indexAlignmentAccumulator.isReady();
  }

  @Override
  public IndexAlignmentAccumulator merge(IndexAlignmentAccumulator indexAlignmentAccumulator, IndexAlignmentAccumulator acc) {
    return indexAlignmentAccumulator.merge(acc);
  }
}