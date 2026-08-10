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

package org.apache.hudi.source.rebalance.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * Partitioner for regular streaming read.
 */
public class StreamReadAppendPartitioner implements Partitioner<Integer> {

  private final int parallelism;

  public StreamReadAppendPartitioner(int parallelism) {
    this.parallelism = parallelism;
  }

  @Override
  public int partition(Integer splitNum, int maxParallelism) {
    return splitNum % parallelism;
  }
}
