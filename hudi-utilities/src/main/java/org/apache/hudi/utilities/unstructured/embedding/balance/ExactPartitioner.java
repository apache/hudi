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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.spark.Partitioner;

/**
 * Sends an Integer key k to partition k, so a {@link BalancingAssignment} is applied exactly.
 */
public class ExactPartitioner extends Partitioner {

  private static final long serialVersionUID = 1L;

  private final int numPartitions;

  public ExactPartitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  public int getPartition(Object key) {
    int partition = (Integer) key;
    if (partition < 0 || partition >= numPartitions) {
      throw new IllegalArgumentException("Partition " + partition + " outside [0, " + numPartitions + ")");
    }
    return partition;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ExactPartitioner && ((ExactPartitioner) other).numPartitions == numPartitions;
  }

  @Override
  public int hashCode() {
    return numPartitions;
  }
}
