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

package org.apache.hudi.source.prune;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Static partition pruner for hoodie table source which partitions list is available in compile phase.
 * After applied this partition pruner, hoodie source could not read the data from other partitions during runtime.
 * Note: the data of new partitions created after the job starts would never be read.
 */
public class StaticPartitionPruner implements PartitionPruner {

  private static final long serialVersionUID = 1L;

  private final Set<String> partitionPaths;

  public StaticPartitionPruner(Set<String> partitionPaths) {
    this.partitionPaths = partitionPaths;
  }

  public Set<String> filter(Collection<String> partitions) {
    return partitions.stream()
        .filter(this.partitionPaths::contains).collect(Collectors.toSet());
  }

  public Set<String> getRequiredPartitions() {
    return this.partitionPaths;
  }
}
