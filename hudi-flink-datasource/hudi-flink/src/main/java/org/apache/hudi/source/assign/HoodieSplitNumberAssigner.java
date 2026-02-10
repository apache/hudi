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

package org.apache.hudi.source.assign;

import org.apache.hudi.source.split.HoodieSourceSplit;

/**
 * Implementation of {@link HoodieSplitAssigner} that assigns Hoodie
 * source splits to task IDs using round-robin distribution based on split number.
 */
public class HoodieSplitNumberAssigner implements HoodieSplitAssigner {
  private final int parallelism;

  /**
   * Creates a new HoodieSplitNumberAssigner.
   *
   * @param parallelism the number of parallel tasks (must be positive)
   * @throws IllegalArgumentException if parallelism is less than or equal to 0
   */
  public HoodieSplitNumberAssigner(int parallelism) {
    if (parallelism <= 0) {
      throw new IllegalArgumentException("Parallelism must be positive, but was: " + parallelism);
    }
    this.parallelism = parallelism;
  }

  @Override
  public int assign(HoodieSourceSplit split) {
    return split.getSplitNum() % parallelism;
  }
}
