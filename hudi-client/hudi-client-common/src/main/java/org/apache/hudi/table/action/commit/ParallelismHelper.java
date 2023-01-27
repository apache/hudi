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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.function.SerializableFunctionUnchecked;

public abstract class ParallelismHelper<I> {

  private final SerializableFunctionUnchecked<I, Integer> partitionNumberExtractor;

  protected ParallelismHelper(SerializableFunctionUnchecked<I, Integer> partitionNumberExtractor) {
    this.partitionNumberExtractor = partitionNumberExtractor;
  }

  protected int deduceShuffleParallelism(I input, int configuredParallelism) {
    // NOTE: In case parallelism was configured by the user we will always
    //       honor that setting.
    //       Otherwise, we'd keep parallelism of the incoming dataset
    if (configuredParallelism > 0) {
      return configuredParallelism;
    }

    return partitionNumberExtractor.apply(input);
  }

}
