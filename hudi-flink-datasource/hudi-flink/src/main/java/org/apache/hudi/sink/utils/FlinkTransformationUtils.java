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

package org.apache.hudi.sink.utils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;

/**
 * Utilities for Flink transformations.
 */
public final class FlinkTransformationUtils {
  private FlinkTransformationUtils() {
  }

  public static <T> void setManagedMemoryWeight(Transformation<T> transformation, long memoryBytes) {
    if (memoryBytes <= 0) {
      return;
    }
    int weight = Math.max(1, (int) (memoryBytes >> 20)); // bytes to MiB
    transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, weight)
        .ifPresent(previousWeight -> {
          throw new IllegalStateException("Managed memory weight has been set, this should not happen.");
        });
  }
}
