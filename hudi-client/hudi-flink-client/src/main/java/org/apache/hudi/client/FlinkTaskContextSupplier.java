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

package org.apache.hudi.client;

import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.util.Option;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.function.Supplier;

/**
 * Flink task context supplier.
 */
public class FlinkTaskContextSupplier extends TaskContextSupplier {
  private RuntimeContext flinkRuntimeContext;

  public FlinkTaskContextSupplier(RuntimeContext flinkRuntimeContext) {
    this.flinkRuntimeContext = flinkRuntimeContext;
  }

  public RuntimeContext getFlinkRuntimeContext() {
    return flinkRuntimeContext;
  }

  @Override
  public Supplier<Integer> getPartitionIdSupplier() {
    return () -> this.flinkRuntimeContext.getIndexOfThisSubtask();
  }

  @Override
  public Supplier<Integer> getStageIdSupplier() {
    // need to check again
    return () -> this.flinkRuntimeContext.getNumberOfParallelSubtasks();
  }

  @Override
  public Supplier<Long> getAttemptIdSupplier() {
    return () -> (long) this.flinkRuntimeContext.getAttemptNumber();
  }

  @Override
  public Option<String> getProperty(EngineProperty prop) {
    // no operation for now
    return Option.empty();
  }

}
