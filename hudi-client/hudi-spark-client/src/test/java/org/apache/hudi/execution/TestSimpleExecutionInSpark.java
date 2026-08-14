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

package org.apache.hudi.execution;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.common.util.queue.SimpleExecutor;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Tests for {@link SimpleExecutor}.
 */
public class TestSimpleExecutionInSpark extends BaseExecutorTestHarness {

  @Override
  protected HoodieExecutor<Integer> createExecutor(
      Iterator<HoodieRecord> records, HoodieConsumer<HoodieRecord, Integer> consumer) {
    return new SimpleExecutor<>(records, consumer, Function.identity());
  }
}
