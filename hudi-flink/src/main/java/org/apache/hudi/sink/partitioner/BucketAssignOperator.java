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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;

/**
 * Operator for {@link BucketAssignFunction}.
 *
 * @param <I> The input type
 */
public class BucketAssignOperator<K, I extends HoodieRecord<?>, O extends HoodieRecord<?>>
    extends KeyedProcessOperator<K, I, O> {
  private final BucketAssignFunction<K, I, O> function;

  public BucketAssignOperator(BucketAssignFunction<K, I, O> function) {
    super(function);
    this.function = function;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.function.setContext(new ContextImpl());
  }

  /**
   * Context to give the function chance to operate the state handle.
   */
  public interface Context {
    void setCurrentKey(Object key);
  }

  public class ContextImpl implements Context {
    public void setCurrentKey(Object key) {
      BucketAssignOperator.this.setCurrentKey(key);
    }
  }
}
