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

package org.apache.hudi.sink.common;

import org.apache.hudi.sink.event.Correspondent;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ProcessOperator;

/**
 * Base class for write operator.
 *
 * @param <I> the input type
 */
public abstract class AbstractWriteOperator<I>
    extends ProcessOperator<I, Object>
    implements OperatorEventHandler, BoundedOneInput {
  private final AbstractWriteFunction<I> function;

  public AbstractWriteOperator(AbstractWriteFunction<I> function) {
    super(function);
    this.function = function;
  }

  public void setCorrespondent(Correspondent correspondent) {
    this.function.setCorrespondent(correspondent);
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.function.setOperatorEventGateway(operatorEventGateway);
  }

  @Override
  public void endInput() {
    this.function.endInput();
  }

  @Override
  public void handleOperatorEvent(OperatorEvent evt) {
    this.function.handleOperatorEvent(evt);
  }
}
