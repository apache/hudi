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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.table.data.RowData;

/**
 * Base class for write function.
 *
 * @param <I> the input type
 */
public abstract class AbstractWriteFunction<I> extends ProcessFunction<I, RowData> implements BoundedOneInput {
  /**
   * Sets up the {@code Correspondent} for responsive request.
   */
  public abstract void setCorrespondent(Correspondent correspondent);

  /**
   * Sets up the event gateway.
   */
  public abstract void setOperatorEventGateway(OperatorEventGateway operatorEventGateway);

  /**
   * Invoked when bounded source ends up.
   */
  public abstract void endInput();

  /**
   * Handles the operator event sent by the coordinator.
   *
   * @param event The event
   */
  public abstract void handleOperatorEvent(OperatorEvent event);
}
