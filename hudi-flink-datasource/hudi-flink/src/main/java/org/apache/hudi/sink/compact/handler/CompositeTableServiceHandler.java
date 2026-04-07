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

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.common.util.Option;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * Base class for composite handlers that encapsulate data table and metadata table service handlers.
 *
 * <p>The composite subclasses expose a single entry point to Flink operators while keeping the
 * specific handler references, and routing inside the handler layer.
 *
 * @param <H> table service handler type
 */
public abstract class CompositeTableServiceHandler<H> implements Closeable {
  protected final Option<H> dataTableHandler;
  protected final Option<H> metadataTableHandler;

  protected CompositeTableServiceHandler(Option<H> dataTableHandler, Option<H> metadataTableHandler) {
    this.dataTableHandler = dataTableHandler;
    this.metadataTableHandler = metadataTableHandler;
  }

  protected H getHandler(boolean isMetadataTable) {
    return isMetadataTable ? metadataTableHandler.get() : dataTableHandler.get();
  }

  protected void forEachHandler(Consumer<H> consumer) {
    dataTableHandler.ifPresent(consumer);
    metadataTableHandler.ifPresent(consumer);
  }
}
