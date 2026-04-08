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

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * Base class for composite handlers that wrap one data-table handler and one metadata-table handler.
 *
 * <p>The composite subclasses provide a single entry point to Flink operators while keeping the
 * table-specific routing inside the handler layer.
 *
 * @param <H> table service handler type
 */
public abstract class CompositeTableServiceHandler<H> implements Closeable {
  protected final H dataTableHandler;
  protected final H metadataTableHandler;

  protected CompositeTableServiceHandler(H dataTableHandler, H metadataTableHandler) {
    this.dataTableHandler = dataTableHandler;
    this.metadataTableHandler = metadataTableHandler;
  }

  protected H getHandler(boolean isMetadataTable) {
    return isMetadataTable ? metadataTableHandler : dataTableHandler;
  }

  protected void forEachHandler(Consumer<H> consumer) {
    consumer.accept(dataTableHandler);
    consumer.accept(metadataTableHandler);
  }
}
