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

package org.apache.hudi.adapter;

import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;

/**
 * Adapter clazz for {@code SupportsPreWriteTopology}.
 *
 * <p>{@code SupportsPreWriteTopology} only exists since Flink 1.19, where it was split out of
 * {@link WithPreWriteTopology}; both declare the same {@code addPreWriteTopology} method, and
 * Flink 1.18 dispatches on {@link WithPreWriteTopology} in {@code SinkTransformationTranslator}.
 * Extending it here keeps the pre-write topology hook wired for Flink 1.18 as well.
 */
public interface SupportsPreWriteTopologyAdapter<InputT> extends WithPreWriteTopology<InputT> {
}
