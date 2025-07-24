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

package org.apache.hudi.utils;

import org.apache.hudi.adapter.SourceFunctionAdapter;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * Adapter utils to get {@code SourceContext}.
 */
public class SourceContextUtils {

  public static <O> SourceFunctionAdapter.SourceContext<O> getSourceContext(
      StreamConfig streamConfig,
      ProcessingTimeService processingTimeService,
      Output<StreamRecord<O>> output,
      long watermarkInterval) {
    return StreamSourceContexts.getSourceContext(
        streamConfig.getTimeCharacteristic(),
        processingTimeService,
        new Object(), // no actual locking needed
        output,
        watermarkInterval,
        -1,
        true);
  }
}
