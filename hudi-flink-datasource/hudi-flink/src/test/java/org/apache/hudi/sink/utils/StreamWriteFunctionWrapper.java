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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;

import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.Map;

/**
 * A wrapper class to manipulate the {@link StreamWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class StreamWriteFunctionWrapper<I> extends BaseStreamWriteFunctionWrapper<I> {

  public StreamWriteFunctionWrapper(String tablePath,  Configuration conf) throws Exception {
    super(tablePath, conf);
  }

  @Override
  public Map<String, List<HoodieRecord>> getDataBuffer() {
    return ((StreamWriteFunction) writeFunction).getDataBuffer();
  }

  @Override
  protected AbstractStreamWriteFunction createWriteFunction() {
    return new StreamWriteFunction(conf, rowType);
  }
}
