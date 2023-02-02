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

package org.apache.hudi.util;

import org.apache.hudi.exception.HoodieException;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

/**
 * Converter that converts a string into Flink StateBackend.
 */
public class FlinkStateBackendConverter implements IStringConverter<StateBackend> {
  @Override
  public StateBackend convert(String value) throws ParameterException {
    switch (value) {
      case "hashmap":
        return new HashMapStateBackend();
      case "rocksdb":
        return new EmbeddedRocksDBStateBackend();
      default:
        throw new HoodieException(String.format("Unknown flink state backend %s. Supports only hashmap and rocksdb by now", value));
    }
  }
}
