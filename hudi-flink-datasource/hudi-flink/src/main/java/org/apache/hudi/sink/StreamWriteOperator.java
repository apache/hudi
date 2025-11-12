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

package org.apache.hudi.sink;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.sink.common.AbstractWriteOperator;
import org.apache.hudi.sink.common.WriteOperatorFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.table.types.logical.RowType;

/**
 * Operator for {@link StreamSink}.
 */
public class StreamWriteOperator extends AbstractWriteOperator<HoodieFlinkInternalRow> {

  public StreamWriteOperator(Configuration conf, RowType rowType) {
    super(new StreamWriteFunction(conf, rowType));
  }

  public static WriteOperatorFactory<HoodieFlinkInternalRow> getFactory(Configuration conf, RowType rowType) {
    return WriteOperatorFactory.instance(conf, new StreamWriteOperator(conf, rowType));
  }
}
