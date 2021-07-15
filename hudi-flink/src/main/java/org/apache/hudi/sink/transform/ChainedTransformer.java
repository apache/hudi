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

package org.apache.hudi.sink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 */
public class ChainedTransformer implements Transformer {

  private List<Transformer> transformers;

  public ChainedTransformer(List<Transformer> transformers) {
    this.transformers = transformers;
  }

  public List<String> getTransformersNames() {
    return transformers.stream().map(t -> t.getClass().getName()).collect(Collectors.toList());
  }

  @Override
  public DataStream<RowData> apply(DataStream<RowData> source) {
    DataStream<RowData> dataStream = source;
    for (Transformer t : transformers) {
      dataStream = t.apply(dataStream);
    }

    return dataStream;
  }
}
