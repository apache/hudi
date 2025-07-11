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

package org.apache.hudi.sink.v2;

import org.apache.hudi.adapter.SinkAdapter;
import org.apache.hudi.sink.v2.utils.PipelinesV2;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Hoodie sink based on Flink sink V2 API.
 */
public class HoodieSink implements SinkAdapter<RowData>, SupportsPreWriteTopology<RowData> {
  private final Configuration conf;
  private final RowType rowType;
  private final boolean overwrite;
  private final boolean isBounded;

  public HoodieSink(Configuration conf, RowType rowType, boolean overwrite, boolean isBounded) {
    this.conf = conf;
    this.rowType = rowType;
    this.overwrite = overwrite;
    this.isBounded = isBounded;
  }

  @Override
  public SinkWriter<RowData> createWriter() {
    return DummySinkWriter.INSTANCE;
  }

  @Override
  public DataStream<RowData> addPreWriteTopology(DataStream<RowData> dataStream) {
    return PipelinesV2.composePipeline(dataStream, conf, rowType, overwrite, isBounded);
  }

  /**
   * Dummy sink writer that does nothing.
   */
  private static class DummySinkWriter implements SinkWriter<RowData> {
    private static final SinkWriter<RowData> INSTANCE = new DummySinkWriter();

    @Override
    public void write(RowData element, Context context) {
      // do nothing.
    }

    @Override
    public void flush(boolean endOfInput) {
      // do nothing.
    }

    @Override
    public void close() {
      // do nothing.
    }
  }
}
