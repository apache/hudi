/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.adapter;

import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/** Adapter class for {@code Output} to handle async compaction/clustering service thread safe issues */
public class MaskingOutputAdapter<OUT> implements Output<StreamRecord<OUT>> {

  private final Output<StreamRecord<OUT>> output;

  public MaskingOutputAdapter(Output<StreamRecord<OUT>> output) {
    this.output = output;
  }

  @Override
  public void emitWatermark(Watermark watermark) {
    // For thread safe, not to propagate the watermark
  }

  @Override
  public void emitLatencyMarker(LatencyMarker latencyMarker) {
    // For thread safe, not to propagate latency marker
  }

  @Override
  public void emitRecordAttributes(RecordAttributes recordAttributes) {
    // For thread safe, not to propagate watermark status
  }

  @Override
  public void emitWatermark(WatermarkEvent watermarkEvent) {
    // For thread safe, not to propagate the watermark
  }

  @Override
  public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
    // For thread safe, not to propagate watermark status
  }

  @Override
  public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> streamRecord) {
    this.output.collect(outputTag, streamRecord);
  }

  @Override
  public void collect(StreamRecord<OUT> outStreamRecord) {
    this.output.collect(outStreamRecord);
  }

  @Override
  public void close() {
    this.output.close();
  }
}
