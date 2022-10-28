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

package org.apache.hudi.sink.common;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for write function.
 *
 * @param <I> the input type
 */
public abstract class AbstractWriteFunction<I> extends ProcessFunction<I, Object> implements BoundedOneInput {
  /**
   * Config options.
   */
  protected final Configuration config;
  /**
   * The current event time this write task seen for now.
   */
  protected  Long currentTimeStamp = Long.MIN_VALUE;

  private int eventTimeFieldIndex;

  private LogicalType eventTimeDataType;

  protected String eventTimeField;

  protected Schema writeSchema;

  public AbstractWriteFunction(Configuration config) {
    this.config = config;
    if (config.containsKey(FlinkOptions.EVENT_TIME_FIELD.key())) {
      this.writeSchema = StreamerUtil.getSourceSchema(config);
      this.eventTimeField = config.getString(FlinkOptions.EVENT_TIME_FIELD);
      this.eventTimeFieldIndex = this.writeSchema.getField(this.eventTimeField).pos();
      RowType rowType =
          (RowType) AvroSchemaConverter.convertToDataType(this.writeSchema).getLogicalType();
      this.eventTimeDataType = rowType.getTypeAt(eventTimeFieldIndex);
    }
  }

  /**
   * Sets up the event gateway.
   */
  public abstract void setOperatorEventGateway(OperatorEventGateway operatorEventGateway);

  /**
   * Invoked when bounded source ends up.
   */
  public abstract void endInput();

  /**
   * Handles the operator event sent by the coordinator.
   *
   * @param event The event
   */
  public abstract void handleOperatorEvent(OperatorEvent event);

  /**
   *  Extract TimeStamp from input value with specified event time field.
   * @param value The input value
   * @return the new timestamp for current.
   */
  public  long extractTimestamp(I value) {
    if (value instanceof HoodieAvroRecord) {
      return extractTimestamp((HoodieAvroRecord) value);
    }
    return extractTimestamp((RowData) value);
  }

  /**
   *  whether enable extract event time stamp from record.
   * @return flag to enable or disable the event time extract.
   */
  public  boolean extractTimeStampEnable() {
    return Objects.nonNull(this.eventTimeField);
  }

  public long extractTimestamp(HoodieAvroRecord value) {
    try {
      GenericRecord record = (GenericRecord) value.getData()
          .getInsertValue(this.writeSchema).get();
      Long eventTime = HoodieAvroUtils.getNestedFieldValAsLong(
          record, eventTimeField,
          true, -1L);
      this.currentTimeStamp =  Math.max(eventTime, this.currentTimeStamp);
      return eventTime;
    } catch (IOException e) {
      throw new HoodieException("extract event time failed. " + e);
    }
  }

  public long extractTimestamp(RowData value) {
    GenericRowData genericRowData = ((GenericRowData) value);
    Object eventTimeObject = genericRowData.getField(this.eventTimeFieldIndex);
    Long eventTime = DataTypeUtils.getAsLong(eventTimeObject, this.eventTimeDataType);
    this.currentTimeStamp = Math.max(eventTime, this.currentTimeStamp);
    return eventTime;
  }
}
