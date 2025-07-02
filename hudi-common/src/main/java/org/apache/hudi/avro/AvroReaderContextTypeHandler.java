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

package org.apache.hudi.avro;

import org.apache.hudi.common.engine.ReaderContextTypeHandler;
import org.apache.hudi.common.util.StringUtils;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;

public class AvroReaderContextTypeHandler extends ReaderContextTypeHandler {
  @Override
  public String castToString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof CharSequence) {
      return value.toString(); // handles Utf8, String, etc.
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) value;
      return StringUtils.fromUTF8Bytes(buffer.array());
    }
    // Fallback toString for unexpected types (e.g., Integer, Boolean)
    return value.toString();
  }

  @Override
  public Object convertValueForAvroLogicalTypes(Schema fieldSchema,
                                                Object fieldValue,
                                                boolean consistentLogicalTimestampEnabled) {
    if (fieldSchema.getLogicalType() == LogicalTypes.date()) {
      return LocalDate.ofEpochDay(Long.parseLong(fieldValue.toString()));
    } else if (fieldSchema.getLogicalType() == LogicalTypes.timestampMillis()
        && consistentLogicalTimestampEnabled) {
      return new Timestamp(Long.parseLong(fieldValue.toString()));
    } else if (fieldSchema.getLogicalType() == LogicalTypes.timestampMicros()
        && consistentLogicalTimestampEnabled) {
      return new Timestamp(Long.parseLong(fieldValue.toString()) / 1000);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
      LogicalTypes.Decimal dc = (LogicalTypes.Decimal) fieldSchema.getLogicalType();
      Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
      if (fieldSchema.getType() == Schema.Type.FIXED) {
        return decimalConversion.fromFixed((GenericFixed) fieldValue, fieldSchema,
            LogicalTypes.decimal(dc.getPrecision(), dc.getScale()));
      } else if (fieldSchema.getType() == Schema.Type.BYTES) {
        ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
        BigDecimal convertedValue = decimalConversion.fromBytes(byteBuffer, fieldSchema,
            LogicalTypes.decimal(dc.getPrecision(), dc.getScale()));
        byteBuffer.rewind();
        return convertedValue;
      }
    }
    return fieldValue;
  }
}
