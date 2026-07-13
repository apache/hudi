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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sink.event;

import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Converts the commit-relevant portion of {@link WriteStatus} to the stable Avro state DTO. */
@SuppressWarnings({"rawtypes", "unchecked"})
final class WriteStatusAvroMapper {

  private WriteStatusAvroMapper() {
  }

  static org.apache.hudi.sink.avro.model.WriteStatus toAvro(WriteStatus status) throws IOException {
    org.apache.hudi.sink.avro.model.WriteStatus state = new org.apache.hudi.sink.avro.model.WriteStatus();
    state.setStat(toAvro(status.getStat()));
    List<org.apache.hudi.sink.avro.model.ErrorEntry> errors = new ArrayList<>();
    for (Map.Entry<HoodieKey, Throwable> entry : status.getErrors().entrySet()) {
      org.apache.hudi.sink.avro.model.ErrorEntry error = new org.apache.hudi.sink.avro.model.ErrorEntry();
      error.setKey(toAvro(entry.getKey()));
      error.setError(toAvro(entry.getValue()));
      errors.add(error);
    }
    state.setErrors(errors);
    state.setGlobalError(toAvro(status.getGlobalError()));
    state.setFileId(status.getFileId());
    state.setPartitionPath(status.getPartitionPath());
    state.setTotalRecords(status.getTotalRecords());
    state.setTotalErrorRecords(status.getTotalErrorRecords());
    state.setRecordsStats(toAvroColumnStats(status.getStat()));
    return state;
  }

  static WriteStatus fromAvro(org.apache.hudi.sink.avro.model.WriteStatus state, boolean isMetadataTable)
      throws IOException {
    WriteStatus status = new WriteStatus(false, 0D, isMetadataTable);
    status.setStat(fromAvro(state.getStat()));
    for (org.apache.hudi.sink.avro.model.ErrorEntry error : state.getErrors()) {
      status.getErrors().put(fromAvro(error.getKey()), fromAvro(error.getError()));
    }
    status.setGlobalError(fromAvro(state.getGlobalError()));
    status.setFileId(state.getFileId());
    status.setPartitionPath(state.getPartitionPath());
    status.setTotalRecords(state.getTotalRecords());
    status.setTotalErrorRecords(state.getTotalErrorRecords());
    Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats =
        fromAvroColumnStats(state.getRecordsStats());
    if (columnStats != null) {
      status.getStat().setRecordsStats(columnStats);
    }
    return status;
  }

  private static Object toAvro(HoodieWriteStat stat) {
    if (stat instanceof HoodieDeltaWriteStat) {
      return JsonUtils.getObjectMapper().convertValue(
          stat, org.apache.hudi.sink.avro.model.HoodieDeltaWriteStat.class);
    }
    return JsonUtils.getObjectMapper().convertValue(
        stat, org.apache.hudi.sink.avro.model.HoodieWriteStat.class);
  }

  private static HoodieWriteStat fromAvro(Object state) throws IOException {
    if (state instanceof org.apache.hudi.sink.avro.model.HoodieDeltaWriteStat) {
      return JsonUtils.getObjectMapper().convertValue(state, HoodieDeltaWriteStat.class);
    }
    if (state instanceof org.apache.hudi.sink.avro.model.HoodieWriteStat) {
      return JsonUtils.getObjectMapper().convertValue(state, HoodieWriteStat.class);
    }
    throw new IOException("Unknown write stat state type: " + state.getClass().getName());
  }

  private static org.apache.hudi.sink.avro.model.HoodieKey toAvro(HoodieKey key) {
    org.apache.hudi.sink.avro.model.HoodieKey state = new org.apache.hudi.sink.avro.model.HoodieKey();
    state.setRecordKey(key.getRecordKey());
    state.setPartitionPath(key.getPartitionPath());
    return state;
  }

  private static HoodieKey fromAvro(org.apache.hudi.sink.avro.model.HoodieKey state) {
    return new HoodieKey(state.getRecordKey(), state.getPartitionPath());
  }

  private static org.apache.hudi.sink.avro.model.Throwable toAvro(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    org.apache.hudi.sink.avro.model.Throwable state = new org.apache.hudi.sink.avro.model.Throwable();
    state.setClassName(throwable.getClass().getName());
    state.setMessage(throwable.getMessage());
    List<org.apache.hudi.sink.avro.model.StackTraceElement> stackTrace = new ArrayList<>();
    for (StackTraceElement element : throwable.getStackTrace()) {
      org.apache.hudi.sink.avro.model.StackTraceElement stackElement =
          new org.apache.hudi.sink.avro.model.StackTraceElement();
      stackElement.setClassName(element.getClassName());
      stackElement.setMethodName(element.getMethodName());
      stackElement.setFileName(element.getFileName());
      stackElement.setLineNumber(element.getLineNumber());
      stackTrace.add(stackElement);
    }
    state.setStackTrace(stackTrace);
    return state;
  }

  private static Throwable fromAvro(org.apache.hudi.sink.avro.model.Throwable state) {
    if (state == null) {
      return null;
    }
    RuntimeException restored = new RuntimeException(state.getClassName()
        + (state.getMessage() == null ? "" : ": " + state.getMessage()));
    StackTraceElement[] stackTrace = new StackTraceElement[state.getStackTrace().size()];
    for (int i = 0; i < stackTrace.length; i++) {
      org.apache.hudi.sink.avro.model.StackTraceElement element = state.getStackTrace().get(i);
      stackTrace[i] = new StackTraceElement(
          element.getClassName(), element.getMethodName(), element.getFileName(), element.getLineNumber());
    }
    restored.setStackTrace(stackTrace);
    return restored;
  }

  private static Map<String, org.apache.hudi.sink.avro.model.ColumnRange> toAvroColumnStats(
      HoodieWriteStat stat) throws IOException {
    if (!stat.getColumnStats().isPresent()) {
      return null;
    }
    Map<String, org.apache.hudi.sink.avro.model.ColumnRange> result = new HashMap<>();
    for (Map.Entry<String, HoodieColumnRangeMetadata<Comparable>> entry : stat.getColumnStats().get().entrySet()) {
      HoodieColumnRangeMetadata<Comparable> value = entry.getValue();
      org.apache.hudi.sink.avro.model.ColumnRange state = new org.apache.hudi.sink.avro.model.ColumnRange();
      state.setFilePath(value.getFilePath());
      state.setColumnName(value.getColumnName());
      state.setMinValue(toAvro(value.getMinValue()));
      state.setMaxValue(toAvro(value.getMaxValue()));
      state.setNullCount(value.getNullCount());
      state.setValueCount(value.getValueCount());
      state.setTotalSize(value.getTotalSize());
      state.setTotalUncompressedSize(value.getTotalUncompressedSize());
      state.setValueMetadata(toAvro(value.getValueMetadata()));
      result.put(entry.getKey(), state);
    }
    return result;
  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> fromAvroColumnStats(
      Map<String, org.apache.hudi.sink.avro.model.ColumnRange> states) throws IOException {
    if (states == null) {
      return null;
    }
    Map<String, HoodieColumnRangeMetadata<Comparable>> result = new HashMap<>();
    for (Map.Entry<String, org.apache.hudi.sink.avro.model.ColumnRange> entry : states.entrySet()) {
      org.apache.hudi.sink.avro.model.ColumnRange state = entry.getValue();
      Comparable minValue = fromAvro(state.getMinValue());
      Comparable maxValue = fromAvro(state.getMaxValue());
      ValueMetadata valueMetadata = state.getValueMetadata() == null
          ? legacyValueMetadata(minValue, maxValue)
          : fromAvro(state.getValueMetadata());
      result.put(entry.getKey(), HoodieColumnRangeMetadata.<Comparable>create(
          state.getFilePath(), state.getColumnName(), minValue, maxValue,
          state.getNullCount(), state.getValueCount(), state.getTotalSize(),
          state.getTotalUncompressedSize(), valueMetadata));
    }
    return result;
  }

  private static ValueMetadata legacyValueMetadata(Comparable minValue, Comparable maxValue) {
    return minValue == null && maxValue == null
        ? ValueMetadata.NULL_METADATA
        : ValueMetadata.getEmptyValueMetadata(HoodieIndexVersion.V1);
  }

  private static org.apache.hudi.sink.avro.model.ValueMetadata toAvro(ValueMetadata metadata) {
    org.apache.hudi.sink.avro.model.ValueMetadata state = new org.apache.hudi.sink.avro.model.ValueMetadata();
    if (metadata == null || metadata == ValueMetadata.NULL_METADATA) {
      state.setKind("NULL");
      return state;
    }
    HoodieValueTypeInfo typeInfo = metadata.getValueTypeInfo();
    if (typeInfo == null) {
      state.setKind("EMPTY");
    } else {
      state.setKind("VALUE");
      state.setTypeOrdinal(typeInfo.getTypeOrdinal());
      state.setAdditionalInfo(typeInfo.getAdditionalInfo());
    }
    return state;
  }

  private static ValueMetadata fromAvro(org.apache.hudi.sink.avro.model.ValueMetadata state) {
    if (state == null || "NULL".equals(state.getKind())) {
      return ValueMetadata.NULL_METADATA;
    }
    if ("EMPTY".equals(state.getKind())) {
      return ValueMetadata.getEmptyValueMetadata(HoodieIndexVersion.V1);
    }
    return ValueMetadata.getValueMetadata(HoodieValueTypeInfo.newBuilder()
        .setTypeOrdinal(state.getTypeOrdinal())
        .setAdditionalInfo(state.getAdditionalInfo())
        .build());
  }

  private static org.apache.hudi.sink.avro.model.TypedValue toAvro(Comparable value) throws IOException {
    if (value == null) {
      return null;
    }
    org.apache.hudi.sink.avro.model.TypedValue state = new org.apache.hudi.sink.avro.model.TypedValue();
    if (value instanceof CharSequence) {
      state.setType("STRING");
      state.setStringValue(value.toString());
    } else if (value instanceof Integer) {
      state.setType("INTEGER");
      state.setIntValue((Integer) value);
    } else if (value instanceof Long) {
      state.setType("LONG");
      state.setLongValue((Long) value);
    } else if (value instanceof Float) {
      state.setType("FLOAT");
      state.setFloatValue((Float) value);
    } else if (value instanceof Double) {
      state.setType("DOUBLE");
      state.setDoubleValue((Double) value);
    } else if (value instanceof Boolean) {
      state.setType("BOOLEAN");
      state.setBooleanValue((Boolean) value);
    } else if (value instanceof BigDecimal) {
      state.setType("BIG_DECIMAL");
      state.setStringValue(value.toString());
    } else if (value instanceof BigInteger) {
      state.setType("BIG_INTEGER");
      state.setStringValue(value.toString());
    } else if (value instanceof Date) {
      state.setType("SQL_DATE");
      state.setLongValue(((Date) value).getTime());
    } else if (value instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) value;
      state.setType("TIMESTAMP");
      state.setLongValue(timestamp.getTime());
      state.setExtraValue(timestamp.getNanos());
    } else if (value instanceof ByteBuffer) {
      ByteBuffer duplicate = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      state.setType("BYTE_BUFFER");
      state.setBytesValue(ByteBuffer.wrap(bytes));
    } else if (value instanceof UUID) {
      state.setType("UUID");
      state.setStringValue(value.toString());
    } else if (value instanceof LocalDate) {
      state.setType("LOCAL_DATE");
      state.setLongValue(((LocalDate) value).toEpochDay());
    } else if (value instanceof LocalTime) {
      state.setType("LOCAL_TIME");
      state.setNanoValue(((LocalTime) value).toNanoOfDay());
    } else if (value instanceof Instant) {
      Instant instant = (Instant) value;
      state.setType("INSTANT");
      state.setLongValue(instant.getEpochSecond());
      state.setExtraValue(instant.getNano());
    } else if (value instanceof LocalDateTime) {
      LocalDateTime dateTime = (LocalDateTime) value;
      state.setType("LOCAL_DATE_TIME");
      state.setLongValue(dateTime.toLocalDate().toEpochDay());
      state.setNanoValue(dateTime.toLocalTime().toNanoOfDay());
    } else {
      throw new IOException("Unsupported column stats value type: " + value.getClass().getName());
    }
    return state;
  }

  private static Comparable fromAvro(org.apache.hudi.sink.avro.model.TypedValue state) throws IOException {
    if (state == null) {
      return null;
    }
    switch (state.getType()) {
      case "STRING":
        return state.getStringValue();
      case "INTEGER":
        return state.getIntValue();
      case "LONG":
        return state.getLongValue();
      case "FLOAT":
        return state.getFloatValue();
      case "DOUBLE":
        return state.getDoubleValue();
      case "BOOLEAN":
        return state.getBooleanValue();
      case "BIG_DECIMAL":
        return new BigDecimal(state.getStringValue());
      case "BIG_INTEGER":
        return new BigInteger(state.getStringValue());
      case "SQL_DATE":
        return new Date(state.getLongValue());
      case "TIMESTAMP":
        Timestamp timestamp = new Timestamp(state.getLongValue());
        timestamp.setNanos(state.getExtraValue());
        return timestamp;
      case "BYTE_BUFFER":
        return state.getBytesValue();
      case "UUID":
        return UUID.fromString(state.getStringValue());
      case "LOCAL_DATE":
        return LocalDate.ofEpochDay(state.getLongValue());
      case "LOCAL_TIME":
        return LocalTime.ofNanoOfDay(state.getNanoValue());
      case "INSTANT":
        return Instant.ofEpochSecond(state.getLongValue(), state.getExtraValue());
      case "LOCAL_DATE_TIME":
        return LocalDateTime.of(
            LocalDate.ofEpochDay(state.getLongValue()), LocalTime.ofNanoOfDay(state.getNanoValue()));
      default:
        throw new IOException("Unknown column stats value type: " + state.getType());
    }
  }
}
