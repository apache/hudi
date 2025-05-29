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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.keygen.KeyGenUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * A converter that creates a {@link RowData} from hoodie record key.
 * The converter is designed to be stateful(not pure stateless tool)
 * in order to reuse the specific converters.
 */
@Internal
public class RecordKeyToRowDataConverter {
  private final Converter[] converters;
  private final int[] fieldsPos;
  private final int rowArity;
  // position of `_hoodie_record_key`
  private final int keyMetaPos;

  public RecordKeyToRowDataConverter(int[] fieldsPos, RowType rowType) {
    this.fieldsPos = fieldsPos;
    this.rowArity = rowType.getFieldCount();
    this.keyMetaPos = rowType.getFieldNames().indexOf(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    this.converters = Arrays.stream(fieldsPos)
        .mapToObj(f -> getConverter(rowType.getTypeAt(f)))
        .toArray(Converter[]::new);
  }

  public RowData convert(String recordKey) {
    final String[] pkVals = KeyGenUtils.extractRecordKeys(recordKey);
    ValidationUtils.checkArgument(converters.length == pkVals.length,
        "Field types and values should equal with number");

    GenericRowData rowData = new GenericRowData(rowArity);
    // set record key metadata field if it's in the data schema.
    if (keyMetaPos > -1) {
      rowData.setField(keyMetaPos, StringData.fromString(recordKey));
    }
    for (int i = 0; i < pkVals.length; i++) {
      rowData.setField(fieldsPos[i], converters[i].convert(pkVals[i]));
    }
    return rowData;
  }

  private interface Converter {
    Object convert(String field);
  }

  private static Converter getConverter(LogicalType logicalType) {
    switch (logicalType.getTypeRoot()) {
      case NULL:
        return field -> null;
      case TINYINT:
        return Byte::parseByte;
      case SMALLINT:
        return Short::parseShort;
      case BOOLEAN:
        return Boolean::parseBoolean;
      case INTEGER:
      case TIME_WITHOUT_TIME_ZONE:
        return Integer::parseInt;
      case BIGINT:
        return Long::parseLong;
      case FLOAT:
        return Float::parseFloat;
      case DOUBLE:
        return Double::parseDouble;
      case DATE:
        // see HoodieAvroUtils#convertValueForAvroLogicalTypes
        return field -> (int) LocalDate.parse(field).toEpochDay();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        final int precision = ((TimestampType) logicalType).getPrecision();
        if (precision <= 3) {
          return field -> TimestampData.fromInstant(Instant.EPOCH.plus(Long.parseLong(field), ChronoUnit.MILLIS));
        } else if (precision <= 6) {
          return field -> TimestampData.fromInstant(Instant.EPOCH.plus(Long.parseLong(field), ChronoUnit.MICROS));
        } else {
          throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
      case CHAR:
      case VARCHAR:
        return StringData::fromString;
      case BINARY:
      case VARBINARY:
        return field -> getUTF8Bytes(field);
      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        return field ->
            DecimalData.fromBigDecimal(
                new BigDecimal(field),
                decimalType.getPrecision(),
                decimalType.getScale());
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }
}
