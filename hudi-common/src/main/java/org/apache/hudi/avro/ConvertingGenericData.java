/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Custom instance of the {@link GenericData} model incorporating conversions from the
 * common Avro logical types like "decimal", "uuid", "date", "time-micros", "timestamp-micros"
 *
 * NOTE: Given that this code has to be interoperable w/ Spark 2 (which relies on Avro 1.8.2)
 *       this model can't support newer conversion introduced in Avro 1.10 at the moment
 */
public class ConvertingGenericData extends GenericData {

  private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();
  private static final Conversions.UUIDConversion UUID_CONVERSION = new Conversions.UUIDConversion();
  private static final TimeConversions.DateConversion DATE_CONVERSION = new TimeConversions.DateConversion();
  private static final TimeConversions.TimeMicrosConversion TIME_MICROS_CONVERSION = new TimeConversions.TimeMicrosConversion();
  private static final TimeConversions.TimestampMicrosConversion TIMESTAMP_MICROS_CONVERSION = new TimeConversions.TimestampMicrosConversion();

  private static final TimeConversions.TimestampMillisConversion TIMESTAMP_MILLIS_CONVERSION = new TimeConversions.TimestampMillisConversion();
  private static final TimeConversions.TimeMillisConversion TIME_MILLIS_CONVERSION = new TimeConversions.TimeMillisConversion();
  private static final TimeConversions.LocalTimestampMillisConversion LOCAL_TIMESTAMP_MILLIS_CONVERSION = new TimeConversions.LocalTimestampMillisConversion();
  private static final TimeConversions.LocalTimestampMicrosConversion LOCAL_TIMESTAMP_MICROS_CONVERSION = new TimeConversions.LocalTimestampMicrosConversion();

  public static final GenericData INSTANCE = new ConvertingGenericData();

  private ConvertingGenericData() {
    addLogicalTypeConversion(DECIMAL_CONVERSION);
    addLogicalTypeConversion(UUID_CONVERSION);
    addLogicalTypeConversion(DATE_CONVERSION);
    addLogicalTypeConversion(TIME_MICROS_CONVERSION);
    addLogicalTypeConversion(TIMESTAMP_MICROS_CONVERSION);
    // NOTE: Those are not supported in Avro 1.8.2
    addLogicalTypeConversion(TIME_MILLIS_CONVERSION);
    addLogicalTypeConversion(TIMESTAMP_MILLIS_CONVERSION);
    addLogicalTypeConversion(LOCAL_TIMESTAMP_MILLIS_CONVERSION);
    addLogicalTypeConversion(LOCAL_TIMESTAMP_MICROS_CONVERSION);
  }

  @Override
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
      case RECORD:
        if (!isRecord(datum)) {
          return false;
        }
        for (Schema.Field f : schema.getFields()) {
          if (!validate(f.schema(), getField(datum, f.name(), f.pos()))) {
            return false;
          }
        }
        return true;
      case ENUM:
        if (!isEnum(datum)) {
          return false;
        }
        return schema.getEnumSymbols().contains(datum.toString());
      case ARRAY:
        if (!(isArray(datum))) {
          return false;
        }
        for (Object element : getArrayAsCollection(datum)) {
          if (!validate(schema.getElementType(), element)) {
            return false;
          }
        }
        return true;
      case MAP:
        if (!(isMap(datum))) {
          return false;
        }
        @SuppressWarnings(value = "unchecked")
        Map<Object, Object> map = (Map<Object, Object>) datum;
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          if (!validate(schema.getValueType(), entry.getValue())) {
            return false;
          }
        }
        return true;
      case UNION:
        try {
          int i = resolveUnion(schema, datum);
          return validate(schema.getTypes().get(i), datum);
        } catch (UnresolvedUnionException e) {
          return false;
        }
      case FIXED:
        return (datum instanceof GenericFixed && ((GenericFixed) datum).bytes().length == schema.getFixedSize())
            || DECIMAL_CONVERSION.getConvertedType().isInstance(datum);
      case STRING:
        return isString(datum)
            || UUID_CONVERSION.getConvertedType().isInstance(datum);
      case BYTES:
        return isBytes(datum)
            || DECIMAL_CONVERSION.getConvertedType().isInstance(datum);
      case INT:
        return isInteger(datum)
            || DATE_CONVERSION.getConvertedType().isInstance(datum);
      case LONG:
        return isLong(datum)
            || TIME_MICROS_CONVERSION.getConvertedType().isInstance(datum)
            || TIMESTAMP_MICROS_CONVERSION.getConvertedType().isInstance(datum)
            || TIMESTAMP_MILLIS_CONVERSION.getConvertedType().isInstance(datum)
            || LOCAL_TIMESTAMP_MICROS_CONVERSION.getConvertedType().isInstance(datum)
            || LOCAL_TIMESTAMP_MILLIS_CONVERSION.getConvertedType().isInstance(datum);
      case FLOAT:
        return isFloat(datum);
      case DOUBLE:
        return isDouble(datum);
      case BOOLEAN:
        return isBoolean(datum);
      case NULL:
        return datum == null;
      default:
        return false;
    }
  }

  @Override
  public int compare(Object o1, Object o2, Schema s) {
    // Handle byte[] by wrapping first in ByteBuffer because byte array is not comparable
    if (s.getType() == Schema.Type.BYTES && o1 instanceof byte[] && o2 instanceof byte[]) {
      return super.compare(ByteBuffer.wrap((byte[]) o1), ByteBuffer.wrap((byte[]) o2), s);
    }
    return super.compare(o1, o2, s);
  }
}

