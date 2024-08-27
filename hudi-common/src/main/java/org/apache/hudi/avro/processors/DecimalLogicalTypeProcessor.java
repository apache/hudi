/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro.processors;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Base64;

public abstract class DecimalLogicalTypeProcessor extends JsonFieldProcessor {

  /**
   * Check if the given schema is a valid decimal type configuration.
   */
  protected static boolean isValidDecimalTypeConfig(Schema schema) {
    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
    // At the time when the schema is found not valid when it is parsed, the Avro Schema.parse will just silently
    // set the schema to be null instead of throwing exceptions. Correspondingly, we just check if it is null here.
    if (decimalType == null) {
      return false;
    }
    // Even though schema is validated at schema parsing phase, still validate here to be defensive.
    decimalType.validate(schema);
    return true;
  }

  /**
   * Parse the object to BigDecimal.
   *
   * @param obj Object to be parsed
   * @return Pair object, with left as boolean indicating if the parsing was successful and right as the
   * BigDecimal value.
   */
  protected static Pair<Boolean, BigDecimal> parseObjectToBigDecimal(Object obj, Schema schema) {
    LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) schema.getLogicalType();
    try {
      if (obj instanceof BigDecimal) {
        return Pair.of(true, ((BigDecimal) obj).setScale(logicalType.getScale(), RoundingMode.UNNECESSARY));
      }
      if (schema.getType() == Schema.Type.BYTES && (obj instanceof String)) {
        try {
          //encoded big decimal
          return Pair.of(true, HoodieAvroUtils.convertBytesToBigDecimal(decodeStringToBigDecimalBytes(obj), logicalType));
        } catch (IllegalArgumentException e) {
          //no-op
        }
      }
      BigDecimal bigDecimal = new BigDecimal(obj.toString(), new MathContext(logicalType.getPrecision(), RoundingMode.UNNECESSARY)).setScale(logicalType.getScale(), RoundingMode.UNNECESSARY);
      return Pair.of(true, bigDecimal);
    } catch (java.lang.NumberFormatException | ArithmeticException ignored) {
      /* ignore */
    }
    return Pair.of(false, null);
  }

  protected static byte[] decodeStringToBigDecimalBytes(Object value) {
    return Base64.getDecoder().decode(((String) value).getBytes());
  }
}
