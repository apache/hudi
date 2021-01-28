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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TypeInformationRawType;

import java.util.List;

/**
 * Converts an Avro schema into Flink's type information. It uses {@link org.apache.flink.api.java.typeutils.RowTypeInfo} for
 * representing objects and converts Avro types into types that are compatible with Flink's Table &
 * SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime classes
 * {@link org.apache.flink.formats.avro.AvroRowDeserializationSchema} and {@link org.apache.flink.formats.avro.AvroRowSerializationSchema}.
 *
 * <p><p>NOTE: reference from Flink release 1.12.0, should remove when Flink version upgrade to that.
 */
public class AvroSchemaConverter {

  /**
   * Converts an Avro schema {@code schema} into a nested row structure with deterministic field order and
   * data types that are compatible with Flink's Table & SQL API.
   *
   * @param schema Avro schema definition
   * @return data type matching the schema
   */
  public static DataType convertToDataType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        final List<Schema.Field> schemaFields = schema.getFields();

        final DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];
        for (int i = 0; i < schemaFields.size(); i++) {
          final Schema.Field field = schemaFields.get(i);
          fields[i] = DataTypes.FIELD(field.name(), convertToDataType(field.schema()));
        }
        return DataTypes.ROW(fields).notNull();
      case ENUM:
        return DataTypes.STRING().notNull();
      case ARRAY:
        return DataTypes.ARRAY(convertToDataType(schema.getElementType())).notNull();
      case MAP:
        return DataTypes.MAP(
            DataTypes.STRING().notNull(),
            convertToDataType(schema.getValueType()))
            .notNull();
      case UNION:
        final Schema actualSchema;
        final boolean nullable;
        if (schema.getTypes().size() == 2
            && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
          actualSchema = schema.getTypes().get(1);
          nullable = true;
        } else if (schema.getTypes().size() == 2
            && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
          actualSchema = schema.getTypes().get(0);
          nullable = true;
        } else if (schema.getTypes().size() == 1) {
          actualSchema = schema.getTypes().get(0);
          nullable = false;
        } else {
          // use Kryo for serialization
          return new AtomicDataType(
              new TypeInformationRawType<>(false, Types.GENERIC(Object.class)));
        }
        DataType converted = convertToDataType(actualSchema);
        return nullable ? converted.nullable() : converted;
      case FIXED:
        // logical decimal type
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
          final LogicalTypes.Decimal decimalType =
              (LogicalTypes.Decimal) schema.getLogicalType();
          return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale())
              .notNull();
        }
        // convert fixed size binary data to primitive byte arrays
        return DataTypes.VARBINARY(schema.getFixedSize()).notNull();
      case STRING:
        // convert Avro's Utf8/CharSequence to String
        return DataTypes.STRING().notNull();
      case BYTES:
        // logical decimal type
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
          final LogicalTypes.Decimal decimalType =
              (LogicalTypes.Decimal) schema.getLogicalType();
          return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale())
              .notNull();
        }
        return DataTypes.BYTES().notNull();
      case INT:
        // logical date and time type
        final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.date()) {
          return DataTypes.DATE().notNull();
        } else if (logicalType == LogicalTypes.timeMillis()) {
          return DataTypes.TIME(3).notNull();
        }
        return DataTypes.INT().notNull();
      case LONG:
        // logical timestamp type
        if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
          return DataTypes.TIMESTAMP(3).notNull();
        } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
          return DataTypes.TIMESTAMP(6).notNull();
        } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
          return DataTypes.TIME(3).notNull();
        } else if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
          return DataTypes.TIME(6).notNull();
        }
        return DataTypes.BIGINT().notNull();
      case FLOAT:
        return DataTypes.FLOAT().notNull();
      case DOUBLE:
        return DataTypes.DOUBLE().notNull();
      case BOOLEAN:
        return DataTypes.BOOLEAN().notNull();
      case NULL:
        return DataTypes.NULL();
      default:
        throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
    }
  }
}

