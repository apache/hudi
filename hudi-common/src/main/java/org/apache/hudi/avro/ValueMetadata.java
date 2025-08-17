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

import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.LogicalTypeTokenParser;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL;

public class ValueMetadata {

  public static ValueMetadata getEmptyValueMetadata(HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return ValueMetadata.NoneMetadata.INSTANCE;
    }
    return NULL_METADATA;
  }

  public static ValueMetadata getValueMetadata(HoodieValueTypeInfo valueTypeInfo) {
    // valueTypeInfo will always be null when version is v1
    if (valueTypeInfo == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromInt(valueTypeInfo.getTypeOrdinal());
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(valueTypeInfo.getAdditionalInfo());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(GenericRecord columnStatsRecord) {
    if (columnStatsRecord == null) {
      return NoneMetadata.INSTANCE;
    }

    GenericRecord valueTypeInfo = (GenericRecord) columnStatsRecord.get(COLUMN_STATS_FIELD_VALUE_TYPE);
    if (valueTypeInfo == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromInt((Integer) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL));
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((String) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO));
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(Schema fieldSchema, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return NoneMetadata.INSTANCE;
    }
    if (fieldSchema == null) {
      return NULL_METADATA;
    }
    Schema valueSchema = resolveNullableSchema(fieldSchema);
    ValueType valueType = ValueType.fromSchema(valueSchema);
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((LogicalTypes.Decimal) valueSchema.getLogicalType());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(PrimitiveType primitiveType, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return NoneMetadata.INSTANCE;
    }
    if (primitiveType == null) {
      return NoneMetadata.INSTANCE;
    }

    ValueType valueType = ValueType.fromPrimitiveType(primitiveType);
    if (valueType == ValueType.NONE) {
      return NoneMetadata.INSTANCE;
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(primitiveType);
    } else {
      return new ValueMetadata(valueType);
    }
  }

  private final ValueType valueType;

  private ValueMetadata(ValueType valueType) {
    this.valueType = valueType;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public HoodieValueTypeInfo getValueTypeInfo() {
    return new HoodieValueTypeInfo(valueType.ordinal(), getAdditionalInfo());
  }

  public String getAdditionalInfo() {
    return null;
  }

  public Comparable<?> standardizeJavaTypeAndPromote(Object val) {
    return this.getValueType().standardizeJavaTypeAndPromote(val, this);
  }

  public Comparable<?> convertIntoPrimitive(Comparable<?> value) {
    return this.getValueType().convertIntoPrimitive(value, this);
  }

  public Comparable<?> convertIntoComplex(Comparable<?> value) {
    return this.getValueType().convertIntoComplex(value, this);
  }

  public void validate(Object minVal, Object maxVal, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      if (getValueType() != ValueType.NONE) {
        throw new IllegalArgumentException("Value type should be NONE");
      }
    }
    if (getValueType() == ValueType.NONE) {
      throw new IllegalArgumentException("Value type should not be NONE");
    }
    this.getValueType().validate(minVal);
    this.getValueType().validate(maxVal);
  }

  public static class NoneMetadata extends ValueMetadata {
    public static final ValueMetadata INSTANCE = new NoneMetadata();
    private NoneMetadata() {
      super(ValueType.NONE);
    }

    @Override
    public HoodieValueTypeInfo getValueTypeInfo() {
      return null;
    }
  }

  public static final ValueMetadata NULL_METADATA = new ValueMetadata(ValueType.NULL);

  public static class DecimalMetadata extends ValueMetadata {

    public static DecimalMetadata create(String additionalInfo) {
      if (additionalInfo == null) {
        throw new IllegalArgumentException("additionalInfo cannot be null");
      }
      //TODO: decide if we want to store things in a better way
      String[] splits = additionalInfo.split(",");
      return new DecimalMetadata(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
    }

    public static DecimalMetadata create(LogicalTypes.Decimal decimal) {
      return new DecimalMetadata(decimal.getPrecision(), decimal.getScale());
    }

    public static DecimalMetadata create(PrimitiveType primitiveType) {
      return new DecimalMetadata(LogicalTypeTokenParser.getPrecision(primitiveType), LogicalTypeTokenParser.getScale(primitiveType));
    }

    private final int precision;
    private final int scale;

    private DecimalMetadata(int precision, int scale) {
      super(ValueType.DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    public String getAdditionalInfo() {
      return String.format("%d,%d", precision, scale);
    }
  }
}