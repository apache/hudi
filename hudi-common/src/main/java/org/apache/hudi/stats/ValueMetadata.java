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

package org.apache.hudi.stats;

import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.LogicalTypeTokenParser;
import org.apache.parquet.schema.PrimitiveType;

import java.io.Serializable;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL;

public class ValueMetadata implements Serializable {
  private final ValueType valueType;

  protected ValueMetadata(ValueType valueType) {
    this.valueType = valueType;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public HoodieValueTypeInfo getValueTypeInfo() {
    return HoodieValueTypeInfo.newBuilder()
        .setTypeOrdinal(valueType.ordinal())
        .setAdditionalInfo(getAdditionalInfo())
        .build();
  }

  public String getAdditionalInfo() {
    return null;
  }

  public Comparable<?> standardizeJavaTypeAndPromote(Object val) {
    return this.getValueType().standardizeJavaTypeAndPromote(val, this);
  }

  public Object wrapValue(Comparable<?> value) {
    return this.getValueType().wrapValue(value, this);
  }

  public Comparable<?> unwrapValue(Object value) {
    return this.getValueType().unwrapValue(value, this);
  }

  public void validate(Object minVal, Object maxVal) {
    if (getValueType() == ValueType.V1) {
      return;
    }
    this.getValueType().validate(minVal);
    this.getValueType().validate(maxVal);
  }

  public static class V1EmptyMetadata extends ValueMetadata {
    private static final V1EmptyMetadata V_1_EMPTY_METADATA = new V1EmptyMetadata();
    public static V1EmptyMetadata get() {
      return V_1_EMPTY_METADATA;
    }

    private V1EmptyMetadata() {
      super(ValueType.V1);
    }

    @Override
    public HoodieValueTypeInfo getValueTypeInfo() {
      // V1 should never be persisted to the MDT. It is only for in memory
      return null;
    }
  }

  public static final ValueMetadata NULL_METADATA = new ValueMetadata(ValueType.NULL);

  public interface DecimalValueMetadata {

    int getPrecision();

    int getScale();

    static String encodeData(DecimalValueMetadata decimalValueMetadata) {
      return String.format("%d,%d", decimalValueMetadata.getPrecision(), decimalValueMetadata.getScale());
    }

    static Pair<Integer, Integer> decodeData(String data) {
      //TODO: decide if we want to store things in a better way
      String[] splits = data.split(",");
      return Pair.of(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
    }
  }

  protected static class DecimalMetadata extends ValueMetadata implements DecimalValueMetadata {

    public static DecimalMetadata create(String additionalInfo) {
      if (additionalInfo == null) {
        throw new IllegalArgumentException("additionalInfo cannot be null");
      }
      Pair<Integer, Integer> data = DecimalValueMetadata.decodeData(additionalInfo);
      return new DecimalMetadata(data.getLeft(), data.getRight());
    }

    public static DecimalMetadata create(LogicalTypes.Decimal decimal) {
      return new DecimalMetadata(decimal.getPrecision(), decimal.getScale());
    }

    public static DecimalMetadata create(PrimitiveType primitiveType) {
      return new DecimalMetadata(LogicalTypeTokenParser.getPrecision(primitiveType), LogicalTypeTokenParser.getScale(primitiveType));
    }

    public static DecimalMetadata create(int precision, int scale) {
      return new DecimalMetadata(precision, scale);
    }

    private final int precision;
    private final int scale;

    private DecimalMetadata(int precision, int scale) {
      super(ValueType.DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public int getPrecision() {
      return precision;
    }

    @Override
    public int getScale() {
      return scale;
    }

    @Override
    public String getAdditionalInfo() {
      return DecimalValueMetadata.encodeData(this);
    }
  }

  public static ValueMetadata getEmptyValueMetadata(HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return V1EmptyMetadata.get();
    }
    return NULL_METADATA;
  }

  public static ValueMetadata getValueMetadata(HoodieValueTypeInfo valueTypeInfo) {
    // valueTypeInfo will always be null when version is v1
    if (valueTypeInfo == null) {
      return V1EmptyMetadata.get();
    }

    ValueType valueType = ValueType.fromInt(valueTypeInfo.getTypeOrdinal());
    if (valueType == ValueType.V1) {
      return V1EmptyMetadata.get();
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(valueTypeInfo.getAdditionalInfo());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(GenericRecord columnStatsRecord) {
    if (columnStatsRecord == null) {
      // TODO: Should we return V1EmptyMetadata here?
      return NULL_METADATA;
    }

    GenericRecord valueTypeInfo = (GenericRecord) columnStatsRecord.get(COLUMN_STATS_FIELD_VALUE_TYPE);
    if (valueTypeInfo == null) {
      return V1EmptyMetadata.get();
    }

    ValueType valueType = ValueType.fromInt((Integer) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL));
    if (valueType == ValueType.V1) {
      throw new IllegalArgumentException("Unsupported value type: " + valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL));
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((String) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO));
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(Schema fieldSchema, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return V1EmptyMetadata.get();
    }
    if (fieldSchema == null) {
      return NULL_METADATA;
    }
    Schema valueSchema = resolveNullableSchema(fieldSchema);
    ValueType valueType = ValueType.fromSchema(valueSchema);
    if (valueType == ValueType.V1) {
      throw new IllegalArgumentException("Unsupported logical type for: " + valueSchema.getLogicalType());
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((LogicalTypes.Decimal) valueSchema.getLogicalType());
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(PrimitiveType primitiveType, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return V1EmptyMetadata.get();
    }
    if (primitiveType == null) {
      return NULL_METADATA;
    }
    ValueType valueType = ValueType.fromPrimitiveType(primitiveType);
    if (valueType == ValueType.V1) {
      throw new IllegalArgumentException("Unsupported logical type: " + primitiveType.getLogicalTypeAnnotation());
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(primitiveType);
    } else {
      return new ValueMetadata(valueType);
    }
  }
}