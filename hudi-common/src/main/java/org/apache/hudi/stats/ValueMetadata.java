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

import org.apache.hudi.ParquetAdapter;
import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieIndexVersion;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.PrimitiveType;

import java.io.Serializable;

import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO;
import static org.apache.hudi.metadata.HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL;

/**
 * Holder for VaueType and additional info
 * Used for wrapping and unwrapping col stat values
 * as well as for type promotion
 */
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class ValueMetadata implements Serializable {

  private static final ParquetAdapter PARQUET_ADAPTER = ParquetAdapter.getAdapter();

  private final ValueType valueType;

  public HoodieValueTypeInfo getValueTypeInfo() {
    return HoodieValueTypeInfo.newBuilder()
        .setTypeOrdinal(valueType.ordinal())
        .setAdditionalInfo(getAdditionalInfo())
        .build();
  }

  String getAdditionalInfo() {
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

  public boolean isV1() {
    return this.getValueType() == ValueType.V1;
  }

  public static class V1EmptyMetadata extends ValueMetadata {
    private static final V1EmptyMetadata V1_EMPTY_METADATA = new V1EmptyMetadata();
    public static V1EmptyMetadata get() {
      return V1_EMPTY_METADATA;
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

  /**
   * decimal is encoded as a string in the format "precision,scale" in the extra info field
   */
  interface DecimalValueMetadata {

    int getPrecision();

    int getScale();

    /**
     * Do not change how we encode decimal without
     * handling upgrade/downgrade and backwards compatibility
     */
    static String encodeData(DecimalValueMetadata decimalValueMetadata) {
      return String.format("%d,%d", decimalValueMetadata.getPrecision(), decimalValueMetadata.getScale());
    }

    /**
     * Do not change how we encode decimal without
     * handling upgrade/downgrade and backwards compatibility
     */
    static Pair<Integer, Integer> decodeData(String data) {
      String[] splits = data.split(",");
      return Pair.of(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
    }
  }

  static class DecimalMetadata extends ValueMetadata implements DecimalValueMetadata {

    static DecimalMetadata create(String additionalInfo) {
      if (additionalInfo == null) {
        throw new IllegalArgumentException("additionalInfo cannot be null");
      }
      Pair<Integer, Integer> data = DecimalValueMetadata.decodeData(additionalInfo);
      return new DecimalMetadata(data.getLeft(), data.getRight());
    }

    static DecimalMetadata create(HoodieSchema.Decimal decimal) {
      return new DecimalMetadata(decimal.getPrecision(), decimal.getScale());
    }

    static DecimalMetadata create(PrimitiveType primitiveType) {
      return new DecimalMetadata(PARQUET_ADAPTER.getPrecision(primitiveType), PARQUET_ADAPTER.getScale(primitiveType));
    }

    static DecimalMetadata create(int precision, int scale) {
      return new DecimalMetadata(precision, scale);
    }

    @Getter
    private final int precision;
    @Getter
    private final int scale;

    private DecimalMetadata(int precision, int scale) {
      super(ValueType.DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    String getAdditionalInfo() {
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

    ValueType valueType = ValueType.fromOrdinal(valueTypeInfo.getTypeOrdinal());
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
      // NOTE: Only legitimate reason for {@code ColumnStatsMetadata} to not be present is when
      //       it's not been read from the storage (ie it's not been a part of projected schema).
      //       Otherwise, it has to be present or the record would be considered invalid
      throw new IllegalStateException("ColumnStatsMetadata is null. Handling should happen in the caller.");
    }

    // This may happen when the record is from old table versions.
    if (!columnStatsRecord.hasField(COLUMN_STATS_FIELD_VALUE_TYPE)) {
      return V1EmptyMetadata.get();
    }

    GenericRecord valueTypeInfo = (GenericRecord) columnStatsRecord.get(COLUMN_STATS_FIELD_VALUE_TYPE);
    if (valueTypeInfo == null) {
      return V1EmptyMetadata.get();
    }

    ValueType valueType = ValueType.fromOrdinal((Integer) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL));
    if (valueType == ValueType.V1) {
      throw new IllegalArgumentException("Unsupported value type: " + valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ORDINAL));
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((String) valueTypeInfo.get(COLUMN_STATS_FIELD_VALUE_TYPE_ADDITIONAL_INFO));
    } else {
      return new ValueMetadata(valueType);
    }
  }

  /**
   * Creates ValueMetadata from HoodieSchema for column statistics type inference.
   * This method uses HoodieSchema for in-memory processing while maintaining
   * compatibility with existing Avro-based serialization.
   *
   * @param fieldSchema the HoodieSchema of the field
   * @param indexVersion the index version to determine metadata format
   * @return ValueMetadata instance for the given schema
   * @throws IllegalArgumentException if schema is null or has unsupported logical type
   * @since 1.2.0
   */
  public static ValueMetadata getValueMetadata(HoodieSchema fieldSchema, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return V1EmptyMetadata.get();
    }
    if (fieldSchema == null) {
      throw new IllegalArgumentException("Field schema cannot be null");
    }
    HoodieSchema valueSchema = HoodieSchemaUtils.getNonNullTypeFromUnion(fieldSchema);
    ValueType valueType = ValueType.fromSchema(valueSchema);
    if (valueType == ValueType.V1) {
      throw new IllegalArgumentException("Unsupported logical type for: " + valueSchema.getType());
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create((HoodieSchema.Decimal) valueSchema);
    } else {
      return new ValueMetadata(valueType);
    }
  }

  public static ValueMetadata getValueMetadata(PrimitiveType primitiveType, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return V1EmptyMetadata.get();
    }
    if (primitiveType == null) {
      throw new IllegalArgumentException("Primitive type cannot be null");
    }
    ValueType valueType = ValueType.fromParquetPrimitiveType(primitiveType);
    if (valueType == ValueType.V1) {
      throw new IllegalStateException("Returned ValueType should never be V1 here. Primitive type: " + primitiveType);
    } else if (valueType == ValueType.DECIMAL) {
      return DecimalMetadata.create(primitiveType);
    } else {
      return new ValueMetadata(valueType);
    }
  }
}