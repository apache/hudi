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

package org.apache.hudi.internal.schema;

import org.apache.hudi.internal.schema.Type.NestedType;
import org.apache.hudi.internal.schema.Type.PrimitiveType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Types supported in schema evolution.
 */
public class Types {
  private Types() {
  }

  /**
   * Boolean primitive type.
   */
  public static class BooleanType extends PrimitiveType {
    private static final BooleanType INSTANCE = new BooleanType();

    public static BooleanType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return Type.TypeID.BOOLEAN;
    }

    @Override
    public String toString() {
      return "boolean";
    }
  }

  /**
   * Integer primitive type.
   */
  public static class IntType extends PrimitiveType {
    private static final IntType INSTANCE = new IntType();

    public static IntType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.INT;
    }

    @Override
    public String toString() {
      return "int";
    }
  }

  /**
   * Long primitive type.
   */
  public static class LongType extends PrimitiveType {
    private static final LongType INSTANCE = new LongType();

    public static LongType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.LONG;
    }

    @Override
    public String toString() {
      return "long";
    }
  }

  /**
   * Float primitive type.
   */
  public static class FloatType extends PrimitiveType {
    private static final FloatType INSTANCE = new FloatType();

    public static FloatType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.FLOAT;
    }

    @Override
    public String toString() {
      return "float";
    }
  }

  /**
   * Double primitive type.
   */
  public static class DoubleType extends PrimitiveType {
    private static final DoubleType INSTANCE = new DoubleType();

    public static DoubleType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.DOUBLE;
    }

    @Override
    public String toString() {
      return "double";
    }
  }

  /**
   * Date primitive type.
   */
  public static class DateType extends PrimitiveType {
    private static final DateType INSTANCE = new DateType();

    public static DateType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.DATE;
    }

    @Override
    public String toString() {
      return "date";
    }
  }

  /**
   * Time primitive type.
   */
  public static class TimeType extends PrimitiveType {
    private static final TimeType INSTANCE = new TimeType();

    public static TimeType get() {
      return INSTANCE;
    }

    private TimeType() {
    }

    @Override
    public TypeID typeId() {
      return TypeID.TIME;
    }

    @Override
    public String toString() {
      return "time";
    }
  }

  /**
   * Time primitive type.
   */
  public static class TimestampType extends PrimitiveType {
    private static final TimestampType INSTANCE = new TimestampType();

    public static TimestampType get() {
      return INSTANCE;
    }

    private TimestampType() {
    }

    @Override
    public TypeID typeId() {
      return TypeID.TIMESTAMP;
    }

    @Override
    public String toString() {
      return "timestamp";
    }
  }

  /**
   * String primitive type.
   */
  public static class StringType extends PrimitiveType {
    private static final StringType INSTANCE = new StringType();

    public static StringType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.STRING;
    }

    @Override
    public String toString() {
      return "string";
    }
  }

  /**
   * Binary primitive type.
   */
  public static class BinaryType extends PrimitiveType {
    private static final BinaryType INSTANCE = new BinaryType();

    public static BinaryType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.BINARY;
    }

    @Override
    public String toString() {
      return "binary";
    }
  }

  /**
   * Fixed primitive type.
   */
  public static class FixedType extends PrimitiveType {
    public static FixedType getFixed(int size) {
      return new FixedType(size);
    }

    private final int size;

    private FixedType(int length) {
      this.size = length;
    }

    public int getFixedSize() {
      return size;
    }

    @Override
    public TypeID typeId() {
      return TypeID.FIXED;
    }

    @Override
    public String toString() {
      return String.format("fixed[%d]", size);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof FixedType)) {
        return false;
      }

      FixedType fixedType = (FixedType) o;
      return size == fixedType.size;
    }

    @Override
    public int hashCode() {
      return Objects.hash(FixedType.class, size);
    }
  }

  public abstract static class DecimalBase extends PrimitiveType {

    protected final int scale;
    protected final int precision;

    protected DecimalBase(int precision, int scale) {
      this.scale = scale;
      this.precision = precision;
    }

    /**
     * Returns whether this DecimalType is wider than `other`. If yes, it means `other`
     * can be casted into `this` safely without losing any precision or range.
     */
    public boolean isWiderThan(PrimitiveType other) {
      if (other instanceof DecimalBase)  {
        DecimalBase dt = (DecimalBase) other;
        return (precision - scale) >= (dt.precision - dt.scale) && scale > dt.scale;
      }
      if (other instanceof IntType) {
        return (precision - scale) >= 10 && scale > 0;
      }
      return false;
    }

    /**
     * Returns whether this DecimalType is tighter than `other`. If yes, it means `this`
     * can be casted into `other` safely without losing any precision or range.
     */
    public boolean isTighterThan(PrimitiveType other) {
      if (other instanceof DecimalBase)  {
        DecimalBase dt = (DecimalBase) other;
        return (precision - scale) <= (dt.precision - dt.scale) && scale <= dt.scale;
      }
      if (other instanceof IntType) {
        return (precision - scale) <= 10 && scale <= 0;
      }
      return false;
    }

    public int scale() {
      return scale;
    }

    public int precision() {
      return precision;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof DecimalBase)) {
        return false;
      }

      DecimalBase that = (DecimalBase) o;
      if (scale != that.scale) {
        return false;
      }
      return precision == that.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.getClass(), scale, precision);
    }
  }

  /**
   * Decimal primitive type.
   */
  public static class DecimalType extends DecimalTypeFixed {
    public static DecimalType get(int precision, int scale) {
      return new DecimalType(precision, scale);
    }

    /**
     * Return the minimum number of bytes needed to store a decimal with a give 'precision'.
     * reference from Spark release 3.1 .
     */
    private static int computeMinBytesForDecimalPrecision(int precision) {
      int numBytes = 1;
      while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
        numBytes += 1;
      }
      return numBytes;
    }

    private DecimalType(int precision, int scale) {
      super(precision, scale, computeMinBytesForDecimalPrecision(precision));
    }

    @Override
    public TypeID typeId() {
      return TypeID.DECIMAL;
    }

    @Override
    public String toString() {
      return String.format("decimal(%d, %d)", precision, scale);
    }
  }

  public static class DecimalTypeBytes extends DecimalBase {
    public static DecimalTypeBytes get(int precision, int scale) {
      return new DecimalTypeBytes(precision, scale);
    }

    private DecimalTypeBytes(int precision, int scale) {
      super(precision, scale);
    }

    @Override
    public TypeID typeId() {
      return TypeID.DECIMAL_BYTES;
    }

    @Override
    public String toString() {
      return String.format("decimal_bytes(%d, %d)", precision, scale);
    }

    @Override
    public boolean equals(Object o) {
      if (super.equals(o)) {
        return o instanceof DecimalTypeBytes;
      }
      return false;
    }
  }

  public static class DecimalTypeFixed extends DecimalBase {
    public static DecimalTypeFixed get(int precision, int scale, int size) {
      return new DecimalTypeFixed(precision, scale, size);
    }

    private final int size;

    public int getFixedSize() {
      return size;
    }

    private DecimalTypeFixed(int precision, int scale, int size) {
      super(precision, scale);
      this.size = size;
    }

    @Override
    public TypeID typeId() {
      return TypeID.DECIMAL_FIXED;
    }

    @Override
    public String toString() {
      return String.format("decimal_fixed(%d, %d)[%d]", precision, scale, size);
    }

    @Override
    public int hashCode() {
      return Objects.hash(DecimalTypeFixed.class, scale, precision, size);
    }

    @Override
    public boolean equals(Object o) {
      if (super.equals(o)) {
        return o instanceof DecimalTypeFixed && ((DecimalTypeFixed) o).size == size;
      }
      return false;
    }
  }

  /**
   * UUID primitive type.
   */
  public static class UUIDType extends PrimitiveType {
    private static final UUIDType INSTANCE = new UUIDType();

    public static UUIDType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.UUID;
    }

    @Override
    public String toString() {
      return "uuid";
    }
  }

  public static class TimeMillisType extends PrimitiveType {
    private static final TimeMillisType INSTANCE = new TimeMillisType();

    public static TimeMillisType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.TIME_MILLIS;
    }

    @Override
    public String toString() {
      return "time-millis";
    }
  }

  public static class TimestampMillisType extends PrimitiveType {
    private static final TimestampMillisType INSTANCE = new TimestampMillisType();

    public static TimestampMillisType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.TIMESTAMP_MILLIS;
    }

    @Override
    public String toString() {
      return "timestamp-millis";
    }
  }

  public static class LocalTimestampMillisType extends PrimitiveType {
    private static final LocalTimestampMillisType INSTANCE = new LocalTimestampMillisType();

    public static LocalTimestampMillisType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.LOCAL_TIMESTAMP_MILLIS;
    }

    @Override
    public String toString() {
      return "local-timestamp-millis";
    }
  }

  public static class LocalTimestampMicrosType extends PrimitiveType {
    private static final LocalTimestampMicrosType INSTANCE = new LocalTimestampMicrosType();

    public static LocalTimestampMicrosType get() {
      return INSTANCE;
    }

    @Override
    public TypeID typeId() {
      return TypeID.LOCAL_TIMESTAMP_MICROS;
    }

    @Override
    public String toString() {
      return "local-timestamp-micros";
    }
  }

  /** A field within a record. */
  public static class Field implements Serializable {
    // Experimental method to support defaultValue
    public static Field get(int id, boolean isOptional, String name, Type type, String doc, Object defaultValue) {
      return new Field(isOptional, id, name, type, doc, defaultValue);
    }

    public static Field get(int id, boolean isOptional, String name, Type type, String doc) {
      return new Field(isOptional, id, name, type, doc, null);
    }

    public static Field get(int id, boolean isOptional, String name, Type type) {
      return new Field(isOptional, id, name, type, null, null);
    }

    public static Field get(int id, String name, Type type) {
      return new Field(true, id, name, type, null, null);
    }

    private final boolean isOptional;
    private final int id;
    private final String name;
    private final Type type;
    private final String doc;
    // Experimental properties
    private final Object defaultValue;

    private Field(boolean isOptional, int id, String name, Type type, String doc, Object defaultValue) {
      this.isOptional = isOptional;
      this.id = id;
      this.name = name;
      this.type = type;
      this.doc = doc;
      this.defaultValue = defaultValue;
    }

    public Object getDefaultValue() {
      return defaultValue;
    }

    public boolean isOptional() {
      return isOptional;
    }

    public int fieldId() {
      return id;
    }

    public String name() {
      return name;
    }

    public Type type() {
      return type;
    }

    public String doc() {
      return doc;
    }

    @Override
    public String toString() {
      return String.format("%d: %s: %s %s",
          id, name, isOptional ? "optional" : "required", type) + (doc != null ? " (" + doc + ")" : "");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof Field)) {
        return false;
      }

      Field that = (Field) o;
      if (isOptional != that.isOptional) {
        return false;
      } else if (id != that.id) {
        return false;
      } else if (!name.equals(that.name)) {
        return false;
      } else if (!Objects.equals(doc, that.doc)) {
        return false;
      }
      return type.equals(that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(Field.class, id, isOptional, name, type);
    }
  }

  /**
   * Record nested type.
   */
  public static class RecordType extends NestedType {
    // NOTE: This field is necessary to provide for lossless conversion b/w Avro and
    //       InternalSchema and back (Avro unfortunately relies not only on structural equivalence of
    //       schemas but also corresponding Record type's "name" when evaluating their compatibility);
    //       This field is nullable
    private final String name;

    private final Field[] fields;

    private transient Map<String, Field> nameToFields = null;
    private transient Map<String, Field> lowercaseNameToFields = null;
    private transient Map<Integer, Field> idToFields = null;

    private RecordType(List<Field> fields, String name) {
      this.name = name;
      this.fields = fields.toArray(new Field[0]);
    }

    @Override
    public List<Field> fields() {
      return Arrays.asList(fields);
    }

    /**
     * Case-sensitive get field by name
     */
    public Field fieldByName(String name) {
      if (nameToFields == null) {
        nameToFields = Arrays.stream(fields)
            .collect(Collectors.toMap(
                Field::name,
                field -> field));
      }
      return nameToFields.get(name);
    }

    public Field fieldByNameCaseInsensitive(String name) {
      if (lowercaseNameToFields == null) {
        lowercaseNameToFields = Arrays.stream(fields)
            .collect(Collectors.toMap(
                field -> field.name.toLowerCase(Locale.ROOT),
                field -> field));
      }
      return lowercaseNameToFields.get(name.toLowerCase(Locale.ROOT));
    }

    @Override
    public Field field(int id) {
      if (idToFields == null) {
        idToFields = Arrays.stream(fields)
            .collect(Collectors.toMap(
                Field::fieldId,
                field -> field));
      }
      return idToFields.get(id);
    }

    @Override
    public Type fieldType(String name) {
      Field field = fieldByNameCaseInsensitive(name);
      if (field != null) {
        return field.type();
      }
      return null;
    }

    public String name() {
      return name;
    }

    @Override
    public TypeID typeId() {
      return TypeID.RECORD;
    }

    @Override
    public String toString() {
      return String.format("Record<%s>", Arrays.stream(fields).map(f -> f.toString()).collect(Collectors.joining("-")));
    }

    @Override
    public boolean equals(Object o) {
      // NOTE: We're not comparing {@code RecordType}'s names here intentionally
      //       relying exclusively on structural equivalence
      if (this == o) {
        return true;
      } else if (!(o instanceof RecordType)) {
        return false;
      }

      RecordType that = (RecordType) o;
      return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
      // NOTE: {@code hashCode} has to match for objects for which {@code equals} returns true,
      //       hence we don't hash the {@code name} in here
      return Objects.hash(Field.class, Arrays.hashCode(fields));
    }

    public static RecordType get(List<Field> fields) {
      return new RecordType(fields, null);
    }

    public static RecordType get(List<Field> fields, String recordName) {
      return new RecordType(fields, recordName);
    }

    public static RecordType get(Field... fields) {
      return new RecordType(Arrays.asList(fields), null);
    }
  }

  /**
   * Array nested type.
   */
  public static class ArrayType extends NestedType {
    public static ArrayType get(int elementId, boolean isOptional, Type elementType) {
      return new ArrayType(Field.get(elementId, isOptional, "element", elementType));
    }

    private final Field elementField;

    private ArrayType(Field elementField) {
      this.elementField = elementField;
    }

    public Type elementType() {
      return elementField.type();
    }

    @Override
    public Type fieldType(String name) {
      if ("element".equals(name)) {
        return elementType();
      }
      return null;
    }

    @Override
    public Field field(int id) {
      if (elementField.fieldId() == id) {
        return elementField;
      }
      return null;
    }

    @Override
    public List<Field> fields() {
      return Collections.singletonList(elementField);
    }

    public int elementId() {
      return elementField.fieldId();
    }

    public boolean isElementOptional() {
      return elementField.isOptional;
    }

    @Override
    public TypeID typeId() {
      return TypeID.ARRAY;
    }

    @Override
    public String toString() {
      return String.format("list<%s>", elementField.type());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof ArrayType)) {
        return false;
      }
      ArrayType listType = (ArrayType) o;
      return elementField.equals(listType.elementField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ArrayType.class, elementField);
    }
  }

  /**
   * Map nested type.
   */
  public static class MapType extends NestedType {

    public static MapType get(int keyId, int valueId, Type keyType, Type valueType) {
      return new MapType(
          Field.get(keyId, "key", keyType),
          Field.get(valueId, "value", valueType));
    }

    public static MapType get(int keyId, int valueId, Type keyType, Type valueType, boolean isOptional) {
      return new MapType(
          Field.get(keyId, isOptional, "key", keyType),
          Field.get(valueId, isOptional, "value", valueType));
    }

    private final Field keyField;
    private final Field valueField;
    private transient List<Field> fields = null;

    private MapType(Field keyField, Field valueField) {
      this.keyField = keyField;
      this.valueField = valueField;
    }

    public Type keyType() {
      return keyField.type();
    }

    public Type valueType() {
      return valueField.type();
    }

    @Override
    public Type fieldType(String name) {
      if ("key".equals(name)) {
        return keyField.type();
      } else if ("value".equals(name)) {
        return valueField.type();
      }
      return null;
    }

    @Override
    public Field field(int id) {
      if (keyField.fieldId() == id) {
        return keyField;
      } else if (valueField.fieldId() == id) {
        return valueField;
      }
      return null;
    }

    @Override
    public List<Field> fields() {
      if (fields == null) {
        fields = Arrays.asList(keyField, valueField);
      }
      return fields;
    }

    public int keyId() {
      return keyField.fieldId();
    }

    public int valueId() {
      return valueField.fieldId();
    }

    public boolean isValueOptional() {
      return valueField.isOptional;
    }

    @Override
    public TypeID typeId() {
      return TypeID.MAP;
    }

    @Override
    public String toString() {
      return String.format("map<%s, %s>", keyField.type(), valueField.type());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof MapType)) {
        return false;
      }

      MapType mapType = (MapType) o;
      if (!keyField.equals(mapType.keyField)) {
        return false;
      }
      return valueField.equals(mapType.valueField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(MapType.class, keyField, valueField);
    }
  }
}
