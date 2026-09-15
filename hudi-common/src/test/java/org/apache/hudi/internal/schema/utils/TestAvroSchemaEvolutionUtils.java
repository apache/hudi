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

package org.apache.hudi.internal.schema.utils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.schema.HoodieJsonProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.InternalSchemaBuilder;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.convert.InternalSchemaConverter;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link AvroSchemaEvolutionUtils}.
 */
public class TestAvroSchemaEvolutionUtils {

  String schemaStr = "{\"type\":\"record\",\"name\":\"newTableName\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"data\","
      + "\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"preferences\",\"type\":[\"null\","
      + "{\"type\":\"record\",\"name\":\"preferences\",\"namespace\":\"newTableName\",\"fields\":[{\"name\":\"feature1\","
      + "\"type\":\"boolean\"},{\"name\":\"feature2\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}],"
      + "\"default\":null},{\"name\":\"locations\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\","
      + "\"name\":\"locations\",\"namespace\":\"newTableName\",\"fields\":[{\"name\":\"lat\",\"type\":\"float\"},{\"name\":\"long\","
      + "\"type\":\"float\"}]}}},{\"name\":\"points\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\","
      + "{\"type\":\"record\",\"name\":\"points\",\"namespace\":\"newTableName\",\"fields\":[{\"name\":\"x\",\"type\":\"long\"},"
      + "{\"name\":\"y\",\"type\":\"long\"}]}]}],\"default\":null},{\"name\":\"doubles\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},"
      + "{\"name\":\"properties\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"string\"]}],\"default\":null}]}";

  @Test
  public void testPrimitiveTypes() {
    HoodieSchema[] schemaPrimitives = new HoodieSchema[] {
        HoodieSchema.create(HoodieSchemaType.BOOLEAN),
        HoodieSchema.create(HoodieSchemaType.INT),
        HoodieSchema.create(HoodieSchemaType.LONG),
        HoodieSchema.create(HoodieSchemaType.FLOAT),
        HoodieSchema.create(HoodieSchemaType.DOUBLE),
        HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
        HoodieSchema.fromAvroSchema(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))),
        HoodieSchema.fromAvroSchema(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))),
        HoodieSchema.create(HoodieSchemaType.STRING),
        HoodieSchema.createUUID(),
        HoodieSchema.createFixed("t1.fixed", null, null, 12),
        HoodieSchema.create(HoodieSchemaType.BYTES),
        HoodieSchema.createDecimal("t1.fixed", null, null, 9, 4, 4)
    };

    Type[] primitiveTypes = new Type[] {
        Types.BooleanType.get(),
        Types.IntType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.get(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.getFixed(12),
        Types.BinaryType.get(),
        Types.DecimalType.get(9, 4)
    };

    for (int i = 0; i < primitiveTypes.length; i++) {
      Type convertPrimitiveResult = InternalSchemaConverter.convertToField(schemaPrimitives[i]);
      Assertions.assertEquals(convertPrimitiveResult, primitiveTypes[i]);
      HoodieSchema convertResult = InternalSchemaConverter.convert(primitiveTypes[i], "t1");
      Assertions.assertEquals(convertResult, schemaPrimitives[i]);
    }
  }

  @Test
  public void testRecordAndPrimitiveTypes() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
        Types.Field.get(2, "long", Types.LongType.get()),
        Types.Field.get(3, "float", Types.FloatType.get()),
        Types.Field.get(4, "double", Types.DoubleType.get()),
        Types.Field.get(5, "date", Types.DateType.get()),
        Types.Field.get(6, "time", Types.TimeType.get()),
        Types.Field.get(7, "timestamp", Types.TimestampType.get()),
        Types.Field.get(8, "string", Types.StringType.get()),
        Types.Field.get(9, "uuid", Types.UUIDType.get()),
        Types.Field.get(10, "fixed", Types.FixedType.getFixed(10)),
        Types.Field.get(11, "binary", Types.BinaryType.get()),
        Types.Field.get(12, "decimal", Types.DecimalType.get(10, 2))
    }));

    HoodieSchema schema = create("t1",
        HoodieSchemaField.of("bool", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("int", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("long", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("float", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.FLOAT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("double", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.DOUBLE)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("date", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))),
            null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("time", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)))),
            null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("timestamp", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))),
            null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("string", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("uuid", HoodieSchema.createNullable(HoodieSchema.createUUID()), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("fixed", HoodieSchema.createNullable(HoodieSchema.createFixed("t1.fixed.fixed", null, null, 10)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("binary", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BYTES)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("decimal", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.decimal(10, 2)
            .addToSchema(Schema.createFixed("t1.decimal.fixed", null, null, 5)))), null, HoodieJsonProperties.NULL_VALUE));
    HoodieSchema convertedSchema = InternalSchemaConverter.convert(record, "t1");
    Assertions.assertEquals(convertedSchema, schema);
    Types.RecordType convertedRecord = InternalSchemaConverter.convert(schema).getRecord();
    Assertions.assertEquals(convertedRecord, record);
  }

  private HoodieSchema create(String name, HoodieSchemaField... fields) {
    return HoodieSchema.createRecord(name, null, null, false, Arrays.asList(fields));
  }

  @Test
  public void testArrayType() {
    Type arrayNestRecordType = Types.ArrayType.get(0, false,
        Types.RecordType.get(Arrays.asList(Types.Field.get(1, false, "a", Types.FloatType.get()),
            Types.Field.get(2, false, "b", Types.FloatType.get()))));

    HoodieSchema schema = HoodieSchema.createArray(create("t1",
        HoodieSchemaField.of("a", HoodieSchema.create(HoodieSchemaType.FLOAT), null, null),
        HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.FLOAT), null, null)));
    HoodieSchema convertedSchema = InternalSchemaConverter.convert(arrayNestRecordType, "t1");
    Assertions.assertEquals(convertedSchema, schema);
    Types.ArrayType convertedRecord = (Types.ArrayType) InternalSchemaConverter.convertToField(schema);
    Assertions.assertEquals(convertedRecord, arrayNestRecordType);
  }

  @Test
  public void testComplexConvert() {
    HoodieSchema schema = HoodieSchema.parse(schemaStr);

    Types.RecordType recordType = Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(7, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(8, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(11, false, "lat", Types.FloatType.get()), Types.Field.get(12, false, "long", Types.FloatType.get())), false)),
        Types.Field.get(4, true, "points", Types.ArrayType.get(13, true,
            Types.RecordType.get(Types.Field.get(14, false, "x", Types.LongType.get()), Types.Field.get(15, false, "y", Types.LongType.get())))),
        Types.Field.get(5, false, "doubles", Types.ArrayType.get(16, false, Types.DoubleType.get())),
        Types.Field.get(6, true, "properties", Types.MapType.get(17, 18, Types.StringType.get(), Types.StringType.get()))
    );
    InternalSchema internalSchema = new InternalSchema(recordType);

    Type convertRecord = InternalSchemaConverter.convert(schema).getRecord();
    Assertions.assertEquals(convertRecord, internalSchema.getRecord());
    Assertions.assertEquals(schema, InternalSchemaConverter.convert(internalSchema, "newTableName"));
  }

  @Test
  public void testNullFieldType() {
    HoodieSchema schema = create("t1",
        HoodieSchemaField.of("nullField", HoodieSchema.create(HoodieSchemaType.NULL), null, HoodieJsonProperties.NULL_VALUE));
    Throwable t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> InternalSchemaConverter.convert(schema));
    assertTrue(t.getMessage().contains("'t1.nullField'"));

    HoodieSchema schemaArray = create("t2",
        HoodieSchemaField.of("nullArray", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.NULL)), null, null));
    t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> InternalSchemaConverter.convert(schemaArray));
    assertTrue(t.getMessage().contains("'t2.nullArray.element'"));

    HoodieSchema schemaMap = create("t3",
        HoodieSchemaField.of("nullMap", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.NULL)), null, null));
    t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> InternalSchemaConverter.convert(schemaMap));
    assertTrue(t.getMessage().contains("'t3.nullMap.value'"));


    HoodieSchema schemaComplex = create("t4",
        HoodieSchemaField.of("complexField", HoodieSchema.createMap(
            create("nestedStruct",
                HoodieSchemaField.of("nestedArray", HoodieSchema.createArray(HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.NULL))),
                    null, null))), null, null));
    t = assertThrows(HoodieNullSchemaTypeException.class,
        () -> InternalSchemaConverter.convert(schemaComplex));
    assertTrue(t.getMessage().contains("'t4.nestedStruct.nestedArray.element.value'"));
  }

  @Test
  public void testRefreshNewId() {
    Types.RecordType record = Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(4, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(5, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false, "locations", Types.MapType.get(6, 7, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(8, false, "lat", Types.FloatType.get()), Types.Field.get(9, false, "long", Types.FloatType.get())), false))
    );
    AtomicInteger newId = new AtomicInteger(100);
    Types.RecordType recordWithNewId = (Types.RecordType) InternalSchemaBuilder.getBuilder().refreshNewId(record, newId);

    Types.RecordType newRecord = Types.RecordType.get(Types.Field.get(100, false, "id", Types.IntType.get()),
        Types.Field.get(101, true, "data", Types.StringType.get()),
        Types.Field.get(102, true, "preferences",
            Types.RecordType.get(Types.Field.get(104, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(105, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(103, false, "locations", Types.MapType.get(106, 107, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(108, false, "lat", Types.FloatType.get()), Types.Field.get(109, false, "long", Types.FloatType.get())), false))
    );
    Assertions.assertEquals(newRecord, recordWithNewId);
  }

  @Test
  public void testFixNullOrdering() {
    HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(TestAvroSchemaEvolutionUtils.class, "/nullWrong.avsc");
    HoodieSchema expectedSchema = SchemaTestUtil.getSchemaFromResource(TestAvroSchemaEvolutionUtils.class, "/nullRight.avsc");
    Assertions.assertEquals(expectedSchema, InternalSchemaConverter.fixNullOrdering(schema));
    Assertions.assertEquals(expectedSchema, InternalSchemaConverter.fixNullOrdering(expectedSchema));
  }

  @Test
  public void testFixNullOrderingSameSchemaCheck() {
    HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(TestAvroSchemaEvolutionUtils.class, "/source_evolved.avsc");
    Assertions.assertEquals(schema, InternalSchemaConverter.fixNullOrdering(schema));
  }

  public enum Enum {
    ENUM1, ENUM2
  }

  /**
   * test record data type changes.
   * int => long/float/double/string
   * long => float/double/string
   * float => double/String
   * double => String/Decimal
   * Decimal => Decimal/String
   * String => date/decimal
   * date => String
   * enum => String
   */
  @Test
  public void testReWriteRecordWithTypeChanged() {
    String enumSchema = "{\"type\":\"enum\",\"name\":\"Enum\",\"namespace\":\"org.apache.hudi.internal.schema.utils.TestAvroSchemaEvolutionUtils\",\"symbols\":[\"ENUM1\",\"ENUM2\"]}";
    HoodieSchema hoodieSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"h0_record\",\"namespace\":\"hoodie.h0\",\"fields\""
        + ":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"comb\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"com1\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"col0\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"col1\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"col11\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"col12\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"col2\",\"type\":[\"null\",\"float\"],\"default\":null},"
        + "{\"name\":\"col21\",\"type\":[\"null\",\"float\"],\"default\":null},"
        + "{\"name\":\"col3\",\"type\":[\"null\",\"double\"],\"default\":null},"
        + "{\"name\":\"col31\",\"type\":[\"null\",\"double\"],\"default\":null},"
        + "{\"name\":\"col4\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col4\","
        + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
        + "{\"name\":\"col41\",\"type\":[\"null\",{\"type\":\"fixed\",\"name\":\"fixed\",\"namespace\":\"hoodie.h0.h0_record.col41\","
        + "\"size\":5,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":4}],\"default\":null},"
        + "{\"name\":\"col5\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"col51\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"col6\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"col7\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}],\"default\":null},"
        + "{\"name\":\"col8\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
        + "{\"name\":\"col9\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"par\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"enum\",\"type\":[\"null\"," + enumSchema + "],\"default\":null}"
        + "]}");
    // create a test record with avroSchema
    GenericData.Record avroRecord = new GenericData.Record(hoodieSchema.toAvroSchema());
    avroRecord.put("id", 1);
    avroRecord.put("comb", 100);
    avroRecord.put("com1", -100);
    avroRecord.put("col0", 256);
    avroRecord.put("col1", 1000L);
    avroRecord.put("col11", -100L);
    avroRecord.put("col12", 2000L);
    avroRecord.put("col2", -5.001f);
    avroRecord.put("col21", 5.001f);
    avroRecord.put("col3", 12.999d);
    avroRecord.put("col31", 9999.999d);
    Schema currentDecimalType = hoodieSchema.getField("col4").get().getAvroField().schema().getTypes().get(1);
    BigDecimal bd = new BigDecimal("123.456").setScale(((LogicalTypes.Decimal) currentDecimalType.getLogicalType()).getScale());
    avroRecord.put("col4", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd, currentDecimalType, currentDecimalType.getLogicalType()));
    Schema currentDecimalType1 = hoodieSchema.getField("col41").get().getAvroField().schema().getTypes().get(1);
    BigDecimal bd1 = new BigDecimal("7890.456").setScale(((LogicalTypes.Decimal) currentDecimalType1.getLogicalType()).getScale());
    avroRecord.put("col41", HoodieAvroUtils.DECIMAL_CONVERSION.toFixed(bd1, currentDecimalType1, currentDecimalType1.getLogicalType()));

    avroRecord.put("col5", "2011-01-01");
    avroRecord.put("col51", "199.342");
    avroRecord.put("col6", 18987);
    avroRecord.put("col7", 1640491505000000L);
    avroRecord.put("col8", false);
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {97, 48, 53});
    avroRecord.put("col9", bb);
    avroRecord.put("enum", new GenericData.EnumSymbol(new Schema.Parser().parse(enumSchema), Enum.ENUM1));
    Assertions.assertEquals(GenericData.get().validate(hoodieSchema.toAvroSchema(), avroRecord), true);
    InternalSchema internalSchema = InternalSchemaConverter.convert(hoodieSchema);
    // do change type operation
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(internalSchema);
    updateChange
        .updateColumnType("id", Types.LongType.get())
        .updateColumnType("comb", Types.FloatType.get())
        .updateColumnType("com1", Types.DoubleType.get())
        .updateColumnType("col0", Types.StringType.get())
        .updateColumnType("col1", Types.FloatType.get())
        .updateColumnType("col11", Types.DoubleType.get())
        .updateColumnType("col12", Types.StringType.get())
        .updateColumnType("col2", Types.DoubleType.get())
        .updateColumnType("col21", Types.StringType.get())
        .updateColumnType("col3", Types.StringType.get())
        .updateColumnType("col31", Types.DecimalType.get(18, 9))
        .updateColumnType("col4", Types.DecimalType.get(18, 9))
        .updateColumnType("col41", Types.StringType.get())
        .updateColumnType("col5", Types.DateType.get())
        .updateColumnType("col51", Types.DecimalType.get(18, 9))
        .updateColumnType("col6", Types.StringType.get())
        .updateColumnType("enum", Types.StringType.get());
    InternalSchema newSchema = SchemaChangeUtils.applyTableChanges2Schema(internalSchema, updateChange);
    HoodieSchema newHoodieSchema = InternalSchemaConverter.convert(newSchema, hoodieSchema.getFullName());
    GenericRecord newRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newHoodieSchema.toAvroSchema(), Collections.emptyMap());

    Assertions.assertEquals("ENUM1", newRecord.get("enum"));
    Assertions.assertEquals(GenericData.get().validate(newHoodieSchema.toAvroSchema(), newRecord), true);
  }

  @Test
  public void testReWriteNestRecord() {
    Types.RecordType record = Types.RecordType.get(Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(Types.Field.get(5, false, "feature1",
                Types.BooleanType.get()), Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(Types.Field.get(10, false, "lat", Types.FloatType.get()), Types.Field.get(11, false, "long", Types.FloatType.get())), false))
    );
    HoodieSchema schema = InternalSchemaConverter.convert(record, "test1");
    GenericData.Record avroRecord = new GenericData.Record(schema.toAvroSchema());
    GenericData.get().validate(schema.toAvroSchema(), avroRecord);
    avroRecord.put("id", 2);
    avroRecord.put("data", "xs");
    // fill record type
    GenericData.Record preferencesRecord = new GenericData.Record(InternalSchemaConverter.convert(record.fieldType("preferences"), "test1.preferences").toAvroSchema());
    preferencesRecord.put("feature1", false);
    preferencesRecord.put("feature2", true);
    Assertions.assertEquals(GenericData.get().validate(InternalSchemaConverter.convert(record.fieldType("preferences"), "test1.preferences").toAvroSchema(), preferencesRecord), true);
    avroRecord.put("preferences", preferencesRecord);
    // fill mapType
    Map<String, GenericData.Record> locations = new HashMap<>();
    Schema mapSchema = InternalSchemaConverter.convert(((Types.MapType)record.fieldByNameCaseInsensitive("locations").type()).valueType(), "test1.locations").toAvroSchema();
    GenericData.Record locationsValue = new GenericData.Record(mapSchema);
    locationsValue.put("lat", 1.2f);
    locationsValue.put("long", 1.4f);
    GenericData.Record locationsValue1 = new GenericData.Record(mapSchema);
    locationsValue1.put("lat", 2.2f);
    locationsValue1.put("long", 2.4f);
    locations.put("key1", locationsValue);
    locations.put("key2", locationsValue1);
    avroRecord.put("locations", locations);

    List<Double> doubles = new ArrayList<>();
    doubles.add(2.0d);
    doubles.add(3.0d);
    avroRecord.put("doubles", doubles);

    // do check
    Assertions.assertTrue(GenericData.get().validate(schema.toAvroSchema(), avroRecord));
    // create newSchema
    Types.RecordType newRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(5, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(6, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(10, true, "laty", Types.FloatType.get()),
                Types.Field.get(11, false, "long", Types.FloatType.get())), false)
        )
    );

    Schema newAvroSchema = InternalSchemaConverter.convert(newRecord, schema.getName()).toAvroSchema();
    GenericRecord newAvroRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, newAvroSchema, Collections.emptyMap());
    // test the correctly of rewrite
    Assertions.assertEquals(GenericData.get().validate(newAvroSchema, newAvroRecord), true);

    // test rewrite with rename
    InternalSchema internalSchema = InternalSchemaConverter.convert(schema);
    // do change rename operation
    TableChanges.ColumnUpdateChange updateChange = TableChanges.ColumnUpdateChange.get(internalSchema);
    updateChange
        .renameColumn("id", "idx")
        .renameColumn("data", "datax")
        .renameColumn("preferences.feature1", "f1")
        .renameColumn("preferences.feature2", "f2")
        .renameColumn("locations.value.lat", "lt");
    InternalSchema internalSchemaRename = SchemaChangeUtils.applyTableChanges2Schema(internalSchema, updateChange);
    HoodieSchema hoodieSchemaRename = InternalSchemaConverter.convert(internalSchemaRename, schema.getFullName());
    Map<String, String> renameCols = InternalSchemaUtils.collectRenameCols(internalSchema, internalSchemaRename);
    GenericRecord avroRecordRename = HoodieAvroUtils.rewriteRecordWithNewSchema(avroRecord, hoodieSchemaRename.toAvroSchema(), renameCols);
    // test the correctly of rewrite
    assertTrue(GenericData.get().validate(hoodieSchemaRename.toAvroSchema(), avroRecordRename));
  }

  @Test
  public void testEvolutionSchemaFromNewAvroSchema() {
    Types.RecordType oldRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(6, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(7, true, "feature2", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(8, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(11, false, "laty", Types.FloatType.get()),
                Types.Field.get(12, false, "long", Types.FloatType.get())), false)
        )
    );
    InternalSchema oldSchema = new InternalSchema(oldRecord);
    Types.RecordType evolvedRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(5, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(6, true, "feature2", Types.BooleanType.get()),
                Types.Field.get(5, true, "feature3", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(7, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(8, 9, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(10, false, "laty", Types.FloatType.get()),
                Types.Field.get(11, false, "long", Types.FloatType.get())), false)
        ),
        Types.Field.get(0, false, "add1", Types.IntType.get()),
        Types.Field.get(2, true, "addStruct",
            Types.RecordType.get(
                Types.Field.get(5, false, "nest1", Types.BooleanType.get()),
                Types.Field.get(5, true, "nest2", Types.BooleanType.get())))
    );
    evolvedRecord = (Types.RecordType)InternalSchemaBuilder.getBuilder().refreshNewId(evolvedRecord, new AtomicInteger(0));
    HoodieSchema evolvedSchema = InternalSchemaConverter.convert(evolvedRecord, "test1");
    InternalSchema result = AvroSchemaEvolutionUtils.reconcileSchema(evolvedSchema, oldSchema, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides(""));
    Types.RecordType checkedRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "data", Types.StringType.get()),
        Types.Field.get(2, true, "preferences",
            Types.RecordType.get(
                Types.Field.get(5, false, "feature1", Types.BooleanType.get()),
                Types.Field.get(6, true, "featurex", Types.BooleanType.get()),
                Types.Field.get(7, true, "feature2", Types.BooleanType.get()),
                Types.Field.get(17, true, "feature3", Types.BooleanType.get()))),
        Types.Field.get(3, false,"doubles", Types.ArrayType.get(8, false, Types.DoubleType.get())),
        Types.Field.get(4, false, "locations", Types.MapType.get(9, 10, Types.StringType.get(),
            Types.RecordType.get(
                Types.Field.get(11, false, "laty", Types.FloatType.get()),
                Types.Field.get(12, false, "long", Types.FloatType.get())), false)
        ),
        Types.Field.get(13, true, "add1", Types.IntType.get()),
        Types.Field.get(14, true, "addStruct",
            Types.RecordType.get(
                Types.Field.get(15, false, "nest1", Types.BooleanType.get()),
                Types.Field.get(16, true, "nest2", Types.BooleanType.get())))
    );
    Assertions.assertEquals(result.getRecord(), checkedRecord);
  }

  @Test
  public void testReconcileSchema() {
    // simple schema test
    // a: boolean, b: int, c: long, d: date
    HoodieSchema schema = create("simple",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE));
    // a: boolean, c: long, c_1: long, d: date
    HoodieSchema incomingSchema = create("simpleIncoming",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("a1", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c1", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c2", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d1", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d2", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE));

    HoodieSchema simpleCheckSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"simple\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
        + "{\"name\":\"b\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"c\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"d\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"a1\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"c1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"c2\",\"type\":[\"null\",\"long\"],\"default\":null},"
        + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},"
        + "{\"name\":\"d2\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null}]}");

    HoodieSchema simpleReconcileSchema = InternalSchemaConverter.convert(AvroSchemaEvolutionUtils
        .reconcileSchema(incomingSchema, InternalSchemaConverter.convert(schema), false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")), "schemaNameFallback");
    Assertions.assertEquals(simpleCheckSchema, simpleReconcileSchema);
  }

  @Test
  public void testNotEvolveSchemaIfReconciledSchemaUnchanged() {
    // a: boolean, c: long, c_1: long, d: date
    HoodieSchema oldSchema = create("simple",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE));
    // incoming schema is part of old schema
    // a: boolean, b: int, c: long
    HoodieSchema incomingSchema = create("simple",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE));

    InternalSchema oldInternalSchema = InternalSchemaConverter.convert(oldSchema);
    // set a non-default schema id for old table schema, e.g., 2.
    oldInternalSchema.setSchemaId(2);
    InternalSchema evolvedSchema = AvroSchemaEvolutionUtils.reconcileSchema(incomingSchema, oldInternalSchema, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides(""));
    // the evolved schema should be the old table schema, since there is no type change at all.
    Assertions.assertEquals(oldInternalSchema, evolvedSchema);
  }

  /**
   * When the incoming schema relaxes an existing required column to nullable, reconcileSchema must evolve
   * that column to nullable in the result, even when makeMissingFieldsNullable is true. Previously the
   * result was rebuilt from the required table and the relaxation was silently dropped, so records with
   * null in that column failed the write / were quarantined.
   */
  @Test
  public void testReconcileSchemaRelaxesExistingColumnToNullable() {
    // table: id (required int), flag (required boolean) -- same column set as the incoming schema
    Types.RecordType oldRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, false, "flag", Types.BooleanType.get())
    );
    InternalSchema oldSchema = new InternalSchema(oldRecord);
    // incoming: identical columns, but the source relaxed "flag" to nullable
    Types.RecordType incomingRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "flag", Types.BooleanType.get())
    );
    incomingRecord = (Types.RecordType) InternalSchemaBuilder.getBuilder().refreshNewId(incomingRecord, new AtomicInteger(0));
    HoodieSchema incomingSchema = InternalSchemaConverter.convert(incomingRecord, "test1");

    InternalSchema result = AvroSchemaEvolutionUtils.reconcileSchema(incomingSchema, oldSchema, true,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides(""));

    Types.RecordType checkedRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "flag", Types.BooleanType.get())
    );
    Assertions.assertEquals(checkedRecord, result.getRecord());
  }

  /**
   * reconcileSchema must only ever relax (widen) an existing column's nullability, never tighten it: if the
   * incoming schema marks a column required but the table has it nullable, the table stays nullable.
   */
  @Test
  public void testReconcileSchemaDoesNotTightenNullableToRequired() {
    // table: id (required int), flag (nullable boolean)
    Types.RecordType oldRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "flag", Types.BooleanType.get())
    );
    InternalSchema oldSchema = new InternalSchema(oldRecord);
    // incoming: source tightened "flag" to required -- must NOT tighten the table
    Types.RecordType incomingRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, false, "flag", Types.BooleanType.get())
    );
    incomingRecord = (Types.RecordType) InternalSchemaBuilder.getBuilder().refreshNewId(incomingRecord, new AtomicInteger(0));
    HoodieSchema incomingSchema = InternalSchemaConverter.convert(incomingRecord, "test1");

    InternalSchema result = AvroSchemaEvolutionUtils.reconcileSchema(incomingSchema, oldSchema, true,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides(""));

    Types.RecordType checkedRecord = Types.RecordType.get(
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "flag", Types.BooleanType.get())
    );
    Assertions.assertEquals(checkedRecord, result.getRecord());
  }

  private static Schema tripAvro(Schema tsType) {
    return Schema.createRecord("trip", null, null, false, Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("ts", tsType, null, null)));
  }

  @Test
  public void testReconcileSchemaTimestampPrecisionEvolution() {
    // A timestamp precision change is rejected unless the field has an explicit override in
    // hoodie.write.timestamp.logical.type.overrides. The override pins the field: an entry equal to
    // the table type coerces the incoming values and keeps the table precision, while a different
    // entry evolves the column. No entry throws, so an unverified micros/millis flip cannot happen.
    HoodieSchema tableSchemaMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    HoodieSchema incomingSchemaMillis = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));

    // Guard: with no override, the precision change is rejected in either direction with an
    // actionable error that names the column and the config to set.
    Throwable rejectedMicrosToMillis = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(incomingSchemaMillis, tableSchemaMicros, false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    assertTrue(rejectedMicrosToMillis.getMessage().contains("without an explicit"));
    assertTrue(rejectedMicrosToMillis.getMessage().contains(HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key()));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(tableSchemaMicros, incomingSchemaMillis, false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));

    // Override to millis: the micros table evolves to millis (the genuine-repair case).
    Schema evolvedToMillis = AvroSchemaEvolutionUtils.reconcileSchema(incomingSchemaMillis, tableSchemaMicros, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-millis")).toAvroSchema();
    Assertions.assertEquals("timestamp-millis", evolvedToMillis.getField("ts").schema().getLogicalType().getName());

    // Override to micros with a millis source (the Apna case): the table stays micros, no flip; the
    // incoming millis values are coerced to micros on write.
    Schema pinnedToMicros = AvroSchemaEvolutionUtils.reconcileSchema(incomingSchemaMillis, tableSchemaMicros, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")).toAvroSchema();
    Assertions.assertEquals("timestamp-micros", pinnedToMicros.getField("ts").schema().getLogicalType().getName());

    // Override to micros against a millis table: the reverse evolution is permitted.
    Schema evolvedToMicros = AvroSchemaEvolutionUtils.reconcileSchema(tableSchemaMicros, incomingSchemaMillis, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")).toAvroSchema();
    Assertions.assertEquals("timestamp-micros", evolvedToMicros.getField("ts").schema().getLogicalType().getName());

    // The same override applies to the local-timestamp variants.
    HoodieSchema tableLocalMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    HoodieSchema incomingLocalMillis = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(incomingLocalMillis, tableLocalMicros, false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    Schema reconciledLocal = AvroSchemaEvolutionUtils.reconcileSchema(incomingLocalMillis, tableLocalMicros, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-millis")).toAvroSchema();
    Assertions.assertEquals("local-timestamp-millis", reconciledLocal.getField("ts").schema().getLogicalType().getName());

    // 0.x did not recognize the local-timestamp logical types, so affected tables persisted those
    // columns as bare long. The override must also allow attaching the logical type on forward-fix.
    HoodieSchema tableBareLong = HoodieSchema.fromAvroSchema(tripAvro(Schema.create(Schema.Type.LONG)));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(incomingLocalMillis, tableBareLong, false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    Schema repairedToLocalMillis = AvroSchemaEvolutionUtils.reconcileSchema(incomingLocalMillis, tableBareLong, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-millis")).toAvroSchema();
    Assertions.assertEquals("local-timestamp-millis", repairedToLocalMillis.getField("ts").schema().getLogicalType().getName());

    HoodieSchema incomingLocalMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    Schema repairedToLocalMicros = AvroSchemaEvolutionUtils.reconcileSchema(incomingLocalMicros, tableBareLong, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-micros")).toAvroSchema();
    Assertions.assertEquals("local-timestamp-micros", repairedToLocalMicros.getField("ts").schema().getLogicalType().getName());
  }

  @Test
  public void testReconcileTimestampLogicalTypeGuardsNonReconcilePath() {
    // reconcileTimestampLogicalType is the guard applied to the deduced writer schema on every path,
    // including the default set.null=false path whose Avro compatibility check is logical-type-blind.
    HoodieSchema tableMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    HoodieSchema writerMillis = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));

    // Guard: no override and the precision differs, so the flip is rejected instead of silently applied.
    Throwable rejected = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(writerMillis, tableMicros,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    assertTrue(rejected.getMessage().contains("without an explicit"));
    assertTrue(rejected.getMessage().contains("'ts'"));

    // Override to micros coerces the millis writer back to micros (no flip, the Apna case).
    Schema coerced = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(writerMillis, tableMicros,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")).toAvroSchema();
    Assertions.assertEquals("timestamp-micros", coerced.getField("ts").schema().getLogicalType().getName());

    // Override to millis keeps the writer at millis (authorized evolution).
    Schema evolved = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(writerMillis, tableMicros,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-millis")).toAvroSchema();
    Assertions.assertEquals("timestamp-millis", evolved.getField("ts").schema().getLogicalType().getName());

    // No precision difference: returned unchanged, no override required and no throw.
    Schema unchanged = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(tableMicros, tableMicros,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")).toAvroSchema();
    Assertions.assertEquals("timestamp-micros", unchanged.getField("ts").schema().getLogicalType().getName());
  }

  /**
   * End-to-end value assertion on the coerce/pin path — the Apna case. Source declares
   * timestamp-millis, table is timestamp-micros, override pins the field to the table's micros
   * type. The reconcile flips the writer schema back to micros. When a record whose source Avro
   * schema declared millis is rewritten to the (now-micros) writer schema, the long must still be
   * rescaled by 1000 — not left as-is because writer == table.
   *
   * <p>The prior boolean flag would have flipped the table to millis without touching values,
   * causing the "reads as year 58466" failure. This test guards that value-level behavior directly.
   */
  @Test
  public void testReconcileTimestampLogicalTypeCoercesValuesOnPin() {
    HoodieSchema tableMicrosSchema = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    Schema sourceMillisSchema = tripAvro(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)));

    // Driver-plan step: apply the guard with the coerce override. The writer schema for `ts`
    // should be pinned back to timestamp-micros (matching the table), not left as millis.
    Schema writerSchema = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(
        HoodieSchema.fromAvroSchema(sourceMillisSchema), tableMicrosSchema,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")).toAvroSchema();
    Assertions.assertEquals("timestamp-micros", writerSchema.getField("ts").schema().getLogicalType().getName());

    // Executor step: an incoming record carrying the SOURCE schema (millis) is rewritten to the
    // deduced WRITER schema (micros). rewriteRecordWithNewSchema must invoke the x1000 rescale so
    // 2024-01-01T00:00:00Z millis (1704067200000L) becomes the equivalent micros
    // (1704067200000000L) — not the same long reinterpreted, which would read as year 55965.
    long millisValue = 1704067200000L; // 2024-01-01T00:00:00Z as epoch millis
    long expectedMicros = 1704067200000000L; // same instant as epoch micros
    GenericRecord sourceRecord = new GenericData.Record(sourceMillisSchema);
    sourceRecord.put("id", "row-1");
    sourceRecord.put("ts", millisValue);
    GenericRecord rewritten = HoodieAvroUtils.rewriteRecordWithNewSchema(sourceRecord, writerSchema);
    Assertions.assertEquals(expectedMicros, rewritten.get("ts"),
        "millis source value must be rescaled to micros when the writer schema is pinned to micros");

    // Symmetric coverage: source declares micros, table is millis, override pins to millis.
    // Rewrite must divide by 1000 (integer division). Pick a value that is exact.
    HoodieSchema tableMillisSchema = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
    Schema sourceMicrosSchema = tripAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    Schema writerSchemaMillis = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(
        HoodieSchema.fromAvroSchema(sourceMicrosSchema), tableMillisSchema,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-millis")).toAvroSchema();
    Assertions.assertEquals("timestamp-millis", writerSchemaMillis.getField("ts").schema().getLogicalType().getName());
    GenericRecord sourceMicros = new GenericData.Record(sourceMicrosSchema);
    sourceMicros.put("id", "row-2");
    sourceMicros.put("ts", expectedMicros);
    GenericRecord rewrittenMillis = HoodieAvroUtils.rewriteRecordWithNewSchema(sourceMicros, writerSchemaMillis);
    Assertions.assertEquals(millisValue, rewrittenMillis.get("ts"),
        "micros source value must be rescaled to millis when the writer schema is pinned to millis");
  }

  /**
   * A UTC/local zone change is not a precision repair. The stored long means a different instant
   * under each interpretation and no rescale can fix that, so a zone change must be rejected on
   * every path and no per-field override may authorize it.
   *
   * <p>Both entry points have to enforce it. reconcileSchema rejects via isTypeUpdateAllow, but
   * reconcileTimestampLogicalType is the only guard on the default non-reconcile path, and the
   * Avro reader/writer compatibility check that runs after it is logical-type-blind for two
   * long-backed fields -- so if the guard skips a zone change, nothing else catches it.
   */
  @Test
  public void testCrossZoneTimestampChangeIsRejected() {
    HoodieSchema tableMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    HoodieSchema localMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    HoodieSchema tableMillis = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
    HoodieSchema localMillis = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));

    // No override: rejected by both entry points, in both zone directions.
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(localMicros, InternalSchemaConverter.convert(tableMicros), false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(tableMicros, InternalSchemaConverter.convert(localMicros), false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    Throwable guarded = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(localMicros, tableMicros,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));
    assertTrue(guarded.getMessage().contains("'ts'"), "Unexpected message: " + guarded.getMessage());
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(tableMicros, localMicros,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));

    // An override must NOT unlock a zone change, whichever zone it names.
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(localMicros, tableMicros,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-micros")));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(localMicros, tableMicros,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(localMicros, InternalSchemaConverter.convert(tableMicros), false,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-micros")));

    // A zone change that also crosses precision is still a zone change.
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(localMillis, tableMicros,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-millis")));
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(localMicros, tableMillis,
            SchemaChangeUtils.parseTimestampLogicalTypeOverrides("")));

    // Same-zone precision changes are unaffected: still gated by the override, not by the zone check.
    Schema stillWorks = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(localMillis, localMicros,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-millis")).toAvroSchema();
    Assertions.assertEquals("local-timestamp-millis", stillWorks.getField("ts").schema().getLogicalType().getName());
  }

  @Test
  void testLongToUtcTimestampGatedInBothReconcilePaths() {
    // Bare long to a UTC timestamp is override-gated exactly like the local-timestamp case: rejected
    // without a per-field override and applied with one, in both reconcile paths. The non-reconcile
    // guard previously skipped this and let it through silently on the default write path.
    HoodieSchema tableBareLong = HoodieSchema.fromAvroSchema(tripAvro(Schema.create(Schema.Type.LONG)));
    HoodieSchema incomingMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));

    // No override: rejected in both paths with the exact actionable error.
    Map<String, Type> noOverride = SchemaChangeUtils.parseTimestampLogicalTypeOverrides("");
    String expectedError = AvroSchemaEvolutionUtils.timestampPrecisionChangeError(
        "ts", Types.LongType.get(), Types.TimestampType.get()).getMessage();
    SchemaCompatibilityException reconcileError = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(incomingMicros, tableBareLong, false, noOverride));
    assertEquals(expectedError, reconcileError.getMessage());
    SchemaCompatibilityException guardError = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(incomingMicros, tableBareLong, noOverride));
    assertEquals(expectedError, guardError.getMessage());

    // With the override: the promotion is applied in both paths.
    Schema viaReconcile = AvroSchemaEvolutionUtils.reconcileSchema(incomingMicros, tableBareLong, false,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")).toAvroSchema();
    assertEquals("timestamp-micros", viaReconcile.getField("ts").schema().getLogicalType().getName());
    Schema viaGuard = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(incomingMicros, tableBareLong,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:timestamp-micros")).toAvroSchema();
    assertEquals("timestamp-micros", viaGuard.getField("ts").schema().getLogicalType().getName());
  }

  @Test
  void testLongToLocalTimestampGatedInBothReconcilePaths() {
    // Bare long to local timestamp is override-gated (not forbidden): rejected without an override
    // and applied with one, and the non-reconcile guard must agree with reconcileSchema.
    HoodieSchema tableBareLong = HoodieSchema.fromAvroSchema(tripAvro(Schema.create(Schema.Type.LONG)));
    HoodieSchema incomingLocalMicros = HoodieSchema.fromAvroSchema(tripAvro(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));

    // No override: rejected in both paths with the exact actionable error.
    Map<String, Type> noOverride = SchemaChangeUtils.parseTimestampLogicalTypeOverrides("");
    String expectedError = AvroSchemaEvolutionUtils.timestampPrecisionChangeError(
        "ts", Types.LongType.get(), Types.LocalTimestampMicrosType.get()).getMessage();
    SchemaCompatibilityException reconcileError = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileSchema(incomingLocalMicros, tableBareLong, false, noOverride));
    assertEquals(expectedError, reconcileError.getMessage());
    SchemaCompatibilityException guardError = assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(incomingLocalMicros, tableBareLong, noOverride));
    assertEquals(expectedError, guardError.getMessage());
    Schema repaired = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(incomingLocalMicros, tableBareLong,
        SchemaChangeUtils.parseTimestampLogicalTypeOverrides("ts:local-timestamp-micros")).toAvroSchema();
    assertEquals("local-timestamp-micros", repaired.getField("ts").schema().getLogicalType().getName());
  }

  @Test
  void testNestedLongToTimestampGated() {
    // The gate resolves fully-qualified column names, so it applies to nested fields too. A nested
    // long -> timestamp (UTC or local) is override-gated via the dotted-key override.
    for (String token : new String[] {"timestamp-micros", "local-timestamp-millis"}) {
      HoodieSchema tableNested = HoodieSchema.fromAvroSchema(nestedTrip(Schema.create(Schema.Type.LONG)));
      HoodieSchema incoming = HoodieSchema.fromAvroSchema(nestedTrip(logicalLong(token)));
      // No override: rejected in both paths with the exact actionable error.
      Map<String, Type> noOverride = SchemaChangeUtils.parseTimestampLogicalTypeOverrides("");
      String expectedError = AvroSchemaEvolutionUtils.timestampPrecisionChangeError("payload.event_ts", Types.LongType.get(),
          SchemaChangeUtils.parseTimestampLogicalTypeOverrides("field:" + token).get("field")).getMessage();
      SchemaCompatibilityException reconcileError = assertThrows(SchemaCompatibilityException.class,
          () -> AvroSchemaEvolutionUtils.reconcileSchema(incoming, tableNested, false, noOverride));
      assertEquals(expectedError, reconcileError.getMessage());
      SchemaCompatibilityException guardError = assertThrows(SchemaCompatibilityException.class,
          () -> AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(incoming, tableNested, noOverride));
      assertEquals(expectedError, guardError.getMessage());
      // The dotted-key override authorizes the nested promotion.
      Schema repairedNested = AvroSchemaEvolutionUtils.reconcileTimestampLogicalType(incoming, tableNested,
          SchemaChangeUtils.parseTimestampLogicalTypeOverrides("payload.event_ts:" + token)).toAvroSchema();
      assertEquals(token,
          repairedNested.getField("payload").schema().getField("event_ts").schema().getLogicalType().getName());
    }
  }

  private static Schema nestedTrip(Schema eventTsType) {
    Schema payload = Schema.createRecord("payloadrec", null, null, false, Arrays.asList(
        new Schema.Field("event_ts", eventTsType, null, null)));
    return Schema.createRecord("trip", null, null, false, Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("payload", payload, null, null)));
  }

  private static Schema logicalLong(String token) {
    Schema longSchema = Schema.create(Schema.Type.LONG);
    switch (token) {
      case "timestamp-micros":
        return LogicalTypes.timestampMicros().addToSchema(longSchema);
      case "timestamp-millis":
        return LogicalTypes.timestampMillis().addToSchema(longSchema);
      case "local-timestamp-micros":
        return LogicalTypes.localTimestampMicros().addToSchema(longSchema);
      case "local-timestamp-millis":
        return LogicalTypes.localTimestampMillis().addToSchema(longSchema);
      default:
        throw new IllegalArgumentException(token);
    }
  }
}
