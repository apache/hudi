/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.query.index;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.keygen.KeyGenUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hudi.query.index.HudiRecordLevelIndexSupport.constructRecordKeys;
import static io.trino.plugin.hudi.query.index.IndexKeyForm.RECORD_KEY;
import static io.trino.plugin.hudi.query.index.IndexKeyForm.SECONDARY_KEY;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Each record key text is checked twice: against the literal a reader of the index would see, and against the key
 * Hudi's own {@link KeyGenUtils#getRecordKey} builds from the Avro record a writer holds.
 */
public class TestIndexKeyForm
{
    // 2024-01-01 10:00:00.123456 UTC
    private static final long EPOCH_MILLIS = 1_704_103_200_123L;
    private static final long EPOCH_MICROS = 1_704_103_200_123_456L;
    // 01:02:03.456789
    private static final long TIME_OF_DAY_MICROS = 3_723_456_789L;
    private static final long PICOS_PER_MICRO = 1_000_000L;

    private static final HoodieSchema DECIMAL_BYTES = HoodieSchema.createDecimal(10, 2);
    private static final HoodieSchema DECIMAL_FIXED = HoodieSchema.createDecimal("dec_fixed", null, null, 20, 4, 9);
    private static final HoodieSchema DECIMAL_HIGH_SCALE = HoodieSchema.createDecimal(12, 10);

    private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("keys", null, null, ImmutableList.of(
            HoodieSchemaField.of("b", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
            HoodieSchemaField.of("i", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("l", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("f", HoodieSchema.create(HoodieSchemaType.FLOAT)),
            HoodieSchemaField.of("dbl", HoodieSchema.create(HoodieSchemaType.DOUBLE)),
            HoodieSchemaField.of("s", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("bin", HoodieSchema.create(HoodieSchemaType.BYTES)),
            HoodieSchemaField.of("u", HoodieSchema.createUUID()),
            HoodieSchemaField.of("dec", DECIMAL_BYTES),
            HoodieSchemaField.of("dec_fixed", DECIMAL_FIXED),
            HoodieSchemaField.of("dec_zero", DECIMAL_HIGH_SCALE),
            HoodieSchemaField.of("d", HoodieSchema.createDate()),
            HoodieSchemaField.of("t_millis", HoodieSchema.createTimeMillis()),
            HoodieSchemaField.of("t_micros", HoodieSchema.createTimeMicros()),
            HoodieSchemaField.of("ts_millis", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("ts_micros", HoodieSchema.createTimestampMicros()),
            HoodieSchemaField.of("lts_millis", HoodieSchema.createLocalTimestampMillis()),
            HoodieSchemaField.of("lts_micros", HoodieSchema.createLocalTimestampMicros()),
            HoodieSchemaField.of("arr", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.LONG)))));

    /** The record a writer holds for the values below, in Avro's stored form. */
    private static final GenericRecord WRITER_RECORD = writerRecord();

    @Test
    public void testRecordKeyTextMatchesWriter()
    {
        assertRecordKey("b", BOOLEAN, true, "true");
        assertRecordKey("i", INTEGER, 42L, "42");
        assertRecordKey("l", BIGINT, 10_000_000_000L, "10000000000");
        assertRecordKey("f", REAL, (long) floatToRawIntBits(1.5f), "1.5");
        assertRecordKey("dbl", DOUBLE, 2.25, "2.25");
        assertRecordKey("s", VARCHAR, utf8Slice("abc"), "abc");
        assertRecordKey("u", UuidType.UUID, javaUuidToTrinoUuid(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")), "123e4567-e89b-12d3-a456-426614174000");
        // Decimals key as BigDecimal.toString at the field's scale, scientific notation included
        assertRecordKey("dec", createDecimalType(10, 2), -12_345L, "-123.45");
        assertRecordKey("dec_fixed", createDecimalType(20, 4), encodeScaledValue(new BigDecimal("1234567890123456.7890"), 4), "1234567890123456.7890");
        assertRecordKey("dec_zero", createDecimalType(12, 10), 0L, "0E-10");
        assertRecordKey("d", DATE, 19_723L, "2024-01-01");
        // A time keys as its stored count of its own unit; Trino holds picoseconds of the day
        assertRecordKey("t_millis", TIME_MILLIS, TIME_OF_DAY_MICROS / 1000 * 1000 * PICOS_PER_MICRO, "3723456");
        assertRecordKey("t_micros", TIME_MICROS, TIME_OF_DAY_MICROS * PICOS_PER_MICRO, "3723456789");
        // A local timestamp-micros keys as its wall-clock epoch micros, which Trino holds a short timestamp as too
        assertRecordKey("lts_micros", TIMESTAMP_MICROS, EPOCH_MICROS, "1704103200123456");
    }

    @Test
    public void testCompositeRecordKeyMatchesWriter()
    {
        TupleDomain<String> domains = TupleDomain.withColumnDomains(Map.of(
                "d", singleValue(DATE, 19_723L),
                "lts_micros", multipleValues(TIMESTAMP_MICROS, List.of(EPOCH_MICROS, EPOCH_MICROS + 1))));
        List<String> keys = constructRecordKeys(domains, List.of("d", "lts_micros"), SCHEMA, RECORD_KEY);
        assertThat(keys).containsExactly("d:2024-01-01,lts_micros:1704103200123456", "d:2024-01-01,lts_micros:1704103200123457");
        assertThat(keys.getFirst()).isEqualTo(KeyGenUtils.getRecordKey(WRITER_RECORD, List.of("d", "lts_micros"), false));

        // A UTC timestamp part has no key text, so the composite key is not built and nothing is pruned
        TupleDomain<String> utcPart = TupleDomain.withColumnDomains(Map.of(
                "d", singleValue(DATE, 19_723L),
                "ts_millis", singleValue(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(EPOCH_MILLIS, UTC_KEY))));
        assertThat(constructRecordKeys(utcPart, List.of("d", "ts_millis"), SCHEMA, RECORD_KEY)).isEmpty();

        // An empty part keys as the writer's placeholder
        TupleDomain<String> emptyPart = TupleDomain.withColumnDomains(Map.of(
                "s", singleValue(VARCHAR, utf8Slice("")),
                "i", singleValue(INTEGER, 42L)));
        assertThat(constructRecordKeys(emptyPart, List.of("s", "i"), SCHEMA, RECORD_KEY)).containsExactly("s:__empty__,i:42");
    }

    @Test
    public void testNoKeyTextWithoutWriterForm()
    {
        // A nested value has no key form
        assertThat(RECORD_KEY.keyText("arr", fieldSchema("arr"), new ArrayType(BIGINT))).isEmpty();
        // A UTC timestamp keys as a number on the Avro path but as java.sql.Timestamp text on Spark's row-writer path,
        // so neither form can be trusted to be the one a table holds
        assertThat(RECORD_KEY.keyText("ts_millis", fieldSchema("ts_millis"), TIMESTAMP_TZ_MILLIS)).isEmpty();
        assertThat(RECORD_KEY.keyText("ts_micros", fieldSchema("ts_micros"), TIMESTAMP_TZ_MICROS)).isEmpty();
        // A local timestamp-millis keys as millis on the Avro path but as Spark's timestamp_ntz micros on the row writer
        assertThat(RECORD_KEY.keyText("lts_millis", fieldSchema("lts_millis"), TIMESTAMP_MILLIS)).isEmpty();
        // Bytes and fixed key as hex or array text on the Avro path but as a byte[]'s identity string on the row writer
        assertThat(RECORD_KEY.keyText("bin", fieldSchema("bin"), VARBINARY)).isEmpty();
        // A column that does not carry the field's type (a partition column keeps its metastore type) is not keyed
        assertThat(RECORD_KEY.keyText("ts_millis", fieldSchema("ts_millis"), BIGINT)).isEmpty();
        assertThat(RECORD_KEY.keyText("ts_micros", fieldSchema("ts_micros"), TIMESTAMP_TZ_MILLIS)).isEmpty();
        // An unknown table schema keys nothing
        assertThat(constructRecordKeys(TupleDomain.withColumnDomains(Map.of("s", singleValue(VARCHAR, utf8Slice("abc")))), List.of("s"), null, RECORD_KEY)).isEmpty();
    }

    @Test
    public void testSecondaryKeyText()
    {
        // A secondary key is the stored value's text: a date as its epoch day
        assertThat(secondaryKey("d", DATE, 19_723L)).isEqualTo("19723");
        assertThat(secondaryKey("ts_micros", TIMESTAMP_TZ_MICROS, LongTimestampWithTimeZone.fromEpochMillisAndFraction(EPOCH_MILLIS, 456_000_000, UTC_KEY)))
                .isEqualTo("1704103200123456");
        assertThat(secondaryKey("t_micros", TIME_MICROS, TIME_OF_DAY_MICROS * PICOS_PER_MICRO)).isEqualTo("3723456789");
        assertThat(secondaryKey("dbl", DOUBLE, 101.0)).isEqualTo("101.0");
        assertThat(secondaryKey("s", VARCHAR, utf8Slice("abc"))).isEqualTo("abc");
        // Spark and Avro reader contexts key a timestamp-millis in different units; Hudi has no secondary index on
        // decimals or local timestamps
        assertThat(SECONDARY_KEY.keyText("ts_millis", fieldSchema("ts_millis"), TIMESTAMP_TZ_MILLIS)).isEmpty();
        assertThat(SECONDARY_KEY.keyText("dec", fieldSchema("dec"), createDecimalType(10, 2))).isEmpty();
        assertThat(SECONDARY_KEY.keyText("lts_micros", fieldSchema("lts_micros"), TIMESTAMP_MICROS)).isEmpty();
    }

    private static void assertRecordKey(String field, Type type, Object trinoValue, String expected)
    {
        String keyText = RECORD_KEY.keyText(field, fieldSchema(field), type).orElseThrow().apply(trinoValue);
        assertThat(keyText).isEqualTo(expected);
        assertThat(keyText).isEqualTo(KeyGenUtils.getRecordKey(WRITER_RECORD, field, false));
        // The same text through the lookup key builder, nullable field included
        HoodieSchema nullableSchema = HoodieSchema.createRecord("nullable", null, null, ImmutableList.of(
                HoodieSchemaField.of(field, HoodieSchema.createNullable(fieldSchema(field)), null, HoodieSchema.NULL_VALUE)));
        assertThat(constructRecordKeys(TupleDomain.withColumnDomains(Map.of(field, singleValue(type, trinoValue))), List.of(field), nullableSchema, RECORD_KEY))
                .containsExactly(expected);
    }

    private static String secondaryKey(String field, Type type, Object trinoValue)
    {
        return SECONDARY_KEY.keyText(field, fieldSchema(field), type).orElseThrow().apply(trinoValue);
    }

    private static HoodieSchema fieldSchema(String field)
    {
        return SCHEMA.getField(field).get().schema();
    }

    private static GenericRecord writerRecord()
    {
        Schema avroSchema = SCHEMA.toAvroSchema();
        Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("b", true);
        record.put("i", 42);
        record.put("l", 10_000_000_000L);
        record.put("f", 1.5f);
        record.put("dbl", 2.25);
        record.put("s", "abc");
        record.put("bin", ByteBuffer.wrap(new byte[] {(byte) 0xCA, (byte) 0xFE}));
        record.put("u", "123e4567-e89b-12d3-a456-426614174000");
        Schema decimalBytes = DECIMAL_BYTES.toAvroSchema();
        record.put("dec", decimalConversion.toBytes(new BigDecimal("-123.45"), decimalBytes, decimalBytes.getLogicalType()));
        Schema decimalFixed = DECIMAL_FIXED.toAvroSchema();
        record.put("dec_fixed", decimalConversion.toFixed(new BigDecimal("1234567890123456.7890"), decimalFixed, decimalFixed.getLogicalType()));
        Schema decimalHighScale = DECIMAL_HIGH_SCALE.toAvroSchema();
        record.put("dec_zero", decimalConversion.toBytes(BigDecimal.ZERO.setScale(10), decimalHighScale, decimalHighScale.getLogicalType()));
        record.put("d", 19_723);
        record.put("t_millis", (int) (TIME_OF_DAY_MICROS / 1000));
        record.put("t_micros", TIME_OF_DAY_MICROS);
        record.put("ts_millis", EPOCH_MILLIS);
        record.put("ts_micros", EPOCH_MICROS);
        record.put("lts_millis", EPOCH_MILLIS);
        record.put("lts_micros", EPOCH_MICROS);
        record.put("arr", List.of(1L));
        return record;
    }
}
