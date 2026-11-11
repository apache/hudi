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
package io.trino.plugin.hudi.util;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiAvroDecimalSerialization
{
    @ParameterizedTest
    @MethodSource("longDecimals")
    public void testAppendLongDecimalFromAvroFixed(int precision, int scale, String value)
    {
        DecimalType type = DecimalType.createDecimalType(precision, scale);
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        HudiAvroSerializer.appendTo(type, avroDecimalFixed(precision, scale, value), builder);
        assertThat(type.getObjectValue(builder.build(), 0).toString()).isEqualTo(value);
    }

    private static Stream<Arguments> longDecimals()
    {
        return Stream.of(
                Arguments.of(20, 2, "1.23"),
                Arguments.of(20, 2, "-0.07"),
                Arguments.of(20, 2, "0.00"),
                Arguments.of(38, 0, "99999999999999999999999999999999999999"),
                Arguments.of(38, 0, "-99999999999999999999999999999999999999"),
                Arguments.of(38, 6, "12345678901234567890123456789012.123456"));
    }

    @ParameterizedTest
    @MethodSource("longDecimals")
    public void testAppendLongDecimalFromAvroBytes(int precision, int scale, String value)
    {
        assertDecimalFromAvroBytes(precision, scale, value);
    }

    @ParameterizedTest
    @MethodSource("shortDecimals")
    public void testAppendShortDecimalFromAvroBytes(int precision, int scale, String value, long expectedUnscaled)
    {
        assertDecimalFromAvroBytes(precision, scale, value);
    }

    private static Stream<Arguments> shortDecimals()
    {
        return Stream.of(
                Arguments.of(10, 2, "123.45", 12345L),
                Arguments.of(10, 2, "-0.07", -7L),
                Arguments.of(10, 2, "0.00", 0L),
                Arguments.of(10, 4, "123.4567", 1234567L),
                Arguments.of(5, 0, "42", 42L),
                Arguments.of(5, 0, "-42", -42L),
                // Widest short decimal: 18 digits, both signs
                Arguments.of(18, 2, "9999999999999999.99", 999999999999999999L),
                Arguments.of(18, 2, "-9999999999999999.99", -999999999999999999L));
    }

    private static void assertDecimalFromAvroBytes(int precision, int scale, String value)
    {
        DecimalType type = DecimalType.createDecimalType(precision, scale);
        LogicalTypes.Decimal logicalType = LogicalTypes.decimal(precision, scale);
        Schema schema = logicalType.addToSchema(Schema.create(Schema.Type.BYTES));
        ByteBuffer encoded = new Conversions.DecimalConversion().toBytes(new BigDecimal(value), schema, logicalType);

        // Padding outside position/limit must not affect the decoded value, including in sliced buffers.
        ByteBuffer padded = ByteBuffer.allocate(encoded.remaining() + 2);
        padded.put((byte) 0x55).put(encoded.duplicate()).put((byte) 0x55);
        padded.flip().position(1).limit(padded.limit() - 1);
        ByteBuffer direct = ByteBuffer.allocateDirect(encoded.remaining());
        direct.put(encoded.duplicate()).flip();
        for (ByteBuffer buffer : List.of(encoded, padded, padded.slice(), padded.asReadOnlyBuffer(), direct)) {
            int position = buffer.position();
            int limit = buffer.limit();
            BlockBuilder builder = type.createBlockBuilder(null, 2);
            HudiAvroSerializer.appendTo(type, buffer, builder);
            HudiAvroSerializer.appendTo(type, buffer, builder);
            Block block = builder.build();
            assertThat(type.getObjectValue(block, 0).toString()).isEqualTo(value);
            assertThat(type.getObjectValue(block, 1).toString()).isEqualTo(value);
            assertThat(buffer.position()).isEqualTo(position);
            assertThat(buffer.limit()).isEqualTo(limit);
        }
    }

    /**
     * Encodes the value the way an Avro writer does, via Avro's own conversion: a fixed sized from the
     * precision, holding the unscaled value left-padded to that width with the sign byte (0xFF for
     * negatives). Building the fixed from the minimal two's-complement encoding instead would leave the
     * padding bytes, and so sign extension across them, untested.
     */
    private static GenericData.Fixed avroDecimalFixed(int precision, int scale, String value)
    {
        LogicalTypes.Decimal decimalType = LogicalTypes.decimal(precision, scale);
        Schema fixedSchema = decimalType.addToSchema(
                Schema.createFixed("fix", null, null, decimalFixedSize(precision)));
        return (GenericData.Fixed) new Conversions.DecimalConversion()
                .toFixed(new BigDecimal(value), fixedSchema, decimalType);
    }

    /** Bytes needed to hold the widest unscaled value at this precision, i.e. the fixed size Avro sizes a decimal to. */
    private static int decimalFixedSize(int precision)
    {
        return BigInteger.TEN.pow(precision).subtract(BigInteger.ONE).toByteArray().length;
    }
}
