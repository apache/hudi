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

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestHudiAvroSerializer
{
    @Test
    public void testDecimalConverter()
    {
        HudiAvroSerializer.AvroDecimalConverter converter = new HudiAvroSerializer.AvroDecimalConverter();

        assertThat(converter.convert(10, 2, unscaledBytes("123.45"))).isEqualTo(new BigDecimal("123.45"));
        // Same (precision, scale) again: served from the cached schema
        assertThat(converter.convert(10, 2, unscaledBytes("-0.07"))).isEqualTo(new BigDecimal("-0.07"));
        // Same precision, different scale, and vice versa: must not collide in the cache
        assertThat(converter.convert(10, 4, unscaledBytes("123.4567"))).isEqualTo(new BigDecimal("123.4567"));
        assertThat(converter.convert(18, 2, unscaledBytes("9999999999999999.99"))).isEqualTo(new BigDecimal("9999999999999999.99"));
        assertThat(converter.convert(5, 0, unscaledBytes("42"))).isEqualTo(new BigDecimal("42"));
    }

    @Test
    public void testAppendShortDecimalFromAvroFixed()
    {
        DecimalType type = DecimalType.createDecimalType(10, 2);
        byte[] bytes = unscaledBytes("123.45");
        GenericData.Fixed fixed = new GenericData.Fixed(Schema.createFixed("fix", null, null, bytes.length), bytes);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        HudiAvroSerializer.appendTo(type, fixed, blockBuilder);
        Block block = blockBuilder.build();

        assertThat(type.getLong(block, 0)).isEqualTo(12345L);
    }

    @Test
    public void testBuildRecordInPage()
    {
        // Schema field order (b, a) deliberately differs from projection order (a, b, pk_int),
        // so correct output proves positions are resolved from the record's schema.
        Schema schema = recordSchema("rec1");
        HudiAvroSerializer serializer = new HudiAvroSerializer(projectedColumns(), prefilledValues());
        PageBuilder pageBuilder = new PageBuilder(List.of(BIGINT, VARCHAR, INTEGER));

        serializer.buildRecordInPage(pageBuilder, record(schema, 1L, "one"));
        // Second record with the same schema instance exercises the cached field positions
        serializer.buildRecordInPage(pageBuilder, record(schema, 2L, "two"));
        // A schema instance with the opposite field order must invalidate the cache; reusing the
        // stale positions would swap the a and b values
        serializer.buildRecordInPage(pageBuilder, record(reversedRecordSchema("rec2"), 3L, "three"));

        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(3);
        for (int position = 0; position < 3; position++) {
            assertThat(BIGINT.getLong(page.getBlock(0), position)).isEqualTo(position + 1);
            assertThat(INTEGER.getInt(page.getBlock(2), position)).isEqualTo(42);
        }
        assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo("one");
        assertThat(VARCHAR.getSlice(page.getBlock(1), 1).toStringUtf8()).isEqualTo("two");
        assertThat(VARCHAR.getSlice(page.getBlock(1), 2).toStringUtf8()).isEqualTo("three");
    }

    private static byte[] unscaledBytes(String decimal)
    {
        return new BigDecimal(decimal).unscaledValue().toByteArray();
    }

    private static Schema recordSchema(String name)
    {
        return SchemaBuilder.record(name).fields()
                .name("b").type().stringType().noDefault()
                .name("a").type().longType().noDefault()
                .endRecord();
    }

    private static Schema reversedRecordSchema(String name)
    {
        return SchemaBuilder.record(name).fields()
                .name("a").type().longType().noDefault()
                .name("b").type().stringType().noDefault()
                .endRecord();
    }

    private static GenericData.Record record(Schema schema, long a, String b)
    {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("a", a);
        record.put("b", b);
        return record;
    }

    private static List<HiveColumnHandle> projectedColumns()
    {
        return List.of(
                HiveColumnHandle.createBaseColumn("a", 0, HiveType.HIVE_LONG, BIGINT, HiveColumnHandle.ColumnType.REGULAR, Optional.empty()),
                HiveColumnHandle.createBaseColumn("b", 1, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty()),
                HiveColumnHandle.createBaseColumn("pk_int", -1, HiveType.HIVE_INT, INTEGER, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.empty()));
    }

    private static PrefilledColumnValues prefilledValues()
    {
        HudiBaseFile baseFile = new HudiBaseFile("s3://bucket/table/file1.parquet", "file1.parquet", 1234, 1700000000123L, 0, 1234);
        HudiSplit split = new HudiSplit(
                baseFile,
                List.of(),
                "001",
                TupleDomain.all(),
                List.of(new HivePartitionKey("pk_int", "42")),
                SplitWeight.standard());
        return PrefilledColumnValues.create(split);
    }
}
