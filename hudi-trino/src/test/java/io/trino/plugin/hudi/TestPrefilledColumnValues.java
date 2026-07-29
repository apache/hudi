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
package io.trino.plugin.hudi;

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.fileSizeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.partitionColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link PrefilledColumnValues}: per-split constant values for partition columns and Trino's
 * hidden metadata columns, delegated to trino-hive's canonical prefilled-column implementation.
 */
class TestPrefilledColumnValues
{
    private static final String FILE_PATH = "s3://bucket/table/year=2020/month=5/file1.parquet";
    private static final long FILE_SIZE = 1234;
    private static final long FILE_MODIFIED_TIME = 1700000000123L;

    @Test
    public void testPartitionKeyValues()
    {
        PrefilledColumnValues values = prefilledValues(
                new HivePartitionKey("pk_string", "abc"),
                new HivePartitionKey("pk_int", "42"),
                new HivePartitionKey("pk_bigint", "123456789012"),
                new HivePartitionKey("pk_date", "2021-12-09"),
                new HivePartitionKey("pk_decimal", "12.34"));

        assertThat(VARCHAR.getSlice(singleValueBlock(values, partitionKey("pk_string", VARCHAR, HiveType.HIVE_STRING)), 0).toStringUtf8())
                .isEqualTo("abc");
        assertThat(INTEGER.getInt(singleValueBlock(values, partitionKey("pk_int", INTEGER, HiveType.HIVE_INT)), 0))
                .isEqualTo(42);
        assertThat(BIGINT.getLong(singleValueBlock(values, partitionKey("pk_bigint", BIGINT, HiveType.HIVE_LONG)), 0))
                .isEqualTo(123456789012L);
        assertThat(DATE.getInt(singleValueBlock(values, partitionKey("pk_date", DATE, HiveType.HIVE_DATE)), 0))
                .isEqualTo((int) LocalDate.parse("2021-12-09").toEpochDay());
        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        assertThat(decimalType.getLong(singleValueBlock(values, partitionKey("pk_decimal", decimalType, HiveType.valueOf("decimal(10,2)"))), 0))
                .isEqualTo(1234);
    }

    @Test
    public void testHiveNullPartitionValue()
    {
        // Trino's HivePartitionKey encodes a null partition value as the literal string "\N"
        PrefilledColumnValues values = prefilledValues(new HivePartitionKey("pk_string", "\\N"));

        Block block = singleValueBlock(values, partitionKey("pk_string", VARCHAR, HiveType.HIVE_STRING));
        assertThat(block.isNull(0)).isTrue();
    }

    @Test
    public void testHiddenColumns()
    {
        PrefilledColumnValues values = prefilledValues(
                new HivePartitionKey("year", "2020"),
                new HivePartitionKey("month", "5"));

        assertThat(VARCHAR.getSlice(singleValueBlock(values, pathColumnHandle()), 0).toStringUtf8())
                .isEqualTo(FILE_PATH);
        assertThat(BIGINT.getLong(singleValueBlock(values, fileSizeColumnHandle()), 0))
                .isEqualTo(FILE_SIZE);
        long packedTimestamp = TIMESTAMP_TZ_MILLIS.getLong(singleValueBlock(values, fileModifiedTimeColumnHandle()), 0);
        assertThat(unpackMillisUtc(packedTimestamp)).isEqualTo(FILE_MODIFIED_TIME);
    }

    @Test
    public void testPartitionNamePreservesKeyOrder()
    {
        // Keys must render in the split's partition-column order, not e.g. hash order
        PrefilledColumnValues values = prefilledValues(
                new HivePartitionKey("year", "2020"),
                new HivePartitionKey("month", "5"),
                new HivePartitionKey("day", "17"));

        assertThat(VARCHAR.getSlice(singleValueBlock(values, partitionColumnHandle()), 0).toStringUtf8())
                .isEqualTo("year=2020/month=5/day=17");
    }

    @Test
    public void testUnknownColumnIsNullFilled()
    {
        PrefilledColumnValues values = prefilledValues();
        HiveColumnHandle dataColumn = HiveColumnHandle.createBaseColumn(
                "some_col", 0, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());

        assertThat(values.isPrefilled(dataColumn)).isFalse();
        Block block = values.toRleBlock(dataColumn, 3);
        assertThat(block.getPositionCount()).isEqualTo(3);
        assertThat(block.isNull(0)).isTrue();
    }

    @Test
    public void testRleBlockShape()
    {
        PrefilledColumnValues values = prefilledValues(new HivePartitionKey("year", "2020"));
        HiveColumnHandle handle = partitionKey("year", VARCHAR, HiveType.HIVE_STRING);

        assertThat(values.isPrefilled(handle)).isTrue();
        Block block = values.toRleBlock(handle, 5);
        assertThat(block).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(block.getPositionCount()).isEqualTo(5);
        assertThat(VARCHAR.getSlice(block, 4).toStringUtf8()).isEqualTo("2020");

        assertThat(values.toRleBlock(handle, 0).getPositionCount()).isEqualTo(0);
    }

    @Test
    public void testAppendTo()
    {
        PrefilledColumnValues values = prefilledValues(new HivePartitionKey("pk_int", "42"));
        HiveColumnHandle handle = partitionKey("pk_int", INTEGER, HiveType.HIVE_INT);

        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, 2);
        values.appendTo(handle, blockBuilder);
        values.appendTo(handle, blockBuilder);
        Block block = blockBuilder.build();

        assertThat(block.getPositionCount()).isEqualTo(2);
        assertThat(INTEGER.getInt(block, 1)).isEqualTo(42);
    }

    private static PrefilledColumnValues prefilledValues(HivePartitionKey... partitionKeys)
    {
        HudiBaseFile baseFile = new HudiBaseFile(FILE_PATH, "file1.parquet", FILE_SIZE, FILE_MODIFIED_TIME, 0, FILE_SIZE);
        HudiSplit split = new HudiSplit(
                baseFile,
                List.of(),
                "001",
                TupleDomain.all(),
                List.of(partitionKeys),
                SplitWeight.standard());
        return PrefilledColumnValues.create(split);
    }

    private static HiveColumnHandle partitionKey(String name, Type type, HiveType hiveType)
    {
        return HiveColumnHandle.createBaseColumn(name, -1, hiveType, type, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.empty());
    }

    private static Block singleValueBlock(PrefilledColumnValues values, HiveColumnHandle handle)
    {
        return values.toRleBlock(handle, 1);
    }
}
