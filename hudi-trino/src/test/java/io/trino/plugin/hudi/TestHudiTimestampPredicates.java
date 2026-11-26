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
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.read;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.writeBaseFile;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Predicates on UTC-adjusted timestamps, which the connector types {@code timestamp with time zone}, over the base file
 * layouts a Hudi table can hold. Each file stores row {@code i} at {@code i} seconds after the epoch over many row
 * groups, so pruning is observable as in {@code TestHudiEvolvedColumnPredicates}: a predicate that survives pushdown
 * reads fewer rows than the file holds, one that was dropped reads all of them, and the engine re-applies it either way.
 */
class TestHudiTimestampPredicates
{
    private static final String ID_COLUMN = "id";
    private static final String TS_COLUMN = "ts";
    private static final int ROW_COUNT = 1000;
    private static final long THRESHOLD_SECONDS = 900;
    private static final int MATCHING_ROW_COUNT = (int) (ROW_COUNT - THRESHOLD_SECONDS - 1);
    private static final int EPOCH_JULIAN_DAY = 2_440_588;

    @TempDir
    static Path tempDir;

    /** {@code ts} as UTC-adjusted TIMESTAMP(MICROS) holding micros, as Spark writes a TIMESTAMP. */
    private static Path microsFile;
    /** {@code ts} as UTC-adjusted TIMESTAMP(MICROS) holding millis, as the releases #14161 repairs wrote a millis field. */
    private static Path misstatedFile;
    /** {@code ts} as INT96, as Spark writes under {@code hoodie.parquet.outputtimestamptype=INT96}. */
    private static Path int96File;

    @BeforeAll
    static void writeBaseFiles()
            throws IOException
    {
        microsFile = tempDir.resolve("micros.parquet");
        misstatedFile = tempDir.resolve("misstated.parquet");
        int96File = tempDir.resolve("int96.parquet");
        MessageType int64Schema = fileSchema(Types.primitive(INT64, OPTIONAL).as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS)).named(TS_COLUMN));
        assertThat(writeBaseFile(microsFile, int64Schema, ROW_COUNT, (group, row) -> group.append(ID_COLUMN, row).append(TS_COLUMN, row * 1_000_000L)))
                .as("row groups").isGreaterThan(1);
        assertThat(writeBaseFile(misstatedFile, int64Schema, ROW_COUNT, (group, row) -> group.append(ID_COLUMN, row).append(TS_COLUMN, row * 1_000L)))
                .as("row groups").isGreaterThan(1);
        MessageType int96Schema = fileSchema(Types.primitive(INT96, OPTIONAL).named(TS_COLUMN));
        writeBaseFile(int96File, int96Schema, ROW_COUNT, (group, row) -> group.append(ID_COLUMN, row).append(TS_COLUMN, int96(row)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTimestampWithTimeZonePredicatePrunes(boolean useParquetColumnNames)
            throws Exception
    {
        HiveColumnHandle ts = column(TS_COLUMN, TIMESTAMP_TZ_MICROS);
        Domain afterThreshold = Domain.create(ValueSet.ofRanges(Range.greaterThan(TIMESTAMP_TZ_MICROS,
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(THRESHOLD_SECONDS * 1000, 0, UTC_KEY))), false);

        MaterializedResult result = read(microsFile, List.of(column(ID_COLUMN, INTEGER), ts), TupleDomain.withColumnDomains(Map.of(ts, afterThreshold)),
                useParquetColumnNames, DynamicFilter.EMPTY);

        assertThat(result.getRowCount()).as("rows read").isLessThan(ROW_COUNT);
        assertThat(secondsOver(result)).as("rows after the threshold").isEqualTo(MATCHING_ROW_COUNT);
    }

    /**
     * The repair and the statistics guard both resolve the file column from the handle, whether columns are matched by
     * name or by position.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMisstatedUnitIsRepairedAndNotPruned(boolean useParquetColumnNames)
            throws Exception
    {
        HoodieSchema tableSchema = HoodieSchema.createRecord("t", null, null, List.of(
                HoodieSchemaField.of(ID_COLUMN, HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of(TS_COLUMN, HoodieSchema.createNullable(HoodieSchema.createTimestampMillis()), null, HoodieSchema.NULL_VALUE)));
        HiveColumnHandle ts = column(TS_COLUMN, TIMESTAMP_TZ_MILLIS);
        Domain afterThreshold = Domain.create(ValueSet.ofRanges(Range.greaterThan(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(THRESHOLD_SECONDS * 1000, UTC_KEY))), false);

        MaterializedResult result = read(misstatedFile, List.of(column(ID_COLUMN, INTEGER), ts), TupleDomain.withColumnDomains(Map.of(ts, afterThreshold)),
                useParquetColumnNames, DynamicFilter.EMPTY, Optional.of(tableSchema));

        // Read as the millis they are, and the statistics, which read a thousand times small, prune nothing
        assertThat(result.getMaterializedRows().get(7).getField(1)).as("row 7").isEqualTo(utc(7));
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(secondsOver(result)).as("rows after the threshold").isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testInt96ReadsAsAnInstant()
            throws Exception
    {
        HiveColumnHandle ts = column(TS_COLUMN, TIMESTAMP_TZ_MILLIS);
        Domain afterThreshold = Domain.create(ValueSet.ofRanges(Range.greaterThan(TIMESTAMP_TZ_MILLIS, packDateTimeWithZone(THRESHOLD_SECONDS * 1000, UTC_KEY))), false);

        MaterializedResult result = read(int96File, List.of(column(ID_COLUMN, INTEGER), ts), TupleDomain.withColumnDomains(Map.of(ts, afterThreshold)),
                true, DynamicFilter.EMPTY);

        // Row groups whose statistics hold only rows before the threshold may be pruned; every row read is the instant
        // its id says
        assertThat(result.getMaterializedRows()).allSatisfy(row -> assertThat(row.getField(1)).isEqualTo(utc((Integer) row.getField(0))));
        assertThat(secondsOver(result)).as("rows after the threshold").isEqualTo(MATCHING_ROW_COUNT);
    }

    private static MessageType fileSchema(org.apache.parquet.schema.Type tsColumn)
    {
        return new MessageType("hudi_base_file", Types.primitive(INT32, OPTIONAL).named(ID_COLUMN), tsColumn);
    }

    private static HiveColumnHandle column(String name, Type type)
    {
        return column(name, name.equals(ID_COLUMN) ? 0 : 1, type);
    }

    private static HiveColumnHandle column(String name, int physicalIndex, Type type)
    {
        HiveType hiveType = type.equals(INTEGER) ? HiveType.HIVE_INT : HiveType.HIVE_TIMESTAMP;
        return createBaseColumn(name, physicalIndex, hiveType, type, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    }

    /** INT96: nanos of the day, then the julian day, little-endian. */
    private static Binary int96(int epochSeconds)
    {
        ByteBuffer buffer = ByteBuffer.allocate(12).order(LITTLE_ENDIAN)
                .putLong(epochSeconds * 1_000_000_000L)
                .putInt(EPOCH_JULIAN_DAY);
        return Binary.fromConstantByteArray(buffer.array());
    }

    private static ZonedDateTime utc(int epochSeconds)
    {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneId.of("UTC"));
    }

    private static long secondsOver(MaterializedResult result)
    {
        return result.getMaterializedRows().stream()
                .map(row -> (ZonedDateTime) row.getField(1))
                .filter(value -> value != null && value.toEpochSecond() > THRESHOLD_SECONDS)
                .count();
    }
}
