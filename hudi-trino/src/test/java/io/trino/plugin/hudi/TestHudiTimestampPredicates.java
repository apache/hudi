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
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hudi.TestingBaseFilePageSource.greaterThan;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.read;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.regularColumn;
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
 * Every read checks that each row holds the instant its id says.
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
    /** {@code ts} as UTC-adjusted TIMESTAMP(MILLIS) holding millis, as written before the field widened to micros. */
    private static Path millisFile;
    /** {@code ts} as UTC-adjusted TIMESTAMP(MICROS) holding millis, as the releases #14161 repairs wrote a millis field. */
    private static Path misstatedFile;
    /** {@code ts} as a bare INT64 holding micros, as written before the long column was promoted to a timestamp (#19384). */
    private static Path promotedFile;
    /** {@code ts} as INT96, as Spark writes under {@code hoodie.parquet.outputtimestamptype=INT96}. */
    private static Path int96File;

    @BeforeAll
    static void writeBaseFiles()
            throws IOException
    {
        microsFile = writeInt64(tempDir.resolve("micros.parquet"), LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS), 1_000_000L);
        millisFile = writeInt64(tempDir.resolve("millis.parquet"), LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS), 1_000L);
        misstatedFile = writeInt64(tempDir.resolve("misstated.parquet"), LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS), 1_000L);
        promotedFile = writeInt64(tempDir.resolve("promoted.parquet"), null, 1_000_000L);
        int96File = tempDir.resolve("int96.parquet");
        assertThat(writeBaseFile(int96File, fileSchema(Types.primitive(INT96, OPTIONAL).named(TS_COLUMN)), ROW_COUNT,
                (group, row) -> group.append(ID_COLUMN, row).append(TS_COLUMN, int96(row))))
                .as("row groups").isGreaterThan(1);
    }

    @Test
    public void testTimestampWithTimeZonePredicatePrunes()
            throws Exception
    {
        assertReads(microsFile, TIMESTAMP_TZ_MICROS, Optional.empty(), true);
    }

    @Test
    public void testMillisFileUnderAMicrosFieldPrunes()
            throws Exception
    {
        // A field widened from millis to micros keeps its older files in millis, read and pruned by their own unit
        assertReads(millisFile, TIMESTAMP_TZ_MICROS, Optional.of(tableSchema(TS_COLUMN, HoodieSchema.createTimestampMicros())), true);
    }

    @Test
    public void testMisstatedUnitIsRepairedAndNotPruned()
            throws Exception
    {
        // The table field, named in another case than the file column, declares the millis the file holds under its
        // MICROS label. Its statistics read a thousand times small, so pruning on them would drop every row group
        assertReads(misstatedFile, TIMESTAMP_TZ_MILLIS, Optional.of(tableSchema("TS", HoodieSchema.createTimestampMillis())), false);
    }

    @Test
    public void testLongPromotedToTimestampIsRepairedAndNotPruned()
            throws Exception
    {
        // The reader refuses a bare INT64 as a timestamp; the field's unit says what the longs count
        assertReads(promotedFile, TIMESTAMP_TZ_MICROS, Optional.of(tableSchema(TS_COLUMN, HoodieSchema.createTimestampMicros())), false);
    }

    @Test
    public void testInt96ReadsAsAnInstantAndPrunes()
            throws Exception
    {
        // Above millisecond precision the domain is rewritten for the INT96 statistics and dictionary
        assertReads(int96File, TIMESTAMP_TZ_MICROS, Optional.empty(), true);
        // At millisecond precision it is dropped: the statistics round sub-millisecond nanos, the reader truncates them
        assertReads(int96File, TIMESTAMP_TZ_MILLIS, Optional.empty(), false);
    }

    /**
     * Reads {@code ts > THRESHOLD_SECONDS} from {@code file} as {@code type}, checking every row holds the instant its id
     * says, the matching rows, and whether row groups were pruned.
     */
    private static void assertReads(Path file, TimestampWithTimeZoneType type, Optional<HoodieSchema> tableSchema, boolean pruned)
            throws Exception
    {
        HiveColumnHandle ts = regularColumn(TS_COLUMN, 1, HiveType.HIVE_TIMESTAMP, type);
        Object threshold = type.isShort()
                ? packDateTimeWithZone(THRESHOLD_SECONDS * 1000, UTC_KEY)
                : LongTimestampWithTimeZone.fromEpochMillisAndFraction(THRESHOLD_SECONDS * 1000, 0, UTC_KEY);

        MaterializedResult result = read(file, List.of(regularColumn(ID_COLUMN, 0, HiveType.HIVE_INT, INTEGER), ts), greaterThan(ts, threshold),
                true, DynamicFilter.EMPTY, tableSchema);

        if (pruned) {
            assertThat(result.getRowCount()).as("rows read").isLessThan(ROW_COUNT);
        }
        else {
            assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        }
        assertThat(result.getMaterializedRows()).allSatisfy(row -> assertThat(row.getField(1)).isEqualTo(utc((Integer) row.getField(0))));
        assertThat(result.getMaterializedRows().stream()
                .map(row -> (ZonedDateTime) row.getField(1))
                .filter(value -> value.toEpochSecond() > THRESHOLD_SECONDS)
                .count())
                .as("rows after the threshold")
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    private static Path writeInt64(Path file, LogicalTypeAnnotation annotation, long unitsPerSecond)
            throws IOException
    {
        assertThat(writeBaseFile(file, fileSchema(Types.primitive(INT64, OPTIONAL).as(annotation).named(TS_COLUMN)), ROW_COUNT,
                (group, row) -> group.append(ID_COLUMN, row).append(TS_COLUMN, row * unitsPerSecond)))
                .as("row groups").isGreaterThan(1);
        return file;
    }

    private static MessageType fileSchema(org.apache.parquet.schema.Type tsColumn)
    {
        return new MessageType("hudi_base_file", Types.primitive(INT32, OPTIONAL).named(ID_COLUMN), tsColumn);
    }

    private static HoodieSchema tableSchema(String tsField, HoodieSchema timestamp)
    {
        return HoodieSchema.createRecord("t", null, null, List.of(
                HoodieSchemaField.of(ID_COLUMN, HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieSchema.NULL_VALUE),
                HoodieSchemaField.of(tsField, HoodieSchema.createNullable(timestamp), null, HoodieSchema.NULL_VALUE)));
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
}
