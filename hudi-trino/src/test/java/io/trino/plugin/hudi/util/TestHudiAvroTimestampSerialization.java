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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated("Changes the JVM default time zone")
public class TestHudiAvroTimestampSerialization
{
    @ParameterizedTest
    @ValueSource(longs = {-1, -999999, -1000000, -1000001, 0, 1})
    public void testTimestampAroundEpoch(long epochMicros)
    {
        BlockBuilder builder = TIMESTAMP_NANOS.createBlockBuilder(null, 1);
        int picosOfMicro = 123000;
        HudiAvroSerializer.appendTo(TIMESTAMP_NANOS, SqlTimestamp.newInstance(9, epochMicros, picosOfMicro), builder);

        SqlTimestamp actual = (SqlTimestamp) TIMESTAMP_NANOS.getObjectValue(builder.build(), 0);
        assertThat(actual.getEpochMicros()).isEqualTo(epochMicros);
        assertThat(actual.getPicosOfMicros()).isEqualTo(picosOfMicro);
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Europe/Belgrade", "America/Los_Angeles", "Asia/Kathmandu"})
    public void testTimestampRoundTripIgnoresJvmTimeZone(String timeZone)
    {
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            HiveColumnHandle column = HiveColumnHandle.createBaseColumn("ts", 0, HiveType.HIVE_TIMESTAMP,
                    TIMESTAMP_NANOS, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
            Schema schema = SchemaBuilder.record("timestamps").fields().requiredLong("ts").endRecord();
            HudiAvroSerializer serializer = new HudiAvroSerializer(List.of(column), prefilledValues(), schema);
            for (String value : List.of(
                    "1969-12-31T23:59:59", "1970-01-01T00:00:01",
                    "2026-01-15T12:00:00", "2026-07-15T12:00:00",
                    "2026-03-29T02:30:00", "2026-10-25T02:30:00",
                    "2026-03-08T02:30:00", "2026-11-01T01:30:00")) {
                long epochMicros = LocalDateTime.parse(value).toEpochSecond(ZoneOffset.UTC) * 1_000_000;
                int picosOfMicro = 123000;
                BlockBuilder input = TIMESTAMP_NANOS.createBlockBuilder(null, 1);
                TIMESTAMP_NANOS.writeObject(input, new LongTimestamp(epochMicros, picosOfMicro));
                Page page = new Page(input.build());
                PageBuilder output = new PageBuilder(List.of(TIMESTAMP_NANOS));
                serializer.buildRecordInPage(output, serializer.serialize(page, 0));
                assertThat(TIMESTAMP_NANOS.getObjectValue(output.build().getBlock(0), 0))
                        .isEqualTo(TIMESTAMP_NANOS.getObjectValue(page.getBlock(0), 0));

                // Avro log blocks carry epoch microseconds directly, without a zone conversion either.
                BlockBuilder logOutput = TIMESTAMP_NANOS.createBlockBuilder(null, 1);
                HudiAvroSerializer.appendTo(TIMESTAMP_NANOS, epochMicros, logOutput);
                assertThat(TIMESTAMP_NANOS.getObjectValue(logOutput.build(), 0))
                        .isEqualTo(SqlTimestamp.newInstance(9, epochMicros, 0));
            }
        }
        finally {
            TimeZone.setDefault(original);
        }
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
