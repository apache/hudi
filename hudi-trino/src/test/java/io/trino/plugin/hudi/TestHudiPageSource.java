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
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiPageSource
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorPageSource.class, HudiPageSource.class);
    }

    @Test
    public void testMergedRecordsAreReturnedInBoundedPages()
            throws Exception
    {
        int rowCount = 20_000;
        String payload = "x".repeat(128);
        Schema schema = SchemaBuilder.record("row").fields()
                .requiredLong("id")
                .requiredString("payload")
                .endRecord();
        AtomicBoolean closed = new AtomicBoolean();
        ClosableIterator<IndexedRecord> records = new ClosableIterator<>()
        {
            private int position;

            @Override
            public boolean hasNext()
            {
                return position < rowCount;
            }

            @Override
            public IndexedRecord next()
            {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("id", (long) position++);
                record.put("payload", payload);
                return record;
            }

            @Override
            public void close()
            {
                closed.set(true);
            }
        };
        List<HiveColumnHandle> columns = List.of(
                HiveColumnHandle.createBaseColumn("id", 0, HiveType.HIVE_LONG, BIGINT, HiveColumnHandle.ColumnType.REGULAR, Optional.empty()),
                HiveColumnHandle.createBaseColumn("payload", 1, HiveType.HIVE_STRING, VARCHAR, HiveColumnHandle.ColumnType.REGULAR, Optional.empty()));
        HudiSplit split = new HudiSplit(new HudiBaseFile("memory:///table/base.parquet", "base.parquet", 0, 0, 0, 0),
                List.of(), "001", TupleDomain.all(), List.of(), SplitWeight.standard());
        int positions = 0;
        int pages = 0;
        try (HudiPageSource source = new HudiPageSource(new EmptyPageSource(), records, columns, PrefilledColumnValues.create(split))) {
            assertThat(source.getMemoryUsage()).isPositive();
            while (!source.isFinished()) {
                var page = source.getNextSourcePage();
                assertThat(page.getPositionCount()).isPositive();
                assertThat(page.getSizeInBytes()).isLessThanOrEqualTo(DEFAULT_MAX_PAGE_SIZE_IN_BYTES + 256L);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(page.getBlock(0), position)).isEqualTo(positions++);
                    assertThat(VARCHAR.getSlice(page.getBlock(1), position).toStringUtf8()).isEqualTo(payload);
                }
                pages++;
            }
        }
        assertThat(positions).isEqualTo(rowCount);
        assertThat(pages).isGreaterThan(1);
        assertThat(closed).isTrue();
    }
}
