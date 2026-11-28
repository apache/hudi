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
package io.trino.plugin.hudi.reader;

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.OrderingValues;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestHudiVectorizedMorPageSource
{
    private static final HoodieSchema REQUIRED_SCHEMA = HoodieSchema.createRecord("required", null, null, List.of(
            HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.LONG))));

    @Test
    void testPositionMatcherWalksPagesWithGapsInTheRowIndex()
    {
        HudiVectorizedMorPageSource.LogRecordMatcher matcher = HudiVectorizedMorPageSource.logRecordMatcher(records(12L, 3L, 1L, 6L, 40L), true, REQUIRED_SCHEMA).orElseThrow();

        // Rows 3 and 4 were pruned from the first page; the second page has no log position in its range
        List<String> merged = new ArrayList<>();
        int[] dropped = matcher.matchPage(rowIndex(0, 1, 2, 5, 6, 9), 6, (_, position, record) -> merged.add(position + ":" + record.getRecordKey()) && false, null);
        assertThat(merged).containsExactly("1:p1", "4:p6");
        assertThat(dropped).startsWith(2, 1, 4);
        assertThat(matcher.matchPage(rowIndex(13, 14, 15), 3, (_, _, _) -> false, null)).isNull();

        // A consecutive page maps a row index to its position directly; the base row of p40 is kept
        merged.clear();
        dropped = matcher.matchPage(rowIndex(38, 39, 40, 41), 4, (_, position, record) -> !merged.add(position + ":" + record.getRecordKey()) || true, null);
        assertThat(merged).containsExactly("2:p40");
        assertThat(dropped[0]).isZero();

        List<String> leftovers = new ArrayList<>();
        Iterator<BufferedRecord<TrinoRow>> iterator = matcher.leftovers();
        iterator.forEachRemaining(record -> leftovers.add(record.getRecordKey()));
        assertThat(leftovers).containsExactly("p3", "p12");
    }

    @Test
    void testMatcherNeedsOneKindOfKey()
    {
        Map<Serializable, BufferedRecord<TrinoRow>> mixed = records(1L);
        mixed.put("key", BufferedRecords.createDelete("key", OrderingValues.getDefault()));
        assertThat(HudiVectorizedMorPageSource.logRecordMatcher(mixed, true, REQUIRED_SCHEMA)).isEmpty();
        assertThat(HudiVectorizedMorPageSource.logRecordMatcher(mixed, false, REQUIRED_SCHEMA)).isEmpty();
        assertThat(HudiVectorizedMorPageSource.logRecordMatcher(Map.of(), true, REQUIRED_SCHEMA)).isPresent();
    }

    @Test
    void testMatcherNeedsRecordsWithEveryRequiredField()
    {
        // A record of a partial update block holds only the fields it updates
        HoodieSchema partialSchema = HoodieSchema.createRecord("partial", null, null, List.of(
                HoodieSchemaField.of("key", HoodieSchema.create(HoodieSchemaType.STRING))));
        TrinoRow partialRow = TrinoRow.ofSingleValueBlocks(new TrinoRow.Layout(partialSchema, List.of(VARCHAR)), new Block[] {VARCHAR.createNullBlock()});
        Map<Serializable, BufferedRecord<TrinoRow>> records = records(1L);
        records.put(2L, new BufferedRecord<>("k2", OrderingValues.getDefault(), partialRow, null, null));
        assertThat(HudiVectorizedMorPageSource.logRecordMatcher(records, true, REQUIRED_SCHEMA)).isEmpty();

        TrinoRow completeRow = TrinoRow.ofSingleValueBlocks(
                new TrinoRow.Layout(REQUIRED_SCHEMA, List.of(VARCHAR, BIGINT)),
                new Block[] {VARCHAR.createNullBlock(), BIGINT.createNullBlock()});
        records.put(2L, new BufferedRecord<>("k2", OrderingValues.getDefault(), completeRow, null, null));
        assertThat(HudiVectorizedMorPageSource.logRecordMatcher(records, true, REQUIRED_SCHEMA)).isPresent();
    }

    private static Map<Serializable, BufferedRecord<TrinoRow>> records(Long... positions)
    {
        Map<Serializable, BufferedRecord<TrinoRow>> records = new HashMap<>();
        for (Long position : positions) {
            records.put(position, BufferedRecords.createDelete("p" + position, OrderingValues.getDefault()));
        }
        return records;
    }

    private static LongArrayBlock rowIndex(long... values)
    {
        return new LongArrayBlock(values.length, Optional.empty(), values);
    }
}
