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
package io.trino.plugin.hudi.testing;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;

import java.io.IOException;

/**
 * Test-only custom {@link HoodieRecordMerger} used to verify that the Hudi Trino connector resolves and
 * applies a user-supplied record merger for Merge-On-Read tables whose record merge mode is {@code CUSTOM}.
 * <p>
 * The merge policy is deliberately distinct from the built-in mergers and depends only on the record key
 * (which is projection-independent): for keys ending in an odd digit it keeps the newer record, otherwise it
 * keeps the older one. This is neither the built-in newest-wins (event-time) behavior nor the base-only view,
 * so its effect is observable in query results regardless of which columns a query projects.
 * <p>
 * Operates on Avro records ({@link HoodieRecord.HoodieRecordType#AVRO}), which is the record type the Trino
 * reader context uses ({@code EngineType.JAVA}).
 */
public class KeyBasedTestRecordMerger
        implements HoodieRecordMerger
{
    /**
     * Unique strategy id identifying this custom merger. The test table's
     * {@code hoodie.record.merge.strategy.id} is set to this value so the reader resolves this implementation.
     */
    public static final String MERGE_STRATEGY_ID = "f9b5c1a2-0d3e-4c7a-8b6f-2a1e4d9c0b7a";

    @Override
    public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props)
            throws IOException
    {
        // Deletes are passed through so tombstones still win; this test does not exercise deletes.
        if (older == null || older.isDelete() || newer.isDelete()) {
            return newer;
        }
        return keepNewer(newer.getRecordKey()) ? newer : older;
    }

    private static boolean keepNewer(String recordKey)
    {
        char last = recordKey.charAt(recordKey.length() - 1);
        return Character.isDigit(last) && ((last - '0') % 2 == 1);
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType()
    {
        return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy()
    {
        return MERGE_STRATEGY_ID;
    }
}
