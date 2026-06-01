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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.Map;

/**
 * Custom RecordContext for Trino that overrides getValue to use column position
 * mapping from the Trino page source rather than Avro schema field positions.
 */
public class HudiTrinoRecordContext
        extends AvroRecordContext
{
    private final Map<String, Integer> colToPosMap;

    public HudiTrinoRecordContext(HoodieTableConfig tableConfig, String payloadClass, Map<String, Integer> colToPosMap)
    {
        super(tableConfig, payloadClass);
        this.colToPosMap = colToPosMap;
    }

    @Override
    public Object getValue(IndexedRecord record, Schema schema, String fieldName)
    {
        if (colToPosMap.containsKey(fieldName)) {
            Object value = record.get(colToPosMap.get(fieldName));
            // Avro log file records have Utf8 string values, but Hudi's merge code
            // (FileGroupRecordBuffer.merge) casts record keys to String directly.
            // Convert Utf8 to String to avoid ClassCastException during merge.
            if (value instanceof org.apache.avro.util.Utf8) {
                return value.toString();
            }
            return value;
        }
        else {
            // record doesn't have the queried field, return null
            return null;
        }
    }
}
