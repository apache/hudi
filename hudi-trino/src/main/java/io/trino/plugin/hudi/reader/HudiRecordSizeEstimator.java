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

import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.SizeEstimator;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;

/**
 * Estimates the heap size of a record in the merge buffer from its key and field values, without walking the
 * object graph. The record schema is shared by every record of the file group, so it is not counted; a graph walk
 * of the record would count it once per record (about 13 KB for TPC-DS store_sales).
 * <p>
 * Values are sized by kind: Trino objects from {@code Type.getObjectValue} for rows read from native log files,
 * and Avro values for rows decoded from Avro log blocks (see {@link HudiRecordSerializer}).
 */
final class HudiRecordSizeEstimator
        implements SizeEstimator<BufferedRecord<IndexedRecord>>
{
    private static final int BUFFERED_RECORD_INSTANCE_SIZE = instanceSize(BufferedRecord.class);
    private static final int RECORD_INSTANCE_SIZE = instanceSize(GenericData.Record.class);
    private static final int UTF8_INSTANCE_SIZE = instanceSize(Utf8.class);
    private static final int BYTE_BUFFER_INSTANCE_SIZE = instanceSize(ByteBuffer.class);
    private static final int FIXED_INSTANCE_SIZE = instanceSize(GenericData.Fixed.class);
    private static final int SQL_DECIMAL_INSTANCE_SIZE = instanceSize(SqlDecimal.class) + instanceSize(BigInteger.class);
    private static final int SQL_VARBINARY_INSTANCE_SIZE = instanceSize(SqlVarbinary.class);
    // Boxed primitives, SqlDate, SqlTimestamp: SqlTimestamp is the largest of these fixed-width values
    private static final int FIXED_WIDTH_VALUE_SIZE = instanceSize(SqlTimestamp.class);

    @Override
    public long sizeEstimate(BufferedRecord<IndexedRecord> record)
    {
        return BUFFERED_RECORD_INSTANCE_SIZE
                + estimatedSizeOf(record.getRecordKey())
                + sizeOfValue(record.getOrderingValue())
                + sizeOfValue(record.getRecord());
    }

    private static long sizeOfValue(Object value)
    {
        return switch (value) {
            case null -> 0;
            case String string -> estimatedSizeOf(string);
            case Utf8 utf8 -> UTF8_INSTANCE_SIZE + sizeOfByteArray(utf8.getByteLength());
            case ByteBuffer buffer -> BYTE_BUFFER_INSTANCE_SIZE + sizeOfByteArray(buffer.capacity());
            case GenericFixed fixed -> FIXED_INSTANCE_SIZE + sizeOfByteArray(fixed.bytes().length);
            case IndexedRecord record -> sizeOfRecord(record);
            case SqlDecimal decimal -> SQL_DECIMAL_INSTANCE_SIZE + sizeOfIntArray(decimal.getUnscaledValue().bitLength() / Integer.SIZE + 1);
            case SqlVarbinary varbinary -> SQL_VARBINARY_INSTANCE_SIZE + sizeOfByteArray(varbinary.getBytes().length);
            case List<?> list -> estimatedSizeOf(list, HudiRecordSizeEstimator::sizeOfValue);
            case Map<?, ?> map -> estimatedSizeOf(map, HudiRecordSizeEstimator::sizeOfValue, HudiRecordSizeEstimator::sizeOfValue);
            default -> FIXED_WIDTH_VALUE_SIZE;
        };
    }

    private static long sizeOfRecord(IndexedRecord record)
    {
        int fieldCount = record.getSchema().getFields().size();
        long size = RECORD_INSTANCE_SIZE + sizeOfObjectArray(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            size += sizeOfValue(record.get(i));
        }
        return size;
    }
}
