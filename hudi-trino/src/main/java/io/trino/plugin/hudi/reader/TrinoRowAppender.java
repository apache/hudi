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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.AvroToBlock;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.ValueToAvro;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.hudi.common.schema.HoodieSchema;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiUtil.getFieldFromSchema;
import static io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.toAvro;
import static io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.toBlock;
import static java.util.Objects.requireNonNull;

/**
 * Appends merged {@link TrinoRow}s to the output page: each data column is copied from the row's block at the row's
 * position, by name, and partition and hidden columns come from {@link PrefilledColumnValues}.
 * <p>
 * A row field whose Trino type is not the column's, as in a payload result written in an older schema, is converted
 * through its Avro value the way {@code HoodieSchemaAvroSerializer.buildRecordInPage} converts a record field, so it is
 * widened or rejected with the field named, never copied as a block of the wrong type.
 */
public class TrinoRowAppender
{
    private final List<HiveColumnHandle> columnHandles;
    private final PrefilledColumnValues prefilledColumnValues;
    // The rows of a split come in a few layouts (the base file's, each log file's, payload results'); how each output
    // channel is filled is resolved once per layout
    private final Map<TrinoRow.Layout, ChannelSource[]> channelSources = new IdentityHashMap<>();
    private TrinoRow.Layout lastLayout;
    private ChannelSource[] lastChannelSources;

    public TrinoRowAppender(List<HiveColumnHandle> columnHandles, PrefilledColumnValues prefilledColumnValues)
    {
        this.columnHandles = List.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.prefilledColumnValues = requireNonNull(prefilledColumnValues, "prefilledColumnValues is null");
    }

    public void append(PageBuilder pageBuilder, TrinoRow row)
    {
        pageBuilder.declarePosition();
        ChannelSource[] sources = channelSources(row.getLayout());
        for (int channel = 0; channel < sources.length; channel++) {
            BlockBuilder output = pageBuilder.getBlockBuilder(channel);
            ChannelSource source = sources[channel];
            if (source == null) {
                prefilledColumnValues.appendTo(columnHandles.get(channel), output);
                continue;
            }
            Block block = row.getBlock(source.field());
            int position = row.getPosition();
            if (source.reader() == null) {
                // The row's block holds the column's type, so its value block appends as is
                output.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
            }
            else if (block.isNull(position)) {
                output.appendNull();
            }
            else {
                source.writer().append(source.reader().convert(block, position), output);
            }
        }
    }

    private ChannelSource[] channelSources(TrinoRow.Layout layout)
    {
        if (layout != lastLayout) {
            lastChannelSources = channelSources.computeIfAbsent(layout, this::resolveChannelSources);
            lastLayout = layout;
        }
        return lastChannelSources;
    }

    /**
     * For each output channel, the row field it is filled from, or null for a prefilled column.
     */
    private ChannelSource[] resolveChannelSources(TrinoRow.Layout layout)
    {
        ChannelSource[] sources = new ChannelSource[columnHandles.size()];
        for (int channel = 0; channel < sources.length; channel++) {
            HiveColumnHandle columnHandle = columnHandles.get(channel);
            if (prefilledColumnValues.isPrefilled(columnHandle)) {
                continue;
            }
            // Matches the column by name, case-insensitively, and fails the way the Avro path fails on a column the
            // record lacks; Avro field positions are HoodieSchema field positions
            int field = getFieldFromSchema(columnHandle.getName(), layout.getSchema().toAvroSchema()).pos();
            Type rowType = layout.getType(field);
            Type columnType = columnHandle.getType();
            if (rowType.equals(columnType)) {
                sources[channel] = new ChannelSource(field, null, null);
            }
            else {
                HoodieSchema fieldSchema = layout.getField(field).schema();
                String fieldName = columnHandle.getName();
                sources[channel] = new ChannelSource(field, toAvro(fieldName, fieldSchema, rowType), toBlock(fieldName, fieldSchema, columnType));
            }
        }
        return sources;
    }

    /**
     * The row field an output channel is filled from; {@code reader} and {@code writer} convert a value whose Trino
     * type is not the column's, and are null when the block appends as is.
     */
    private record ChannelSource(int field, ValueToAvro reader, AvroToBlock writer) {}
}
