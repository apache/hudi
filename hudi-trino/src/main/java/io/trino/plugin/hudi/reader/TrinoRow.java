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
import io.trino.spi.type.Type;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * One position of Trino blocks, viewed as a record of a {@link HoodieSchema}: the record type the file-group reader
 * merges when {@code hudi.schema-column-types.enabled} is on (see {@link TrinoRecordContext}).
 * <p>
 * Block {@code i} holds field {@code i} of the layout's schema. Rows read from a page share one block array per page,
 * so a base row costs this view object and nothing else; the array is never written after the row is built.
 */
public final class TrinoRow
{
    private static final int INSTANCE_SIZE = instanceSize(TrinoRow.class);

    private final Layout layout;
    private final Block[] blocks;
    private final int position;
    private final boolean compact;

    /**
     * A view of {@code position} in blocks that may hold other rows, such as a page read from a file.
     */
    TrinoRow(Layout layout, Block[] blocks, int position)
    {
        this(layout, blocks, position, false);
    }

    private TrinoRow(Layout layout, Block[] blocks, int position, boolean compact)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.blocks = requireNonNull(blocks, "blocks is null");
        checkArgument(blocks.length == layout.getFieldCount(), "Got %s blocks for %s fields", blocks.length, layout.getFieldCount());
        this.position = position;
        this.compact = compact;
    }

    /**
     * A row over blocks that each hold this row's value alone, copied out of other blocks or built from values.
     */
    static TrinoRow ofSingleValueBlocks(Layout layout, Block[] singleValueBlocks)
    {
        return new TrinoRow(layout, singleValueBlocks, 0, true);
    }

    /**
     * A view with another layout over the same blocks and position, compact when this row is.
     */
    TrinoRow withLayout(Layout layout, Block[] blocks)
    {
        return new TrinoRow(layout, blocks, position, compact);
    }

    /**
     * The block array itself, for readers that cache per page by its identity. Never written.
     */
    Block[] blocks()
    {
        return blocks;
    }

    public Layout getLayout()
    {
        return layout;
    }

    public HoodieSchema getSchema()
    {
        return layout.getSchema();
    }

    /**
     * The block holding field {@code field} of the layout's schema; the row's value is at {@link #getPosition()}.
     */
    public Block getBlock(int field)
    {
        return blocks[field];
    }

    public int getPosition()
    {
        return position;
    }

    public boolean isNull(int field)
    {
        return blocks[field].isNull(position);
    }

    /**
     * Whether every block holds this row's value alone, so the row pins no other rows' data. Known from how the row
     * was built, not from its blocks: a one-position dictionary or RLE block, or a region of a larger block, still
     * retains other rows' data.
     */
    public boolean isCompact()
    {
        return compact;
    }

    /**
     * Copies the row's value out of each block into a block of its own.
     */
    public TrinoRow compact()
    {
        if (isCompact()) {
            return this;
        }
        Block[] singleValueBlocks = new Block[blocks.length];
        for (int field = 0; field < blocks.length; field++) {
            singleValueBlocks[field] = blocks[field].getSingleValueBlock(position);
        }
        return ofSingleValueBlocks(layout, singleValueBlocks);
    }

    /**
     * The memory the row retains: all of every block it views, so a compact row reports its own values only.
     */
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(blocks);
        for (Block block : blocks) {
            size += block.getRetainedSizeInBytes();
        }
        return size;
    }

    /**
     * A {@link HoodieSchema} and the Trino type of each of its fields. Shared by every row of the same shape; a reader
     * caches what it compiles per layout by identity.
     */
    public static final class Layout
    {
        private final HoodieSchema schema;
        private final List<Type> types;

        public Layout(HoodieSchema schema, List<Type> types)
        {
            this.schema = requireNonNull(schema, "schema is null");
            this.types = List.copyOf(requireNonNull(types, "types is null"));
            checkArgument(this.types.size() == schema.getFields().size(), "Got %s types for %s fields", this.types.size(), schema.getFields().size());
        }

        public HoodieSchema getSchema()
        {
            return schema;
        }

        public int getFieldCount()
        {
            return types.size();
        }

        public Type getType(int field)
        {
            return types.get(field);
        }

        public HoodieSchemaField getField(int field)
        {
            return schema.getFields().get(field);
        }

        /**
         * The index of the field named exactly {@code name}, as hudi-common names merge fields, or -1.
         */
        public int fieldIndex(String name)
        {
            return schema.getField(name).map(HoodieSchemaField::pos).orElse(-1);
        }
    }
}
