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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.ObjLongConsumer;
import java.util.function.UnaryOperator;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * The page source of a vectorized MOR read: the pages of the base file pass through as they are read, minus the rows a
 * log record replaces or deletes, and the merged log records are appended to pages of their own.
 * <p>
 * The log records of the file slice are loaded before the base file is opened. With positional merge they are keyed by
 * the base file row they replace and matched through the parquet row index, so a base page with no log position
 * between its first and last row index is returned without looking at its rows, and the record key is never read.
 * Without positions they are matched by the {@code _hoodie_record_key} of each base row.
 */
public final class HudiVectorizedMorPageSource
        implements ConnectorPageSource
{
    /**
     * Connector metric counting the splits read by the vectorized MOR read.
     */
    public static final String VECTORIZED_MOR_SPLITS = "hudiVectorizedMorSplits";

    private static final long BUFFERED_RECORD_INSTANCE_SIZE = instanceSize(BufferedRecord.class);

    /**
     * Connector metric counting the vectorized MOR splits whose log records matched base rows by position.
     */
    public static final String POSITION_MERGE_SPLITS = "hudiPositionMergeSplits";

    private final ConnectorPageSource basePageSource;
    private final LogRecordMatcher matcher;
    private final int matchChannel;
    private final RecordMergeMode mergeMode;
    private final BufferedRecordMerger<TrinoRow> merger;
    private final Optional<BaseOrderingValues> baseOrderingValues;
    private final Optional<UnaryOperator<TrinoRow>> outputConverter;
    private final List<HiveColumnHandle> outputColumns;
    // Output channel to base page channel, or -1 for a prefilled column
    private final int[] outputToBaseChannel;
    private final PrefilledColumnValues prefilledColumnValues;
    private final PageBuilder pageBuilder;
    private final TrinoRowAppender appender;
    private final long logRecordsRetainedBytes;
    private final Metrics metrics;
    private final ArrayDeque<SourcePage> ready = new ArrayDeque<>();

    private boolean baseFinished;
    private Iterator<BufferedRecord<TrinoRow>> leftovers;
    private boolean finished;

    private HudiVectorizedMorPageSource(
            ConnectorPageSource basePageSource,
            LogRecordMatcher matcher,
            int matchChannel,
            RecordMergeMode mergeMode,
            BufferedRecordMerger<TrinoRow> merger,
            Optional<BaseOrderingValues> baseOrderingValues,
            Optional<UnaryOperator<TrinoRow>> outputConverter,
            List<HiveColumnHandle> outputColumns,
            int[] outputToBaseChannel,
            PrefilledColumnValues prefilledColumnValues)
    {
        this.basePageSource = requireNonNull(basePageSource, "basePageSource is null");
        this.matcher = requireNonNull(matcher, "matcher is null");
        this.matchChannel = matchChannel;
        this.mergeMode = requireNonNull(mergeMode, "mergeMode is null");
        this.merger = requireNonNull(merger, "merger is null");
        this.baseOrderingValues = requireNonNull(baseOrderingValues, "baseOrderingValues is null");
        this.outputConverter = requireNonNull(outputConverter, "outputConverter is null");
        this.outputColumns = List.copyOf(outputColumns);
        this.outputToBaseChannel = outputToBaseChannel.clone();
        this.prefilledColumnValues = requireNonNull(prefilledColumnValues, "prefilledColumnValues is null");
        this.pageBuilder = new PageBuilder(outputColumns.stream().map(HiveColumnHandle::getType).toList());
        this.appender = new TrinoRowAppender(outputColumns, prefilledColumnValues);
        this.logRecordsRetainedBytes = matcher.retainedSizeInBytes();
        // A split without log records takes the positional path, but merges nothing by position
        this.metrics = new Metrics(matcher instanceof PositionMatcher positionMatcher && positionMatcher.positions.length > 0
                ? Map.of(VECTORIZED_MOR_SPLITS, new LongCount(1), POSITION_MERGE_SPLITS, new LongCount(1))
                : Map.of(VECTORIZED_MOR_SPLITS, new LongCount(1)));
    }

    /**
     * The log records of a file slice, as {@link org.apache.hudi.common.table.read.HoodieFileGroupReader#getLogRecordsByPositionOrKey}
     * returns them, ready to be matched to base rows: by row position when {@code byPosition} and every key is a
     * position, by record key when every key is a record key. Empty when the keys mix the two, as after a fallback to
     * key-based merging that kept position deletes, or when a record lacks a field of {@code requiredSchema}, as the
     * records of a partial update block do.
     */
    public static Optional<LogRecordMatcher> logRecordMatcher(Map<Serializable, BufferedRecord<TrinoRow>> records, boolean byPosition, HoodieSchema requiredSchema)
    {
        Set<TrinoRow.Layout> completeLayouts = Collections.newSetFromMap(new IdentityHashMap<>());
        for (BufferedRecord<TrinoRow> record : records.values()) {
            TrinoRow row = record.getRecord();
            if (row != null && !completeLayouts.contains(row.getLayout())) {
                for (HoodieSchemaField field : requiredSchema.getFields()) {
                    if (row.getLayout().fieldIndex(field.name()) < 0) {
                        return Optional.empty();
                    }
                }
                completeLayouts.add(row.getLayout());
            }
        }
        if (byPosition) {
            return PositionMatcher.of(records).map(LogRecordMatcher.class::cast);
        }
        return KeyMatcher.of(records).map(LogRecordMatcher.class::cast);
    }

    /**
     * @param basePageSource the base file read in channels that hold the output data columns, the match column at
     * {@code matchChannel} (the parquet row index for a position matcher, {@code _hoodie_record_key} for a key matcher)
     * and, under event-time ordering, the ordering fields {@code baseOrderingValues} reads
     * @param outputToBaseChannel for each output column, its base page channel, or -1 for a column of
     * {@code prefilledColumnValues}
     */
    public static HudiVectorizedMorPageSource create(
            ConnectorPageSource basePageSource,
            LogRecordMatcher matcher,
            int matchChannel,
            RecordMergeMode mergeMode,
            BufferedRecordMerger<TrinoRow> merger,
            Optional<BaseOrderingValues> baseOrderingValues,
            Option<UnaryOperator<TrinoRow>> outputConverter,
            List<HiveColumnHandle> outputColumns,
            int[] outputToBaseChannel,
            PrefilledColumnValues prefilledColumnValues)
    {
        if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING && mergeMode != RecordMergeMode.EVENT_TIME_ORDERING) {
            throw new IllegalArgumentException("The vectorized MOR read merges by commit time or event time, not " + mergeMode);
        }
        if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING && baseOrderingValues.isEmpty()) {
            throw new IllegalArgumentException("An event time merge needs the ordering values of base rows");
        }
        return new HudiVectorizedMorPageSource(
                basePageSource,
                matcher,
                matchChannel,
                mergeMode,
                merger,
                baseOrderingValues,
                outputConverter.isPresent() ? Optional.of(outputConverter.get()) : Optional.empty(),
                outputColumns,
                outputToBaseChannel,
                prefilledColumnValues);
    }

    @Override
    public long getCompletedBytes()
    {
        return basePageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return basePageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return basePageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        while (true) {
            if (!ready.isEmpty()) {
                return ready.poll();
            }
            if (!baseFinished) {
                SourcePage page = basePageSource.getNextSourcePage();
                if (page == null) {
                    if (!basePageSource.isFinished()) {
                        return null;
                    }
                    baseFinished = true;
                    leftovers = matcher.leftovers();
                }
                else {
                    mergeBasePage(page);
                }
                continue;
            }
            // Log records no base row took: base rows the base read skipped, or records without a base row
            while (leftovers.hasNext() && !pageBuilder.isFull()) {
                appendLogRecord(leftovers.next());
            }
            if (!pageBuilder.isEmpty()) {
                return flushLogRows();
            }
            if (!leftovers.hasNext()) {
                finished = true;
                return null;
            }
        }
    }

    private void mergeBasePage(SourcePage page)
    {
        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }
        Block matchBlock = page.getBlock(matchChannel);
        int[] dropped = matcher.matchPage(matchBlock, positionCount, this::mergeRow, page);
        if (dropped == null || dropped[0] == 0) {
            ready.add(new OutputPage(page, outputToBaseChannel, outputColumns, prefilledColumnValues));
        }
        else {
            int droppedCount = dropped[0];
            int kept = positionCount - droppedCount;
            if (kept > 0) {
                int[] keep = new int[kept];
                int keepCount = 0;
                int next = 1;
                for (int position = 0; position < positionCount; position++) {
                    if (next <= droppedCount && dropped[next] == position) {
                        next++;
                    }
                    else {
                        keep[keepCount++] = position;
                    }
                }
                page.selectPositions(keep, 0, kept);
                ready.add(new OutputPage(page, outputToBaseChannel, outputColumns, prefilledColumnValues));
            }
        }
        if (pageBuilder.isFull()) {
            ready.add(flushLogRows());
        }
    }

    /**
     * Merges the log record of base row {@code position} into the output and returns whether the base row stays.
     */
    private boolean mergeRow(SourcePage page, int position, BufferedRecord<TrinoRow> logRecord)
    {
        try {
            if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
                appendLogRecord(logRecord);
                return false;
            }
            BufferedRecord<TrinoRow> baseRecord = baseOrderingValues.orElseThrow().baseRecord(page, position);
            BufferedRecord<TrinoRow> result = merger.finalMerge(baseRecord, logRecord);
            if (result == baseRecord) {
                return true;
            }
            appendLogRecord(result);
            return false;
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CURSOR_ERROR, "Failed to merge a log record into a base row", e);
        }
    }

    private void appendLogRecord(BufferedRecord<TrinoRow> record)
    {
        if (record.isDelete() || record.getRecord() == null) {
            return;
        }
        TrinoRow row = record.getRecord();
        appender.append(pageBuilder, outputConverter.isPresent() ? outputConverter.get().apply(row) : row);
    }

    private SourcePage flushLogRows()
    {
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    @Override
    public long getMemoryUsage()
    {
        return basePageSource.getMemoryUsage() + pageBuilder.getRetainedSizeInBytes() + logRecordsRetainedBytes;
    }

    @Override
    public void close()
            throws IOException
    {
        basePageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return basePageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return basePageSource.getMetrics().mergeWith(metrics);
    }

    @FunctionalInterface
    interface RowMerger
    {
        boolean merge(SourcePage page, int position, BufferedRecord<TrinoRow> logRecord);
    }

    /**
     * Finds the log records of the rows of base pages.
     */
    public sealed interface LogRecordMatcher
            permits PositionMatcher, KeyMatcher
    {
        /**
         * Hands each base row of the page that has a log record to {@code merger}, and returns the rows the merger
         * drops as {@code [count, position...]} in ascending order, or null when no row of the page can have one.
         */
        int[] matchPage(Block matchBlock, int positionCount, RowMerger merger, SourcePage page);

        /**
         * The log records no base row took.
         */
        Iterator<BufferedRecord<TrinoRow>> leftovers();

        long retainedSizeInBytes();
    }

    /**
     * Positional merge: the log records sorted by the base file row they replace, walked with the row index of the base
     * pages, which ascends through the file.
     */
    static final class PositionMatcher
            implements LogRecordMatcher
    {
        private static final long MAX_PACKED_POSITION = Integer.MAX_VALUE;

        private final long[] positions;
        private final BufferedRecord<TrinoRow>[] records;
        private int cursor;

        private PositionMatcher(long[] positions, BufferedRecord<TrinoRow>[] records)
        {
            this.positions = positions;
            this.records = records;
        }

        @SuppressWarnings("unchecked")
        static Optional<PositionMatcher> of(Map<Serializable, BufferedRecord<TrinoRow>> records)
        {
            int count = records.size();
            BufferedRecord<TrinoRow>[] unsorted = new BufferedRecord[count];
            // Each position is packed above its index into the unsorted records, so one primitive sort orders both
            long[] packed = new long[count];
            int index = 0;
            for (Map.Entry<Serializable, BufferedRecord<TrinoRow>> entry : records.entrySet()) {
                if (!(entry.getKey() instanceof Long position) || position < 0 || position > MAX_PACKED_POSITION) {
                    return Optional.empty();
                }
                unsorted[index] = entry.getValue();
                packed[index] = (position << 32) | index;
                index++;
            }
            Arrays.sort(packed);
            long[] positions = new long[count];
            BufferedRecord<TrinoRow>[] sorted = new BufferedRecord[count];
            for (int i = 0; i < count; i++) {
                positions[i] = packed[i] >>> 32;
                sorted[i] = unsorted[(int) packed[i]];
            }
            return Optional.of(new PositionMatcher(positions, sorted));
        }

        @Override
        public int[] matchPage(Block rowIndexBlock, int positionCount, RowMerger merger, SourcePage page)
        {
            long first = BIGINT.getLong(rowIndexBlock, 0);
            long last = BIGINT.getLong(rowIndexBlock, positionCount - 1);
            while (cursor < positions.length && positions[cursor] < first) {
                cursor++;
            }
            if (cursor >= positions.length || positions[cursor] > last) {
                return null;
            }
            // A page of consecutive rows, as without page pruning, maps a row index to its position directly
            boolean consecutive = last - first == positionCount - 1;
            int[] dropped = null;
            int droppedCount = 0;
            int low = 0;
            for (; cursor < positions.length && positions[cursor] <= last; cursor++) {
                int position = consecutive
                        ? (int) (positions[cursor] - first)
                        : find(rowIndexBlock, low, positionCount, positions[cursor]);
                if (position < 0) {
                    // The base read skipped this row: its log record stays a leftover
                    continue;
                }
                low = position + 1;
                BufferedRecord<TrinoRow> record = records[cursor];
                records[cursor] = null;
                if (!merger.merge(page, position, record)) {
                    if (dropped == null) {
                        dropped = new int[Math.min(positionCount, positions.length - cursor) + 1];
                    }
                    dropped[++droppedCount] = position;
                }
            }
            if (dropped == null) {
                return new int[] {0};
            }
            dropped[0] = droppedCount;
            return dropped;
        }

        private static int find(Block rowIndexBlock, int low, int high, long rowIndex)
        {
            int left = low;
            int right = high - 1;
            while (left <= right) {
                int middle = (left + right) >>> 1;
                long value = BIGINT.getLong(rowIndexBlock, middle);
                if (value < rowIndex) {
                    left = middle + 1;
                }
                else if (value > rowIndex) {
                    right = middle - 1;
                }
                else {
                    return middle;
                }
            }
            return -1;
        }

        @Override
        public Iterator<BufferedRecord<TrinoRow>> leftovers()
        {
            return Arrays.stream(records).filter(record -> record != null).iterator();
        }

        @Override
        public long retainedSizeInBytes()
        {
            return retainedSize(Arrays.asList(records)) + (long) Long.BYTES * positions.length;
        }
    }

    /**
     * Key-based merge: a hash lookup of the record key of each base row.
     */
    static final class KeyMatcher
            implements LogRecordMatcher
    {
        private final Map<Slice, BufferedRecord<TrinoRow>> byKey;

        private KeyMatcher(Map<Slice, BufferedRecord<TrinoRow>> byKey)
        {
            this.byKey = byKey;
        }

        static Optional<KeyMatcher> of(Map<Serializable, BufferedRecord<TrinoRow>> records)
        {
            Map<Slice, BufferedRecord<TrinoRow>> byKey = new HashMap<>(records.size() * 2);
            for (Map.Entry<Serializable, BufferedRecord<TrinoRow>> entry : records.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    return Optional.empty();
                }
                byKey.put(Slices.utf8Slice(key), entry.getValue());
            }
            return Optional.of(new KeyMatcher(byKey));
        }

        @Override
        public int[] matchPage(Block recordKeyBlock, int positionCount, RowMerger merger, SourcePage page)
        {
            if (byKey.isEmpty()) {
                return null;
            }
            int[] dropped = null;
            int droppedCount = 0;
            for (int position = 0; position < positionCount; position++) {
                if (recordKeyBlock.isNull(position)) {
                    continue;
                }
                BufferedRecord<TrinoRow> record = byKey.remove(VARCHAR.getSlice(recordKeyBlock, position));
                if (record != null && !merger.merge(page, position, record)) {
                    if (dropped == null) {
                        dropped = new int[positionCount + 1];
                    }
                    dropped[++droppedCount] = position;
                }
            }
            if (dropped == null) {
                return new int[] {0};
            }
            dropped[0] = droppedCount;
            return dropped;
        }

        @Override
        public Iterator<BufferedRecord<TrinoRow>> leftovers()
        {
            return byKey.values().iterator();
        }

        @Override
        public long retainedSizeInBytes()
        {
            return retainedSize(byKey.values());
        }
    }

    private static long retainedSize(Iterable<BufferedRecord<TrinoRow>> records)
    {
        long size = 0;
        for (BufferedRecord<TrinoRow> record : records) {
            if (record != null) {
                size += BUFFERED_RECORD_INSTANCE_SIZE;
                if (record.getRecord() != null) {
                    size += record.getRecord().getRetainedSizeInBytes();
                }
            }
        }
        return size;
    }

    /**
     * Builds the record of a base row an event time merge compares with its log record: the row's ordering value, read
     * from the base page channels of the ordering fields.
     */
    public static final class BaseOrderingValues
    {
        private final TrinoRecordContext recordContext;
        private final HoodieSchema orderingSchema;
        private final TrinoRow.Layout layout;
        private final int[] channels;
        private final List<String> orderingFieldNames;
        private SourcePage currentPage;
        private Block[] currentBlocks;

        /**
         * @param orderingSchema a record schema of the ordering fields, in the order of {@code channels}
         * @param types the type of the base page block of each ordering field
         */
        public BaseOrderingValues(TrinoRecordContext recordContext, HoodieSchema orderingSchema, List<Type> types, int[] channels, List<String> orderingFieldNames)
        {
            this.recordContext = requireNonNull(recordContext, "recordContext is null");
            this.orderingSchema = requireNonNull(orderingSchema, "orderingSchema is null");
            this.layout = new TrinoRow.Layout(orderingSchema, types);
            this.channels = channels.clone();
            this.orderingFieldNames = List.copyOf(orderingFieldNames);
        }

        BufferedRecord<TrinoRow> baseRecord(SourcePage page, int position)
        {
            if (page != currentPage) {
                Block[] blocks = new Block[channels.length];
                for (int field = 0; field < channels.length; field++) {
                    blocks[field] = page.getBlock(channels[field]);
                }
                currentBlocks = blocks;
                currentPage = page;
            }
            TrinoRow row = new TrinoRow(layout, currentBlocks, position);
            Comparable<?> orderingValue = recordContext.getOrderingValue(row, orderingSchema, orderingFieldNames);
            return BufferedRecords.fromEngineRecord(row, orderingSchema, recordContext, orderingValue, null, false);
        }
    }

    /**
     * A base page in output channels: data columns are base page channels, loaded on first access, and the other columns
     * are constant blocks of the split's prefilled values.
     */
    private static final class OutputPage
            implements SourcePage
    {
        private static final long INSTANCE_SIZE = instanceSize(OutputPage.class);

        private final SourcePage basePage;
        private final int[] outputToBaseChannel;
        private final List<HiveColumnHandle> outputColumns;
        private final PrefilledColumnValues prefilledColumnValues;
        private final Block[] prefilledBlocks;

        private OutputPage(SourcePage basePage, int[] outputToBaseChannel, List<HiveColumnHandle> outputColumns, PrefilledColumnValues prefilledColumnValues)
        {
            this.basePage = basePage;
            this.outputToBaseChannel = outputToBaseChannel;
            this.outputColumns = outputColumns;
            this.prefilledColumnValues = prefilledColumnValues;
            this.prefilledBlocks = new Block[outputToBaseChannel.length];
        }

        @Override
        public int getPositionCount()
        {
            return basePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return basePage.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + basePage.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            consumer.accept(this, INSTANCE_SIZE);
            basePage.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return outputToBaseChannel.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            int baseChannel = outputToBaseChannel[channel];
            if (baseChannel >= 0) {
                return basePage.getBlock(baseChannel);
            }
            Block block = prefilledBlocks[channel];
            if (block == null) {
                block = prefilledColumnValues.toRleBlock(outputColumns.get(channel), basePage.getPositionCount());
                prefilledBlocks[channel] = block;
            }
            return block;
        }

        @Override
        public Page getPage()
        {
            Block[] blocks = new Block[outputToBaseChannel.length];
            for (int channel = 0; channel < blocks.length; channel++) {
                blocks[channel] = getBlock(channel);
            }
            return new Page(getPositionCount(), blocks);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            basePage.selectPositions(positions, offset, size);
            Arrays.fill(prefilledBlocks, null);
        }
    }
}
