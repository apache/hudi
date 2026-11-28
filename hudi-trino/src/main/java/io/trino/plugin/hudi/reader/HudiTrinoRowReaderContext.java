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
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext.LogFileParquetPageSourceFactory;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordSerializer;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;

/**
 * The reader context of merge reads with {@code hudi.schema-column-types.enabled} on, for tables merged by commit time,
 * by event time or by a payload class: the file-group reader merges {@link TrinoRow}s (see {@link TrinoRecordContext}),
 * so a base row no log record touches reaches the output as a view of its page position. Tables with a custom
 * {@link HoodieRecordMerger} keep {@link HudiTrinoReaderContext}: a user merger may cast records to Avro.
 */
public class HudiTrinoRowReaderContext
        extends HoodieReaderContext<TrinoRow>
{
    private static final long BUFFERED_RECORD_INSTANCE_SIZE = instanceSize(BufferedRecord.class);

    private final MergePageSources pageSources;
    private final TrinoRecordContext trinoRecordContext;

    public HudiTrinoRowReaderContext(
            StorageConfiguration storageConfiguration,
            HoodieTableConfig tableConfig,
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> columnHandles,
            LogFileParquetPageSourceFactory logPageSourceFactory,
            TypedProperties props,
            TrinoTypeMapper typeMapper)
    {
        this(storageConfiguration, tableConfig, pageSource, columnHandles, logPageSourceFactory,
                new TrinoRecordContext(tableConfig, ConfigUtils.getMergeProps(props, tableConfig), typeMapper), typeMapper);
    }

    private HudiTrinoRowReaderContext(
            StorageConfiguration storageConfiguration,
            HoodieTableConfig tableConfig,
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> columnHandles,
            LogFileParquetPageSourceFactory logPageSourceFactory,
            TrinoRecordContext trinoRecordContext,
            TrinoTypeMapper typeMapper)
    {
        super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), trinoRecordContext);
        this.trinoRecordContext = trinoRecordContext;
        this.pageSources = new MergePageSources(pageSource, columnHandles, logPageSourceFactory, Optional.of(typeMapper));
    }

    @Override
    public void setSchemaHandler(FileGroupReaderSchemaHandler<TrinoRow> schemaHandler)
    {
        super.setSchemaHandler(schemaHandler);
        List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(getMergeMode(), tableConfig);
        trinoRecordContext.initOrderingValueConverter(schemaHandler.getTableSchema(), orderingFieldNames);
    }

    @Override
    public ClosableIterator<TrinoRow> getFileRecordIterator(
            StoragePath storagePath,
            long start,
            long length,
            HoodieSchema dataSchema,
            HoodieSchema requiredSchema,
            HoodieStorage storage)
    {
        return getFileRecordIterator(storagePath, start, length, requiredSchema);
    }

    @Override
    public ClosableIterator<TrinoRow> getFileRecordIterator(
            StoragePathInfo storagePathInfo,
            long start,
            long length,
            HoodieSchema dataSchema,
            HoodieSchema requiredSchema,
            HoodieStorage storage)
    {
        return getFileRecordIterator(storagePathInfo.getPath(), start, length, requiredSchema);
    }

    /**
     * Reads the given file through {@link MergePageSources#open}, as rows of {@code requiredSchema} over the page
     * channels: no value is copied or converted.
     */
    private ClosableIterator<TrinoRow> getFileRecordIterator(StoragePath path, long start, long length, HoodieSchema requiredSchema)
    {
        MergePageSources.FilePageSource file = pageSources.open(path, start, length, requiredSchema);
        return MergePageSources.recordIterator(file.pageSource(), new PageRowReader(requiredSchema, file.projection()));
    }

    /**
     * Views page positions as rows of the required schema. The rows of one page share one block array, in schema field
     * order, built when the page is first seen.
     */
    private static final class PageRowReader
            implements MergePageSources.PositionReader<TrinoRow>
    {
        private final TrinoRow.Layout layout;
        private final int[] fieldChannels;
        private Page currentPage;
        private Block[] currentBlocks;

        private PageRowReader(HoodieSchema requiredSchema, List<HiveColumnHandle> projection)
        {
            Map<String, Integer> channels = new HashMap<>();
            for (int channel = 0; channel < projection.size(); channel++) {
                channels.putIfAbsent(projection.get(channel).getName().toLowerCase(Locale.ROOT), channel);
            }
            List<HoodieSchemaField> fields = requiredSchema.getFields();
            fieldChannels = new int[fields.size()];
            Type[] types = new Type[fields.size()];
            for (int field = 0; field < fields.size(); field++) {
                Integer channel = channels.get(fields.get(field).name().toLowerCase(Locale.ROOT));
                if (channel == null) {
                    // MergePageSources.open puts every required field in the projection
                    throw new TrinoException(HUDI_SCHEMA_ERROR, "Required field %s is not a column of the read projection".formatted(fields.get(field).name()));
                }
                fieldChannels[field] = channel;
                // The blocks hold the projection's types, which the planner took from TrinoTypeMapper
                types[field] = projection.get(channel).getType();
            }
            layout = new TrinoRow.Layout(requiredSchema, List.of(types));
        }

        @Override
        public TrinoRow read(Page page, int position)
        {
            if (page != currentPage) {
                Block[] blocks = new Block[fieldChannels.length];
                for (int field = 0; field < fieldChannels.length; field++) {
                    blocks[field] = page.getBlock(fieldChannels[field]);
                }
                currentBlocks = blocks;
                currentPage = page;
            }
            return new TrinoRow(layout, currentBlocks, position);
        }
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        // The dispatch HudiTrinoReaderContext uses. The connector routes only commit-time ordering, event-time
        // ordering and the payload strategy here, whose mergers never cast a record to Avro: OverwriteWithLatestMerger
        // ignores the records, and HoodieAvroRecordMerger converts the two records it merges through the
        // RecordContext, as PayloadUpdateProcessor does for a log record with no base row.
        return HudiTrinoReaderContext.recordMerger(mergeMode, mergeStrategyId, mergeImplClasses);
    }

    @Override
    public CustomSerializer<BufferedRecord<TrinoRow>> getRecordSerializer()
    {
        // Spilled log records go to disk as Avro binary, as HoodieAvroReaderContext spills them
        return new BufferedRecordSerializer<>(trinoRecordContext.recordSerializer());
    }

    @Override
    public SizeEstimator<BufferedRecord<TrinoRow>> getRecordSizeEstimator()
    {
        // The default estimator walks the object graph of the record, which from a row reaches Trino types and
        // through them Class objects and their class loader. A buffered row is compact (toBinaryRow), so its
        // blocks' retained size is its size.
        return record -> {
            long size = BUFFERED_RECORD_INSTANCE_SIZE + estimatedSizeOf(record.getRecordKey());
            TrinoRow row = record.getRecord();
            return row == null ? size : size + row.getRetainedSizeInBytes();
        };
    }

    @Override
    public ClosableIterator<TrinoRow> mergeBootstrapReaders(ClosableIterator<TrinoRow> skeletonFileIterator, HoodieSchema skeletonRequiredSchema, ClosableIterator<TrinoRow> dataFileIterator, HoodieSchema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues)
    {
        // As HudiTrinoReaderContext: reads of bootstrap tables go through the regular page-source path
        throw new UnsupportedOperationException("HudiTrinoRowReaderContext does not support bootstrap merge");
    }
}
