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
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestMerger;
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

import java.util.List;
import java.util.Optional;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

public class HudiTrinoReaderContext
        extends HoodieReaderContext<IndexedRecord>
{
    private final PrefilledColumnValues prefilledColumnValues;
    private final MergePageSources pageSources;
    // hudi.schema-column-types.enabled: the projection carries TrinoTypeMapper types, which only the
    // HoodieSchema-driven serializer reads
    private final boolean schemaColumnTypes;

    /**
     * Factory for building a Trino parquet page source over a native (RFC-103) delta-log file on demand.
     * Kept as an injected interface so this reader context stays free of Trino-parquet/session types.
     */
    @FunctionalInterface
    public interface LogFileParquetPageSourceFactory
    {
        ConnectorPageSource create(String path, long start, long length, List<HiveColumnHandle> projection);
    }

    public HudiTrinoReaderContext(
            StorageConfiguration storageConfiguration,
            HoodieTableConfig tableConfig,
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> columnHandles,
            PrefilledColumnValues prefilledColumnValues,
            LogFileParquetPageSourceFactory logPageSourceFactory,
            TypedProperties props)
    {
        this(storageConfiguration, tableConfig, pageSource, columnHandles, prefilledColumnValues, logPageSourceFactory, props, Optional.empty());
    }

    public HudiTrinoReaderContext(
            StorageConfiguration storageConfiguration,
            HoodieTableConfig tableConfig,
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> columnHandles,
            PrefilledColumnValues prefilledColumnValues,
            LogFileParquetPageSourceFactory logPageSourceFactory,
            TypedProperties props,
            Optional<TrinoTypeMapper> schemaTypeMapper)
    {
        super(storageConfiguration, tableConfig, Option.empty(), Option.empty(),
                new AvroRecordContext(tableConfig, tableConfig.getPayloadClass(), ConfigUtils.getMergeProps(props, tableConfig)));
        this.prefilledColumnValues = prefilledColumnValues;
        this.pageSources = new MergePageSources(pageSource, columnHandles, logPageSourceFactory, schemaTypeMapper);
        this.schemaColumnTypes = schemaTypeMapper.isPresent();
    }

    @Override
    public void setSchemaHandler(FileGroupReaderSchemaHandler<IndexedRecord> schemaHandler)
    {
        super.setSchemaHandler(schemaHandler);
        List<String> orderingFieldNames = HoodieRecordUtils.getOrderingFieldNames(getMergeMode(), tableConfig);
        ((AvroRecordContext) recordContext).initOrderingValueConverter(schemaHandler.getTableSchema(), orderingFieldNames);
    }

    @Override
    public ClosableIterator<IndexedRecord> getFileRecordIterator(
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
    public ClosableIterator<IndexedRecord> getFileRecordIterator(
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
     * Reads the given file through {@link MergePageSources#open}, as records that CARRY {@code requiredSchema}.
     */
    private ClosableIterator<IndexedRecord> getFileRecordIterator(
            StoragePath path,
            long start,
            long length,
            HoodieSchema requiredSchema)
    {
        MergePageSources.FilePageSource file = pageSources.open(path, start, length, requiredSchema);
        return MergePageSources.recordIterator(file.pageSource(), recordSerializer(file.projection(), requiredSchema));
    }

    private MergePageSources.PositionReader<IndexedRecord> recordSerializer(List<HiveColumnHandle> projection, HoodieSchema requiredSchema)
    {
        if (schemaColumnTypes) {
            return new HoodieSchemaAvroSerializer(projection, prefilledColumnValues, requiredSchema.toAvroSchema())::serialize;
        }
        return new HudiAvroSerializer(projection, prefilledColumnValues, requiredSchema.toAvroSchema())::serialize;
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        // Dispatch on the table's merge mode, mirroring HoodieAvroReaderContext. The Trino reader
        // operates on IndexedRecord, so the Avro mergers apply directly. The CUSTOM arm's return
        // value drives merging (combineAndGetUpdateValue at read time, covered end-to-end by
        // TestHudiMorPayloadSemantics; apache/hudi#18898). The ordering arms' mergers are reached
        // through partialMerge when a log block carries IS_PARTIAL (BufferedRecordMergerFactory),
        // which these suites do not cover; TestHudiMorMergeModeSemantics pins the mode semantics
        // themselves. TODO(apache/hudi#19413): cover the ordering arms' partialMerge path
        // (IS_PARTIAL log blocks) once the Avro mergers implement it.
        return recordMerger(mergeMode, mergeStrategyId, mergeImplClasses);
    }

    /**
     * The merger dispatch both reader contexts share. Every merger it returns for the ordering modes, and
     * HoodieAvroRecordMerger for payload classes, is generic over the record type: it reads records through the
     * RecordContext and converts only the two records it merges to Avro.
     */
    static Option<HoodieRecordMerger> recordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        switch (mergeMode) {
            case EVENT_TIME_ORDERING:
                return Option.of(new HoodieAvroRecordMerger());
            case COMMIT_TIME_ORDERING:
                return Option.of(new OverwriteWithLatestMerger());
            case CUSTOM:
            default:
                // createValidRecordMerger dereferences the strategy id on its first line, so a table that
                // resolved to CUSTOM without persisting one must be rejected before it reaches hudi-common.
                HudiUtil.validateCustomMergeStrategyId(mergeStrategyId);
                Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.JAVA, mergeImplClasses, mergeStrategyId);
                if (recordMerger.isEmpty()) {
                    throw new IllegalArgumentException("No valid merger implementation set for `" + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
                }
                return recordMerger;
        }
    }

    /**
     * Sizes merge buffer records from their values. The inherited estimator walks the record graph on every 100th
     * put and counts the schema shared by all records each time (13 KB per TPC-DS store_sales row), so the merge
     * buffer spilled after about 64k records.
     */
    @Override
    public SizeEstimator<BufferedRecord<IndexedRecord>> getRecordSizeEstimator()
    {
        return new HudiRecordSizeEstimator();
    }

    /**
     * Spills merge buffer records without their schema and keeps the Trino value kinds of native log rows, which the
     * inherited Kryo serializer cannot write: it encodes the record as Avro binary, prefixed with its schema JSON.
     */
    @Override
    public CustomSerializer<BufferedRecord<IndexedRecord>> getRecordSerializer()
    {
        return new BufferedRecordSerializer<>(new HudiRecordSerializer(schemaId -> getRecordContext().decodeAvroSchema(schemaId).toAvroSchema()));
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator, HoodieSchema skeletonRequiredSchema, ClosableIterator<IndexedRecord> dataFileIterator, HoodieSchema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues)
    {
        // Bootstrap merge is not exercised by the Trino connector; reads of bootstrap tables go
        // through the regular page-source path. Throwing surfaces accidental use loudly.
        throw new UnsupportedOperationException("HudiTrinoReaderContext does not support bootstrap merge");
    }
}
