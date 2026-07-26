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

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.VarcharType;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.inline.InLineFSUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

public class HudiTrinoReaderContext
        extends HoodieReaderContext<IndexedRecord>
{
    ConnectorPageSource pageSource;
    private final HudiAvroSerializer avroSerializer;
    private final SynthesizedColumnHandler synthesizedColumnHandler;
    private final LogFileParquetPageSourceFactory logPageSourceFactory;
    Map<String, Integer> colToPosMap;
    Map<String, HiveColumnHandle> colNameToHandle;
    List<HiveColumnHandle> dataHandles;
    List<HiveColumnHandle> columnHandles;

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
            List<HiveColumnHandle> dataHandles,
            List<HiveColumnHandle> columnHandles,
            SynthesizedColumnHandler synthesizedColumnHandler,
            LogFileParquetPageSourceFactory logPageSourceFactory)
    {
        super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), new AvroRecordContext(tableConfig, tableConfig.getPayloadClass()));
        this.pageSource = pageSource;
        this.synthesizedColumnHandler = synthesizedColumnHandler;
        this.avroSerializer = new HudiAvroSerializer(columnHandles, synthesizedColumnHandler);
        this.dataHandles = dataHandles;
        this.columnHandles = columnHandles;
        this.logPageSourceFactory = logPageSourceFactory;
        this.colToPosMap = new HashMap<>();
        this.colNameToHandle = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            HiveColumnHandle handle = columnHandles.get(i);
            colToPosMap.put(handle.getBaseColumnName(), i);
            colNameToHandle.put(handle.getBaseColumnName().toLowerCase(Locale.ROOT), handle);
        }
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
     * Reads the given file and projects {@code requiredSchema}. For a native (RFC-103) delta-log parquet file a
     * fresh page source is built on demand with predicate pushdown disabled so every log record is read and
     * merged; for the base file the pre-built base page source is reused. Classic Avro log blocks never reach
     * here (they deserialize inline).
     */
    private ClosableIterator<IndexedRecord> getFileRecordIterator(
            StoragePath path,
            long start,
            long length,
            HoodieSchema requiredSchema)
    {
        if (FSUtils.isLogFile(path)) {
            // Inline parquet log blocks (inlinefs:// scheme) need a separate inline-aware reader; out of scope here.
            if (InLineFSUtils.SCHEME.equals(path.toUri().getScheme())) {
                throw new UnsupportedOperationException("Inline log blocks are not supported by the Hudi Trino connector: " + path);
            }
            List<HiveColumnHandle> logProjection = buildRequiredColumnHandles(requiredSchema);
            ConnectorPageSource logSource = logPageSourceFactory.create(path.toString(), start, length, logProjection);
            HudiAvroSerializer logSerializer = new HudiAvroSerializer(logProjection, synthesizedColumnHandler);
            return createRecordIterator(logSource, logSerializer);
        }
        return createRecordIterator(pageSource, avroSerializer);
    }

    /**
     * Resolves the {@link HiveColumnHandle} for each field of {@code requiredSchema} against the reader's column
     * handles (keyed by lowercased base column name) so the on-demand log page source reads exactly the columns
     * the file-group reader needs to merge. The file-group reader can add meta fields to {@code requiredSchema}
     * that the connector projection does not carry -- the projection holds only the query columns plus
     * {@code HUDI_REQUIRED_META_COLUMNS} (record key + partition path), whereas the merge may also need e.g.
     * {@code _hoodie_commit_time}; for such a Hudi meta column a handle is synthesized (see the inline note)
     * rather than failing the read.
     */
    private List<HiveColumnHandle> buildRequiredColumnHandles(HoodieSchema requiredSchema)
    {
        List<HiveColumnHandle> handles = new ArrayList<>();
        for (HoodieSchemaField field : requiredSchema.getFields()) {
            String name = field.name();
            HiveColumnHandle handle = colNameToHandle.get(name.toLowerCase(Locale.ROOT));
            if (handle == null) {
                if (!HoodieRecord.HOODIE_META_COLUMNS.contains(name)) {
                    // A data column outside the projection cannot be typed at this layer. The file-group reader
                    // only asks for one when the table's custom merger is not projection compatible (it then
                    // reads the FULL table schema), which this connector does not support.
                    throw new TrinoException(NOT_SUPPORTED, format(
                            "Column '%s' is required for merging but is not in the connector's read projection. "
                                    + "This usually means the table's custom record merger is not projection compatible; "
                                    + "the Hudi Trino connector requires custom mergers to override isProjectionCompatible() "
                                    + "to true and declare getMandatoryFieldsForMerging().", name));
                }
                // Synthesize a handle for a Hudi meta column absent from the connector projection. This is safe:
                //   1. Every Hudi meta column is a UTF8 string on disk, so HIVE_STRING/VARCHAR is the correct
                //      physical type -- the same handle HudiUtil.prependHudiMetaAndMergeRequiredColumns builds for the
                //      HUDI_REQUIRED_META_COLUMNS.
                //   2. hiveColumnIndex (0) is a throwaway placeholder: HudiPageSourceProvider.createPageSource
                //      resolves parquet columns by NAME (directly when useColumnNames=true, otherwise
                //      remapColumnIndicesToPhysical rebuilds every index from the file schema by name), so this
                //      ordinal is never read.
                // TODO(apache/hudi#19249): remove this synthesis. Build colNameToHandle from the full data schema
                //  (all meta + data columns, typed once from the table schema) so every requiredSchema field
                //  resolves by lookup, dropping both the hand-built handle and the HOODIE_META_COLUMNS guard
                //  above -- which would also lift the projection-compatible-merger restriction.
                handle = new HiveColumnHandle(name, 0, HiveType.HIVE_STRING, VarcharType.VARCHAR,
                        Optional.empty(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
            }
            handles.add(handle);
        }
        return handles;
    }

    private ClosableIterator<IndexedRecord> createRecordIterator(ConnectorPageSource source, HudiAvroSerializer serializer)
    {
        return new ClosableIterator<>()
        {
            private Page currentPage;
            private int currentPosition;

            @Override
            public void close()
            {
                try {
                    source.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext()
            {
                // If all records in the current page are consume, try to get next page
                if (currentPage == null || currentPosition >= currentPage.getPositionCount()) {
                    if (source.isFinished()) {
                        return false;
                    }

                    // Get next page and reset currentPosition. Unwrap the SourcePage to the
                    // underlying Page so the serializer's Block accessors keep working.
                    SourcePage nextSourcePage = source.getNextSourcePage();
                    currentPage = nextSourcePage == null ? null : nextSourcePage.getPage();
                    currentPosition = 0;

                    // If no more pages are available
                    return currentPage != null;
                }

                return true;
            }

            @Override
            public IndexedRecord next()
            {
                if (!hasNext()) {
                    // TODO: This can probably be removed or ignored, added this as a sanity check
                    throw new RuntimeException("No more records in the iterator");
                }

                IndexedRecord record = serializer.serialize(currentPage, currentPosition);
                currentPosition++;
                return record;
            }
        };
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        // Dispatch on the table's merge mode, mirroring HoodieAvroReaderContext. The Trino reader
        // operates on IndexedRecord, so the Avro mergers apply directly. Using the read-time merger
        // (combineAndGetUpdateValue) rather than a fixed preCombine merger keeps COMMIT_TIME_ORDERING
        // and custom-payload tables correct on MoR reads.
        // TODO(apache/hudi#18898): add MoR read tests for delete markers and custom payloads to
        //  exercise the EVENT_TIME_ORDERING (combineAndGetUpdateValue) and CUSTOM branches below.
        switch (mergeMode) {
            case EVENT_TIME_ORDERING:
                return Option.of(new HoodieAvroRecordMerger());
            case COMMIT_TIME_ORDERING:
                return Option.of(new OverwriteWithLatestMerger());
            case CUSTOM:
            default:
                Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.JAVA, mergeImplClasses, mergeStrategyId);
                if (recordMerger.isEmpty()) {
                    throw new IllegalArgumentException("No valid merger implementation set for `" + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
                }
                return recordMerger;
        }
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator, HoodieSchema skeletonRequiredSchema, ClosableIterator<IndexedRecord> dataFileIterator, HoodieSchema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues)
    {
        // Bootstrap merge is not exercised by the Trino connector; reads of bootstrap tables go
        // through the regular page-source path. Throwing surfaces accidental use loudly.
        throw new UnsupportedOperationException("HudiTrinoReaderContext does not support bootstrap merge");
    }
}
