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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
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
import java.util.NoSuchElementException;
import java.util.Set;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static java.lang.String.format;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

public class HudiTrinoReaderContext
        extends HoodieReaderContext<IndexedRecord>
{
    private final ConnectorPageSource pageSource;
    private final HudiAvroSerializer avroSerializer;
    private final PrefilledColumnValues prefilledColumnValues;
    private final LogFileParquetPageSourceFactory logPageSourceFactory;
    private final Map<String, HiveColumnHandle> colNameToHandle;
    private final Set<String> baseProjectionNames;

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
            LogFileParquetPageSourceFactory logPageSourceFactory)
    {
        super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), new AvroRecordContext(tableConfig, tableConfig.getPayloadClass()));
        this.pageSource = pageSource;
        this.prefilledColumnValues = prefilledColumnValues;
        this.avroSerializer = new HudiAvroSerializer(columnHandles, prefilledColumnValues);
        this.logPageSourceFactory = logPageSourceFactory;
        this.colNameToHandle = new HashMap<>();
        for (HiveColumnHandle handle : columnHandles) {
            colNameToHandle.put(handle.getBaseColumnName().toLowerCase(Locale.ROOT), handle);
        }
        // Immutable snapshot of the read projection for the base-read guard in getFileRecordIterator;
        // colNameToHandle cannot serve that purpose because buildRequiredColumnHandles adds log-side
        // handles to it on demand.
        this.baseProjectionNames = ImmutableSet.copyOf(colNameToHandle.keySet());
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
            HudiAvroSerializer logSerializer = new HudiAvroSerializer(logProjection, prefilledColumnValues);
            return createRecordIterator(logSource, logSerializer);
        }
        // The base read reuses the pre-built page source, so it can only satisfy requiredSchema fields the
        // read projection carries. The connector predicts the file-group reader's demands up front
        // (HudiUtil.getMergeRequiredColumnHandles, HudiPageSourceProvider.requiresFullSchemaRead); if the
        // two ever drift, fail loudly here instead of silently merging with null column values.
        List<String> missingColumns = requiredSchema.getFields().stream()
                .map(HoodieSchemaField::name)
                .filter(name -> !baseProjectionNames.contains(name.toLowerCase(Locale.ROOT)))
                .toList();
        if (!missingColumns.isEmpty()) {
            throw new TrinoException(HUDI_SCHEMA_ERROR, format(
                    "The file-group reader requires columns %s for merging, but the base-file read projection "
                            + "does not carry them. The connector's merge projection is out of sync with "
                            + "FileGroupReaderSchemaHandler.generateRequiredSchema.",
                    missingColumns));
        }
        return createRecordIterator(pageSource, avroSerializer);
    }

    /**
     * Resolves the {@link HiveColumnHandle} for each field of {@code requiredSchema} so the on-demand log
     * page source reads exactly the columns the file-group reader needs to merge. Fields the connector
     * projection carries resolve to their projection handles (planner-authoritative types); the file-group
     * reader can also require fields the projection does not carry (e.g. {@code _hoodie_commit_time} or a
     * delete-marker column on a narrow query) -- every such field originates from the table schema and
     * carries its real Avro type, so its handle is typed directly from the field and cached.
     */
    private List<HiveColumnHandle> buildRequiredColumnHandles(HoodieSchema requiredSchema)
    {
        List<HiveColumnHandle> handles = new ArrayList<>();
        for (HoodieSchemaField field : requiredSchema.getFields()) {
            handles.add(colNameToHandle.computeIfAbsent(
                    field.name().toLowerCase(Locale.ROOT),
                    _ -> HudiUtil.toColumnHandle(field)));
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
                    throw new NoSuchElementException("No more records in the iterator");
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

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator, HoodieSchema skeletonRequiredSchema, ClosableIterator<IndexedRecord> dataFileIterator, HoodieSchema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues)
    {
        // Bootstrap merge is not exercised by the Trino connector; reads of bootstrap tables go
        // through the regular page-source path. Throwing surfaces accidental use loudly.
        throw new UnsupportedOperationException("HudiTrinoReaderContext does not support bootstrap merge");
    }
}
