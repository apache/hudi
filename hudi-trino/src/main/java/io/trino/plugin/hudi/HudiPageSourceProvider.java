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
package io.trino.plugin.hudi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext;
import io.trino.plugin.hudi.reader.HudiTrinoRowReaderContext;
import io.trino.plugin.hudi.reader.HudiVectorizedMorPageSource;
import io.trino.plugin.hudi.reader.HudiVectorizedMorPageSource.LogRecordMatcher;
import io.trino.plugin.hudi.reader.TrinoRecordContext;
import io.trino.plugin.hudi.reader.TrinoRow;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.plugin.hudi.util.ParquetReadConversions;
import io.trino.plugin.hudi.util.ParquetReadConversions.ReadConversion;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.PrefetchedLogFileStorage;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.PARQUET_ROW_INDEX_COLUMN;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.ParquetReaderProvider;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createParquetPageSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getColumnType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiSessionProperties.getMorVectorizedReadLogPrefetchMaxSize;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetMaxReadBlockRowCount;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetMaxReadBlockSize;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetSmallFileThreshold;
import static io.trino.plugin.hudi.HudiSessionProperties.getRecordMergerImpls;
import static io.trino.plugin.hudi.HudiSessionProperties.isMorVectorizedReadAsyncLoadEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isMorVectorizedReadEarlyBaseOpenEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isMorVectorizedReadEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isMorVectorizedReadLogPrefetchEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isMorVectorizedReadPredicatePushdownEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetIgnoreStatistics;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetUseColumnIndex;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetVectorizedDecodingEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isSchemaColumnTypesEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.shouldUseParquetColumnNames;
import static io.trino.plugin.hudi.HudiSessionProperties.useParquetBloomFilter;
import static io.trino.plugin.hudi.HudiUtil.appendMissingMergeRequiredColumns;
import static io.trino.plugin.hudi.HudiUtil.appendMissingSchemaColumns;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.plugin.hudi.HudiUtil.convertToFileSlice;
import static io.trino.plugin.hudi.HudiUtil.prependHudiMetaAndMergeRequiredColumns;
import static io.trino.plugin.hudi.HudiUtil.resolveMergeModeAndStrategyId;
import static io.trino.plugin.hudi.HudiUtil.usesNonProjectionCompatibleMerger;
import static io.trino.plugin.hudi.storage.HudiTrinoStorage.convertToLocation;
import static io.trino.plugin.hudi.util.ParquetStatisticsDomains.dropIncomparableDomains;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.SKIP_DELETE_BLOCK_CONTENT;

public class HudiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(HudiPageSourceProvider.class);
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;
    private static final Set<String> VECTORIZED_MERGE_PAYLOAD_CLASSES = Set.of(
            OverwriteWithLatestAvroPayload.class.getName(),
            DefaultHoodieRecordPayload.class.getName());

    private final TrinoFileSystemFactory fileSystemFactory;
    // Loads log records and opens base files off the split's thread; rejects work when every thread is busy
    private final ExecutorService splitOpenExecutor;
    private final FileFormatDataSourceStats dataSourceStats;
    private final ParquetReaderOptions options;
    private final TrinoTypeMapper typeMapper;
    private final DateTimeZone timeZone = DateTimeZone.forID("UTC");

    @Inject
    public HudiPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager,
            @ForHudiSplitOpen ExecutorService splitOpenExecutor)
    {
        this.splitOpenExecutor = requireNonNull(splitOpenExecutor, "splitOpenExecutor is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataSourceStats = requireNonNull(dataSourceStats, "dataSourceStats is null");
        this.options = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.typeMapper = new TrinoTypeMapper(requireNonNull(typeManager, "typeManager is null").getTypeOperators());
    }

    @Override
    @SuppressWarnings("deprecation")
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) connectorTable;
        HudiSplit hudiSplit = (HudiSplit) connectorSplit;
        Optional<HudiBaseFile> hudiBaseFileOpt = hudiSplit.getBaseFile();

        String dataFilePath = hudiBaseFileOpt.isPresent()
                ? hudiBaseFileOpt.get().getPath()
                : hudiSplit.getLogFiles().getFirst().getPath();
        // Filter out metadata table splits
        // TODO: Move this check into a higher calling stack, such that the split is not created at all
        if (dataFilePath.contains(new StoragePath(
                ((HudiTableHandle) connectorTable).getBasePath()).toUri().getPath() + "/.hoodie/metadata")) {
            return new EmptyPageSource();
        }

        long start = 0;
        long length = 10;
        if (hudiBaseFileOpt.isPresent()) {
            start = hudiBaseFileOpt.get().getStart();
            length = hudiBaseFileOpt.get().getLength();
        }

        // Enable predicate pushdown for splits containing only base files
        boolean isBaseFileOnly = hudiSplit.getLogFiles().isEmpty();
        // Convert columns to HiveColumnHandles
        List<HiveColumnHandle> hiveColumnHandles = getHiveColumns(columns);

        // Get non-synthesized columns (columns that are available in data file)
        List<HiveColumnHandle> dataColumnHandles = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                .collect(Collectors.toList());
        // The `columns` list could be empty when count(*) is issued,
        // prepending hoodie meta columns for Hudi split with log files
        // to allow a non-empty dataPageSource to be returned
        List<HiveColumnHandle> hudiMetaAndDataColumnHandles = prependHudiMetaAndMergeRequiredColumns(hudiTableHandle, dataColumnHandles);

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        ParquetReaderOptions sessionOptions = ParquetReaderOptions.builder(options)
                .withIgnoreStatistics(isParquetIgnoreStatistics(session))
                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session))
                .withMaxReadBlockRowCount(getParquetMaxReadBlockRowCount(session))
                .withSmallFileThreshold(getParquetSmallFileThreshold(session))
                .withUseColumnIndex(isParquetUseColumnIndex(session))
                .withBloomFilter(useParquetBloomFilter(session))
                .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session))
                .build();
        PrefilledColumnValues prefilledColumnValues = PrefilledColumnValues.create(hudiSplit);

        // Avoid avro serialization if split/filegroup only contains base files
        if (isBaseFileOnly) {
            return new HudiBaseFileOnlyPageSource(
                    createBaseFilePageSource(session, dataColumnHandles, hudiSplit, fileSystem, sessionOptions, start, length, dynamicFilter, true),
                    hiveColumnHandles,
                    dataColumnHandles,
                    prefilledColumnValues);
        }

        // The merge path below is built around a base-file page source; fail log-only file slices with a
        // clear error instead of an opaque NoSuchElementException from getBaseFile().orElseThrow().
        // TODO: support log-only file slices by feeding the file-group reader an empty base page source.
        if (hudiBaseFileOpt.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Hudi splits with log files but no base file are not supported: "
                    + hudiSplit.getLogFiles().getFirst().getPath());
        }

        TableState tableState = hudiTableHandle.getTableState();
        HoodieTableConfig tableConfig = tableState.getTableConfig();
        HoodieSchema dataSchema = requireNonNull(hudiTableHandle.getTableSchema(), "table schema is not set on the table handle");
        TypedProperties readerProps = buildReaderProperties(session, tableConfig);
        // Handles synthesized from schema fields must be typed the way the planner typed the projection
        Optional<TrinoTypeMapper> schemaTypeMapper = isSchemaColumnTypesEnabled(session) ? Optional.of(typeMapper) : Optional.empty();

        // A non-projection-compatible CUSTOM merger makes the file-group reader demand the FULL table
        // schema as requiredSchema for this split (it has log files), for the base and log reads alike.
        // Expand the read projection to the full schema up front so the base page source carries every
        // merge column; the log page sources resolve their columns from the same expanded handles.
        List<HiveColumnHandle> readColumnHandles;
        if (requiresFullSchemaRead(tableConfig, readerProps)) {
            log.debug("Expanding the read projection of %s to the full table schema: the resolved record merger is not projection compatible",
                    hudiTableHandle.getSchemaTableName());
            readColumnHandles = appendMissingSchemaColumns(dataSchema, hudiMetaAndDataColumnHandles, schemaTypeMapper);
        }
        else {
            // The metastore may lack merge-required columns the table schema carries (e.g. hive sync with
            // omit_metadata_fields=true drops _hoodie_operation); recover them from the already-resolved
            // schema so the base read is not starved of them.
            readColumnHandles = appendMissingMergeRequiredColumns(dataSchema, hudiMetaAndDataColumnHandles, tableConfig, readerProps, schemaTypeMapper);
        }

        // Build native (RFC-103) delta-log parquet page sources on demand, projected on the file-group
        // reader's requiredSchema with predicate pushdown OFF so every log record is read and merged.
        HudiTrinoReaderContext.LogFileParquetPageSourceFactory logPageSourceFactory =
                (logPath, logStart, logLength, projection) -> createPageSource(
                        session,
                        projection,
                        hudiSplit,
                        fileSystem.newInputFile(Location.of(logPath)),
                        logPath,
                        logStart,
                        logLength,
                        OptionalLong.empty(),
                        dataSourceStats,
                        sessionOptions,
                        timeZone,
                        DynamicFilter.EMPTY,
                        false);
        HudiTrinoStorage storage = new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration());
        Schema requestedSchema = constructSchema(dataSchema.toAvroSchema(), readColumnHandles.stream().map(HiveColumnHandle::getName).toList());
        FileSlice fileSlice = convertToFileSlice(hudiSplit, hudiTableHandle.getBasePath());
        // The merge mode is resolved only with schema column types on, so the path with them off is unchanged
        if (schemaTypeMapper.isPresent() && usesTrinoRows(tableConfig)) {
            List<HiveColumnHandle> readColumns = readColumnHandles;
            long splitStart = start;
            long splitLength = length;
            Function<BooleanSupplier, ConnectorPageSource> open = closed -> {
                if (isMorVectorizedReadEnabled(session)) {
                    Optional<ConnectorPageSource> vectorized = createVectorizedMorPageSource(
                            session, hudiTableHandle, hudiSplit, tableConfig, readerProps, dataSchema, requestedSchema, fileSlice, hiveColumnHandles,
                            dataColumnHandles, readColumns, logPageSourceFactory, storage, fileSystem, sessionOptions, dynamicFilter, prefilledColumnValues, closed);
                    if (vectorized.isPresent()) {
                        return vectorized.get();
                    }
                }
                if (closed.getAsBoolean()) {
                    return new EmptyPageSource();
                }
                ConnectorPageSource dataPageSource =
                        createBaseFilePageSource(session, readColumns, hudiSplit, fileSystem, sessionOptions, splitStart, splitLength, dynamicFilter, false);
                try {
                    HudiTrinoRowReaderContext readerContext = new HudiTrinoRowReaderContext(
                            storage.getConf(),
                            tableConfig,
                            dataPageSource,
                            readColumns,
                            logPageSourceFactory,
                            readerProps,
                            typeMapper);
                    HoodieFileGroupReader<TrinoRow> fileGroupReader = buildFileGroupReader(
                            readerContext, tableState, storage, fileSlice, dataSchema, requestedSchema, hudiTableHandle.getLatestCommitTime(), readerProps, splitStart, splitLength);
                    return HudiPageSource.forTrinoRows(dataPageSource, fileGroupReader, hiveColumnHandles, prefilledColumnValues);
                }
                catch (RuntimeException e) {
                    closeAfterFailure(dataPageSource, e);
                    throw e;
                }
            };
            if (isMorVectorizedReadEnabled(session) && isMorVectorizedReadAsyncLoadEnabled(session)) {
                // When every split-open thread is busy, the split's own thread opens it, as without the async load
                return new HudiAsyncPageSource(open, command -> {
                    try {
                        splitOpenExecutor.execute(command);
                    }
                    catch (RejectedExecutionException e) {
                        command.run();
                    }
                });
            }
            return open.apply(() -> false);
        }
        ConnectorPageSource dataPageSource =
                createBaseFilePageSource(session, readColumnHandles, hudiSplit, fileSystem, sessionOptions, start, length, dynamicFilter, false);
        HudiTrinoReaderContext readerContext = new HudiTrinoReaderContext(
                storage.getConf(),
                tableConfig,
                dataPageSource,
                readColumnHandles,
                prefilledColumnValues,
                logPageSourceFactory,
                readerProps,
                schemaTypeMapper);
        HoodieFileGroupReader<IndexedRecord> fileGroupReader = buildFileGroupReader(
                readerContext, tableState, storage, fileSlice, dataSchema, requestedSchema, hudiTableHandle.getLatestCommitTime(), readerProps, start, length);
        return HudiPageSource.forAvroRecords(
                dataPageSource,
                fileGroupReader,
                hiveColumnHandles,
                prefilledColumnValues,
                schemaTypeMapper.isPresent());
    }

    /**
     * Whether the merge read merges {@link TrinoRow}s ({@link HudiTrinoRowReaderContext}) rather than Avro records
     * ({@link HudiTrinoReaderContext}): with schema column types on, for the merge modes whose mergers read records only
     * through the record context. Commit-time and event-time ordering compare ordering values; the payload strategy's
     * HoodieAvroRecordMerger converts the two records it merges to Avro. Any other CUSTOM strategy resolves a user
     * HoodieRecordMerger, which may cast a record to Avro.
     *
     * @param mergeMode the merge mode the file-group reader resolves ({@link HudiUtil#resolveMergeModeAndStrategyId})
     */
    private static boolean usesTrinoRows(HoodieTableConfig tableConfig)
    {
        Pair<RecordMergeMode, String> mergeModeAndStrategyId = resolveMergeModeAndStrategyId(tableConfig);
        return usesTrinoRows(true, mergeModeAndStrategyId.getLeft(), mergeModeAndStrategyId.getRight());
    }

    @VisibleForTesting
    static boolean usesTrinoRows(boolean schemaColumnTypes, RecordMergeMode mergeMode, String mergeStrategyId)
    {
        if (!schemaColumnTypes) {
            return false;
        }
        return switch (mergeMode) {
            case COMMIT_TIME_ORDERING, EVENT_TIME_ORDERING -> true;
            case CUSTOM -> PAYLOAD_BASED_MERGE_STRATEGY_UUID.equals(mergeStrategyId);
        };
    }

    /**
     * Opens a split through the vectorized MOR read ({@link HudiVectorizedMorPageSource}) when the table and the split's log
     * records allow it; empty leaves the split to the merge read. The log records are loaded first, so the base file
     * read knows whether they match base rows by position or by record key: by position it reads the parquet row index
     * and never the record key.
     */
    private Optional<ConnectorPageSource> createVectorizedMorPageSource(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HudiSplit hudiSplit,
            HoodieTableConfig tableConfig,
            TypedProperties readerProps,
            HoodieSchema dataSchema,
            Schema requestedSchema,
            FileSlice fileSlice,
            List<HiveColumnHandle> outputColumns,
            List<HiveColumnHandle> dataColumns,
            List<HiveColumnHandle> readColumns,
            HudiTrinoReaderContext.LogFileParquetPageSourceFactory logPageSourceFactory,
            HudiTrinoStorage storage,
            TrinoFileSystem fileSystem,
            ParquetReaderOptions sessionOptions,
            DynamicFilter dynamicFilter,
            PrefilledColumnValues prefilledColumnValues,
            BooleanSupplier closed)
    {
        RecordMergeMode mergeMode = resolveMergeModeAndStrategyId(tableConfig).getLeft();
        if (!isVectorizedMorReadSupported(tableConfig, readerProps, mergeMode)) {
            return Optional.empty();
        }
        Map<String, HiveColumnHandle> readColumnsByName = new HashMap<>();
        for (HiveColumnHandle column : readColumns) {
            readColumnsByName.putIfAbsent(column.getName().toLowerCase(Locale.ROOT), column);
        }
        List<String> orderingFieldNames = mergeMode == RecordMergeMode.EVENT_TIME_ORDERING
                ? HoodieRecordUtils.getOrderingFieldNames(mergeMode, tableConfig)
                : List.of();
        if (!orderingFieldNames.stream().allMatch(name -> readColumnsByName.containsKey(name.toLowerCase(Locale.ROOT)))) {
            return Optional.empty();
        }

        HudiBaseFile baseFile = hudiSplit.getBaseFile().orElseThrow();
        // Log blocks name the base file their positions index by its instant time, which only the file name carries:
        // the split's commit time is the query's
        HoodieBaseFile positionedBaseFile = new HoodieBaseFile(baseFile.getPath());
        // Under commit time ordering the base read is known before the log records load, provided they match base rows
        // by position: it is opened while they load, and dropped if they match by key
        Optional<CompletableFuture<ConnectorPageSource>> earlyBase = Optional.empty();
        if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING && isMorVectorizedReadEarlyBaseOpenEnabled(session)) {
            List<HiveColumnHandle> positionalColumns = new ArrayList<>(dataColumns);
            addBaseColumn(positionalColumns, PARQUET_ROW_INDEX_COLUMN);
            boolean pushdown = isMorVectorizedReadPredicatePushdownEnabled(session);
            try {
                earlyBase = Optional.of(CompletableFuture.supplyAsync(
                        () -> createBaseFilePageSource(session, positionalColumns, hudiSplit, fileSystem, sessionOptions, baseFile.getStart(), baseFile.getLength(), dynamicFilter, pushdown),
                        splitOpenExecutor));
            }
            catch (RejectedExecutionException e) {
                // Every split-open thread is busy: the base file is opened after the log records load
            }
        }
        boolean earlyBaseUsed = false;
        try {
            TypedProperties logProps = TypedProperties.copy(readerProps);
            if (mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
                // The merge drops a deleted base row without looking at its key
                logProps.setProperty(SKIP_DELETE_BLOCK_CONTENT, "true");
            }
            HudiTrinoRowReaderContext readerContext = new HudiTrinoRowReaderContext(
                    storage.getConf(),
                    tableConfig,
                    new EmptyPageSource(),
                    readColumns,
                    logPageSourceFactory,
                    logProps,
                    typeMapper);
            HoodieStorage logStorage = isMorVectorizedReadLogPrefetchEnabled(session)
                    ? PrefetchedLogFileStorage.prefetch(storage, prefetchedLogFiles(hudiSplit), getMorVectorizedReadLogPrefetchMaxSize(session).toBytes(), (path, buffer) -> {
                        // One ranged read of the whole file, as the parquet reader reads
                        try (TrinoInput input = fileSystem.newInputFile(convertToLocation(path), buffer.length).newInput()) {
                            input.readFully(0, buffer, 0, buffer.length);
                        }
                    })
                    : storage;
            LogRecordMatcher matcher;
            boolean byPosition;
            BufferedRecordMerger<TrinoRow> merger;
            Option<UnaryOperator<TrinoRow>> outputConverter;
            try (HoodieFileGroupReader<TrinoRow> logReader = HoodieFileGroupReader.<TrinoRow>builder()
                    .withReaderContext(readerContext)
                    .withTableState(tableHandle.getTableState())
                    .withStorage(logStorage)
                    .withBaseFileOption(Option.of(positionedBaseFile))
                    .withLogFiles(fileSlice.getLogFiles())
                    .withPartitionPath(fileSlice.getPartitionPath())
                    .withDataSchema(dataSchema)
                    .withRequestedSchema(HoodieSchema.fromAvroSchema(requestedSchema))
                    .withLatestCommitTime(tableHandle.getLatestCommitTime())
                    .withProps(logProps)
                    .withShouldUseRecordPosition(true)
                    .withStart(baseFile.getStart())
                    .withLength(baseFile.getLength())
                    .build()) {
                Map<Serializable, BufferedRecord<TrinoRow>> records = logReader.getLogRecordsByPositionOrKey();
                // Log records spilled to disk stay with the merge read, which streams them
                if (readerContext.getMergeMode() != mergeMode
                        || (records instanceof ExternalSpillableMap<?, ?> spillable && spillable.getDiskBasedMapNumEntries() > 0)) {
                    return Optional.empty();
                }
                // Without log records nothing is matched, and the row index is the cheapest match column
                byPosition = records.isEmpty() || readerContext.getShouldMergeUseRecordPosition();
                // A record missing a required field, as in a partial update block, needs the merge read
                Optional<LogRecordMatcher> recordMatcher = HudiVectorizedMorPageSource.logRecordMatcher(
                        records, byPosition, readerContext.getSchemaHandler().getRequiredSchema());
                if (recordMatcher.isEmpty()) {
                    return Optional.empty();
                }
                matcher = recordMatcher.get();
                merger = BufferedRecordMergerFactory.create(
                        readerContext,
                        mergeMode,
                        false,
                        readerContext.getRecordMerger(),
                        readerContext.getSchemaHandler().getRequiredSchema(),
                        readerContext.getPayloadClasses(logProps),
                        logProps,
                        Option.empty());
                outputConverter = readerContext.getSchemaHandler().getOutputConverter();
            }
            catch (IOException e) {
                throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Failed to read the log files of " + baseFile.getPath(), e);
            }
            finally {
                if (logStorage != storage) {
                    try {
                        logStorage.close();
                    }
                    catch (IOException _) {
                    }
                }
            }

            if (closed.getAsBoolean()) {
                // The split was closed while its log records loaded
                return Optional.of(new EmptyPageSource());
            }
            List<HiveColumnHandle> baseColumns = new ArrayList<>(dataColumns);
            int matchChannel = byPosition
                    ? addBaseColumn(baseColumns, PARQUET_ROW_INDEX_COLUMN)
                    : addBaseColumn(baseColumns, readColumnsByName.get(RECORD_KEY_METADATA_FIELD));
            int[] orderingChannels = new int[orderingFieldNames.size()];
            List<io.trino.spi.type.Type> orderingTypes = new ArrayList<>(orderingFieldNames.size());
            for (int field = 0; field < orderingChannels.length; field++) {
                HiveColumnHandle orderingColumn = readColumnsByName.get(orderingFieldNames.get(field).toLowerCase(Locale.ROOT));
                orderingChannels[field] = addBaseColumn(baseColumns, orderingColumn);
                orderingTypes.add(baseColumns.get(orderingChannels[field]).getType());
            }
            // Under commit time ordering a log record replaces its base row whatever the row holds, so a base row the
            // predicate prunes loses nothing: its log record is emitted as a leftover, and the engine filters the output
            boolean pushdown = byPosition && mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING && isMorVectorizedReadPredicatePushdownEnabled(session);
            HudiVectorizedMorPageSource.EarlyBaseOpen earlyBaseOpen = HudiVectorizedMorPageSource.EarlyBaseOpen.NONE;
            ConnectorPageSource basePageSource;
            if (earlyBase.isPresent() && byPosition) {
                // The early base read has this projection and pushdown: the data columns, then the row index
                basePageSource = joinBaseOpen(earlyBase.get());
                earlyBaseUsed = true;
                earlyBaseOpen = HudiVectorizedMorPageSource.EarlyBaseOpen.USED;
            }
            else {
                if (earlyBase.isPresent()) {
                    earlyBaseOpen = HudiVectorizedMorPageSource.EarlyBaseOpen.DISCARDED;
                }
                basePageSource = createBaseFilePageSource(
                        session, baseColumns, hudiSplit, fileSystem, sessionOptions, baseFile.getStart(), baseFile.getLength(), dynamicFilter, pushdown);
            }

            try {
                Optional<HudiVectorizedMorPageSource.BaseOrderingValues> baseOrderingValues = Optional.empty();
                if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
                    HoodieSchema orderingSchema = HoodieSchema.fromAvroSchema(constructSchema(dataSchema.toAvroSchema(), orderingFieldNames));
                    baseOrderingValues = Optional.of(new HudiVectorizedMorPageSource.BaseOrderingValues(
                            (TrinoRecordContext) readerContext.getRecordContext(), orderingSchema, orderingTypes, orderingChannels, orderingFieldNames));
                }
                Map<String, Integer> dataChannels = new HashMap<>();
                for (int channel = 0; channel < dataColumns.size(); channel++) {
                    dataChannels.putIfAbsent(dataColumns.get(channel).getName().toLowerCase(Locale.ROOT), channel);
                }
                int[] outputToBaseChannel = new int[outputColumns.size()];
                for (int channel = 0; channel < outputToBaseChannel.length; channel++) {
                    outputToBaseChannel[channel] = dataChannels.getOrDefault(outputColumns.get(channel).getName().toLowerCase(Locale.ROOT), -1);
                }
                return Optional.of(HudiVectorizedMorPageSource.create(
                        basePageSource,
                        matcher,
                        matchChannel,
                        mergeMode,
                        merger,
                        baseOrderingValues,
                        outputConverter,
                        outputColumns,
                        outputToBaseChannel,
                        prefilledColumnValues,
                        earlyBaseOpen));
            }
            catch (RuntimeException e) {
                closeAfterFailure(basePageSource, e);
                throw e;
            }
        }
        finally {
            if (!earlyBaseUsed) {
                earlyBase.ifPresent(HudiPageSourceProvider::discardBaseOpen);
            }
        }
    }

    private static void closeAfterFailure(ConnectorPageSource pageSource, RuntimeException failure)
    {
        try {
            pageSource.close();
        }
        catch (IOException | RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    /**
     * The page source an early base open opened, or the exception opening it threw.
     */
    private static ConnectorPageSource joinBaseOpen(CompletableFuture<ConnectorPageSource> baseOpen)
    {
        try {
            return baseOpen.join();
        }
        catch (CompletionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Closes the page source of an early base open the read does not use, once the open completes.
     */
    private static void discardBaseOpen(CompletableFuture<ConnectorPageSource> baseOpen)
    {
        baseOpen.whenComplete((pageSource, _) -> {
            if (pageSource != null) {
                try {
                    pageSource.close();
                }
                catch (IOException | RuntimeException e) {
                    log.debug(e, "Failed to close an unused base file page source");
                }
            }
        });
    }

    /**
     * Whether the vectorized MOR read can merge a table's log records: commit time or event time ordering, which pick
     * one whole record by commit order or by ordering value, with no payload class other than the two those orderings
     * stand for, no partial update mode in the table config, record keys in the meta fields and top-level ordering fields.
     * Everything else keeps the merge read, as does a split whose log records turn out not to fit the vectorized read
     * (spilled to disk, keyed by both position and record key, or missing a required field as partial updates do).
     */
    @VisibleForTesting
    static boolean isVectorizedMorReadSupported(HoodieTableConfig tableConfig, TypedProperties readerProps, RecordMergeMode mergeMode)
    {
        if (mergeMode != RecordMergeMode.COMMIT_TIME_ORDERING && mergeMode != RecordMergeMode.EVENT_TIME_ORDERING) {
            return false;
        }
        String payloadClass = tableConfig.getPayloadClass();
        if (payloadClass != null && !payloadClass.isEmpty() && !VECTORIZED_MERGE_PAYLOAD_CLASSES.contains(payloadClass)) {
            return false;
        }
        if (tableConfig.getPartialUpdateMode().isPresent() || !tableConfig.populateMetaFields()) {
            return false;
        }
        if (HoodieRecordUtils.getOrderingFieldNames(mergeMode, tableConfig).stream().anyMatch(name -> name.contains("."))) {
            return false;
        }
        return !ConfigUtils.getStringWithAltKeys(readerProps, HoodieReaderConfig.MERGE_TYPE, true).equalsIgnoreCase(HoodieReaderConfig.REALTIME_SKIP_MERGE);
    }

    /**
     * The channel of {@code column} in {@code baseColumns}, by name, appending it when absent.
     */
    private static int addBaseColumn(List<HiveColumnHandle> baseColumns, HiveColumnHandle column)
    {
        requireNonNull(column, "column is null");
        for (int channel = 0; channel < baseColumns.size(); channel++) {
            if (baseColumns.get(channel).getName().equalsIgnoreCase(column.getName())) {
                return channel;
            }
        }
        baseColumns.add(column);
        return baseColumns.size() - 1;
    }

    /**
     * The log files of a split that {@link PrefetchedLogFileStorage} fetches: those the log reader opens as files, with
     * their sizes. Native log files are read as parquet through their own page sources.
     */
    private static List<HoodieLogFile> prefetchedLogFiles(HudiSplit hudiSplit)
    {
        return hudiSplit.getLogFiles().stream()
                .map(logFile -> new HoodieLogFile(new StoragePath(logFile.getPath()), logFile.getFileSize()))
                .filter(logFile -> !logFile.isNativeLogFile())
                .toList();
    }

    private static <T> HoodieFileGroupReader<T> buildFileGroupReader(
            HoodieReaderContext<T> readerContext,
            TableState tableState,
            HoodieStorage storage,
            FileSlice fileSlice,
            HoodieSchema dataSchema,
            Schema requestedSchema,
            String latestCommitTime,
            TypedProperties readerProps,
            long start,
            long length)
    {
        return HoodieFileGroupReader.<T>builder()
                .withReaderContext(readerContext)
                .withTableState(tableState)
                .withStorage(storage)
                .withBaseFileOption(fileSlice.getBaseFile())
                .withLogFiles(fileSlice.getLogFiles())
                .withPartitionPath(fileSlice.getPartitionPath())
                .withDataSchema(dataSchema)
                .withRequestedSchema(HoodieSchema.fromAvroSchema(requestedSchema))
                .withLatestCommitTime(latestCommitTime)
                .withProps(readerProps)
                .withShouldUseRecordPosition(false)
                .withStart(start)
                .withLength(length)
                .build();
    }

    private ConnectorPageSource createBaseFilePageSource(
            ConnectorSession session,
            List<HiveColumnHandle> columns,
            HudiSplit hudiSplit,
            TrinoFileSystem fileSystem,
            ParquetReaderOptions sessionOptions,
            long start,
            long length,
            DynamicFilter dynamicFilter,
            boolean enablePredicatePushDown)
    {
        HudiBaseFile baseFile = hudiSplit.getBaseFile().orElseThrow();
        return createPageSource(
                session,
                columns,
                hudiSplit,
                fileSystem.newInputFile(Location.of(baseFile.getPath()), baseFile.getFileSize()),
                baseFile.getPath(),
                start,
                length,
                OptionalLong.of(baseFile.getFileSize()),
                dataSourceStats,
                sessionOptions,
                timeZone,
                dynamicFilter,
                enablePredicatePushDown);
    }

    /**
     * Mirrors {@code FileGroupReaderSchemaHandler.generateRequiredSchema}'s full-schema decision for
     * CUSTOM merge mode: resolves the merge mode and strategy id with the version-gated inference the
     * file-group reader applies ({@link HudiUtil#resolveMergeModeAndStrategyId}) and asks the same
     * resolved merger whether it is projection compatible. Only this decision is mirrored exactly; the
     * mandatory-fields side ({@link HudiUtil#getMergeRequiredColumnHandles}) is a superset prediction,
     * and the {@code HudiTrinoReaderContext.getFileRecordIterator} guard catches any residual drift.
     */
    private static boolean requiresFullSchemaRead(HoodieTableConfig tableConfig, TypedProperties readerProps)
    {
        Pair<RecordMergeMode, String> mergeModeAndStrategyId = resolveMergeModeAndStrategyId(tableConfig);
        String mergeImplClasses = readerProps.getString(RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY,
                readerProps.getString(RECORD_MERGE_IMPL_CLASSES_DEPRECATED_WRITE_CONFIG_KEY, ""));
        return usesNonProjectionCompatibleMerger(mergeModeAndStrategyId.getLeft(), mergeModeAndStrategyId.getRight(), mergeImplClasses);
    }

    /**
     * Builds the properties passed to the {@link HoodieFileGroupReader}, starting from the persisted table config
     * and layering in any custom record merger implementation classes configured on the connector or session.
     * The merger impl classes are a read/write config and are not persisted in {@code hoodie.properties}, so they
     * must be supplied here for {@link HudiTrinoReaderContext#getRecordMerger} to resolve a CUSTOM record merger.
     */
    private static TypedProperties buildReaderProperties(ConnectorSession session, HoodieTableConfig tableConfig)
    {
        TypedProperties props = new TypedProperties();
        TypedProperties.putAll(props, tableConfig.getProps());
        List<String> recordMergerImpls = getRecordMergerImpls(session);
        if (!recordMergerImpls.isEmpty()) {
            props.setProperty(RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY, String.join(",", recordMergerImpls));
        }
        return props;
    }

    static ConnectorPageSource createPageSource(
            ConnectorSession session,
            List<HiveColumnHandle> columns,
            HudiSplit hudiSplit,
            TrinoInputFile inputFile,
            String path,
            long start,
            long length,
            OptionalLong estimatedFileSize,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderOptions options,
            DateTimeZone timeZone,
            DynamicFilter dynamicFilter,
            boolean enablePredicatePushDown)
    {
        ParquetDataSource dataSource = null;
        boolean useColumnNames = shouldUseParquetColumnNames(session);
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            dataSource = createDataSource(inputFile, estimatedFileSize, options, memoryContext, dataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // When not using columnNames, physical indexes are used and there could be cases when the physical index in HiveColumnHandle is different from the fileSchema of the
            // parquet files. This could happen when schema evolution happened. In such a case, we will need to remap the column indices in the HiveColumnHandles.
            // The projection and the predicate resolve the same names against the same file, so the name-to-position
            // map is built once per split and shared: one lookup table means the two can never disagree about which
            // physical column a name denotes, and a wide table pays for the lower-casing pass only once.
            // HiveColumnHandle names are in lower case, case-insensitive
            Optional<Map<String, Integer>> physicalIndexMap = Optional.empty();
            if (!useColumnNames) {
                Map<String, Integer> indexMap = buildPhysicalIndexMap(fileSchema, false);
                columns = remapColumnIndicesToPhysical(fileSchema, columns, indexMap, false);
                physicalIndexMap = Optional.of(indexMap);
            }

            Optional<MessageType> message = getParquetMessageType(columns, useColumnNames, fileSchema);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);

            // A domain typed by the metastore cannot be matched against the statistics of a column the file stores
            // under the type it had before a schema evolution, so those are dropped before the parquet predicate
            // ever sees them. See ParquetStatisticsDomains.
            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics() || !enablePredicatePushDown
                    ? TupleDomain.all()
                    : dropIncomparableDomains(getParquetTupleDomain(descriptorsByPath, getPushdownPredicate(hudiSplit, dynamicFilter, physicalIndexMap), fileSchema, useColumnNames));

            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);

            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    start,
                    length,
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    timeZone,
                    DOMAIN_COMPACTION_THRESHOLD,
                    options);

            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetDataSource finalDataSource = dataSource;
            ParquetReaderProvider parquetReaderProvider = (fields, appendRowNumberColumn) -> new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    fields,
                    appendRowNumberColumn,
                    rowGroups,
                    finalDataSource,
                    timeZone,
                    memoryContext,
                    options,
                    exception -> handleException(dataSourceId, exception),
                    Optional.of(parquetPredicate),
                    Optional.empty(),
                    parquetMetadata.getDecryptionContext());

            // Off, no column would need a conversion anyway (metastore types never contain uuid, and metastore arrays
            // map to parquet LIST groups); the gate keeps that path visibly untouched.
            if (!isSchemaColumnTypesEnabled(session)) {
                return createParquetPageSource(columns, fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
            }
            List<Optional<ReadConversion>> readConversions = readConversions(columns, fileSchema, useColumnNames);
            ConnectorPageSource pageSource = createParquetPageSource(withReadTypes(columns, readConversions), fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
            return toColumnTypes(pageSource, readConversions);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException _) {
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(HUDI_BAD_DATA, e);
            }
            String message = "Error opening Hudi split %s (offset=%s, length=%s): %s".formatted(path, start, length, e.getMessage());
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * The conversion each column needs from the type the parquet reader can produce for it to its column type, see
     * {@link ParquetReadConversions}. Only base data columns are considered: this connector never pushes a
     * dereference down, and the page source is only ever asked for data columns.
     */
    private static List<Optional<ReadConversion>> readConversions(List<HiveColumnHandle> columns, MessageType fileSchema, boolean useColumnNames)
    {
        ImmutableList.Builder<Optional<ReadConversion>> conversions = ImmutableList.builderWithExpectedSize(columns.size());
        for (HiveColumnHandle column : columns) {
            if (!column.isBaseColumn() || column.getColumnType() != HiveColumnHandle.ColumnType.REGULAR) {
                conversions.add(Optional.empty());
                continue;
            }
            Optional<Type> parquetType = getColumnType(column, fileSchema, useColumnNames);
            conversions.add(parquetType.flatMap(type -> ParquetReadConversions.forColumn(column.getType(), type)));
        }
        return conversions.build();
    }

    /**
     * The handles the parquet reader decodes with: {@code createParquetPageSource} reads each column as its handle's
     * base type, so a column that needs a conversion is read as the conversion's read type.
     */
    private static List<HiveColumnHandle> withReadTypes(List<HiveColumnHandle> columns, List<Optional<ReadConversion>> readConversions)
    {
        ImmutableList.Builder<HiveColumnHandle> readColumns = ImmutableList.builderWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);
            readColumns.add(readConversions.get(i)
                    .map(conversion -> new HiveColumnHandle(
                            column.getBaseColumnName(),
                            column.getBaseHiveColumnIndex(),
                            column.getBaseHiveType(),
                            conversion.readType(),
                            column.getHiveColumnProjectionInfo(),
                            column.getColumnType(),
                            column.getComment()))
                    .orElse(column));
        }
        return readColumns.build();
    }

    /**
     * Wraps {@code pageSource} so the blocks it returns are of the column types; channels without a conversion pass
     * through untouched.
     */
    private static ConnectorPageSource toColumnTypes(ConnectorPageSource pageSource, List<Optional<ReadConversion>> readConversions)
    {
        if (readConversions.stream().allMatch(Optional::isEmpty)) {
            return pageSource;
        }
        TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
        for (int channel = 0; channel < readConversions.size(); channel++) {
            transforms.column(channel, readConversions.get(channel).map(ReadConversion::toColumnType));
        }
        return transforms.build(pageSource);
    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(HUDI_BAD_DATA, exception);
        }
        return new TrinoException(HUDI_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }

    /**
     * Creates a new list of ColumnHandles where the index associated with each handle corresponds to its physical position within the provided fileSchema (MessageType).
     * This is necessary when a downstream component relies on the handle's index for physical data access, and the logical schema order (potentially reflected in the
     * original handles) differs from the physical file layout.
     * <p>
     * A requested column the file schema does not carry is mapped one past the last physical field instead of failing: base files written before the column was added
     * legitimately lack it (schema evolution), and so do old records for a merge-required column. {@code ParquetPageSourceFactory} reads an index-based column through
     * {@code getBaseColumnParquetType}, which reports any index at or beyond the file's field count as absent, so the parquet reader skips it and the page source emits
     * a null block -- the same result the name-based path ({@code hudi.parquet.use-column-names=true}) produces.
     *
     * @param fileSchema The MessageType representing the physical schema of the Parquet file.
     * @param requestedColumns The original list of Trino ColumnHandles as received from the engine.
     * @param caseSensitive Whether the lookup between Trino column names (from handles) and Parquet field names (from fileSchema) should be case-sensitive.
     * @return A new list of HiveColumnHandle, preserving the original order, but with each handle containing the correct physical index relative to fileSchema.
     */
    @VisibleForTesting
    public static List<HiveColumnHandle> remapColumnIndicesToPhysical(
            MessageType fileSchema,
            List<HiveColumnHandle> requestedColumns,
            boolean caseSensitive)
    {
        // Create a map from column name to its physical index in the fileSchema.
        return remapColumnIndicesToPhysical(fileSchema, requestedColumns, buildPhysicalIndexMap(fileSchema, caseSensitive), caseSensitive);
    }

    /**
     * {@link #remapColumnIndicesToPhysical(MessageType, List, boolean)} against a {@code physicalIndexMap} the caller
     * already built, so a split that remaps both its projection and its predicate builds the map once.
     * {@code caseSensitive} must be the one the map was built with, or the lookups miss.
     */
    private static List<HiveColumnHandle> remapColumnIndicesToPhysical(
            MessageType fileSchema,
            List<HiveColumnHandle> requestedColumns,
            Map<String, Integer> physicalIndexMap,
            boolean caseSensitive)
    {
        // Iterate through the columns requested by Trino IN ORDER.
        List<HiveColumnHandle> remappedHandles = new ArrayList<>(requestedColumns.size());
        for (HiveColumnHandle originalHandle : requestedColumns) {
            // The parquet reader recognizes the row index column by identity
            if (originalHandle == PARQUET_ROW_INDEX_COLUMN) {
                remappedHandles.add(originalHandle);
                continue;
            }
            // Find the physical index from the file schema map constructed from fileSchema. A column the file
            // does not carry keeps an index one past the last field, which the parquet reader null-fills.
            Integer physicalIndex = physicalIndexMap.get(normalizeColumnName(originalHandle.getBaseColumnName(), caseSensitive));
            remappedHandles.add(withPhysicalIndex(originalHandle, physicalIndex == null ? fileSchema.getFieldCount() : physicalIndex));
        }

        return remappedHandles;
    }

    /**
     * Rebuilds a predicate's column handles on physical file ordinals, the predicate-side counterpart of
     * {@link #remapColumnIndicesToPhysical}. With {@code hudi.parquet.use-column-names=false},
     * {@code ParquetPageSourceFactory.getParquetTupleDomain} resolves a predicate column positionally, as
     * {@code fileSchema.getType(handle.getBaseHiveColumnIndex())}, but the handles reaching it carry METASTORE
     * ordinals: a metastore that omits the Hudi meta fields (hive sync with {@code omit_metadata_fields=true})
     * shifts every data column, and so does reordering or dropping one. Left unremapped, the domain attaches to
     * whichever column happens to sit at the stale ordinal and row groups are pruned on that column's statistics,
     * silently dropping rows.
     * <p>
     * Resolution is by name, so the predicate ends up bound to exactly the column the projection reads - which is
     * the property that matters, since the two are compared against each other. It is not a defence against a
     * column being dropped and re-added under full schema evolution: name-based binding will match the new column
     * to the old one, exactly as the projection remap and the whole {@code use-column-names=true} mode already do.
     * <p>
     * A column the file does not carry is dropped from the predicate rather than mapped to the
     * {@link #remapColumnIndicesToPhysical} sentinel, which every absent column would share. Dropping loses row
     * group pruning but never a row: the static half of the predicate is handed back to the engine in full as
     * {@code HudiMetadata.applyFilter}'s remaining filter, and the dynamic half is by construction redundant with
     * the join above the scan. It is also what already happens today for a predicate column the query does not
     * read, since {@code descriptorsByPath} is derived from the projection and
     * {@code getParquetTupleDomain} skips any column it cannot resolve.
     *
     * @param fileSchema The MessageType representing the physical schema of the Parquet file.
     * @param predicate The predicate to push down, keyed on handles carrying metastore ordinals.
     * @param caseSensitive Whether the lookup between Trino column names (from handles) and Parquet field names (from fileSchema) should be case-sensitive.
     * @return The same domains, keyed on handles carrying physical ordinals, minus the columns the file lacks.
     */
    @VisibleForTesting
    public static TupleDomain<HiveColumnHandle> remapPredicateColumnIndicesToPhysical(
            MessageType fileSchema,
            TupleDomain<HiveColumnHandle> predicate,
            boolean caseSensitive)
    {
        return remapPredicateColumnIndicesToPhysical(predicate, buildPhysicalIndexMap(fileSchema, caseSensitive), caseSensitive);
    }

    /**
     * {@link #remapPredicateColumnIndicesToPhysical(MessageType, TupleDomain, boolean)} against a
     * {@code physicalIndexMap} the caller already built, so a split that remaps both its projection and its predicate
     * builds the map once. {@code caseSensitive} must be the one the map was built with, or the lookups miss.
     */
    private static TupleDomain<HiveColumnHandle> remapPredicateColumnIndicesToPhysical(
            TupleDomain<HiveColumnHandle> predicate,
            Map<String, Integer> physicalIndexMap,
            boolean caseSensitive)
    {
        if (predicate.isAll() || predicate.isNone()) {
            return predicate;
        }

        Set<Map.Entry<Integer, Optional<HiveColumnProjectionInfo>>> pushedFields = new HashSet<>();
        Map<HiveColumnHandle, Domain> remappedDomains = new LinkedHashMap<>();
        for (Map.Entry<HiveColumnHandle, Domain> entry : predicate.getDomains().orElseThrow().entrySet()) {
            Integer physicalIndex = physicalIndexMap.get(normalizeColumnName(entry.getKey().getBaseColumnName(), caseSensitive));
            if (physicalIndex == null) {
                continue;
            }
            // Deduplicate on what getParquetTupleDomain resolves the handle to rather than on the handle itself: two
            // handles whose names differ only by case resolve to one file column while staying unequal to each other,
            // and pushing both down would hand it the same ColumnDescriptor twice, which it rejects by failing the
            // split. The base column alone is too coarse a key, because a dereference handle carries its subfield
            // path into the descriptor, so the projection is part of the key and two projections of one base column
            // both survive. Neither collision is reachable today - the case one needs a metastore holding two such
            // columns, which Hive's name normalisation rules out, and trino-parquet lower-cases every field name when
            // it builds the MessageType from the footer anyway - but keeping only the first domain is a cheap
            // guarantee that the read can never be made worse than pushing nothing down.
            if (pushedFields.add(Map.entry(physicalIndex, entry.getKey().getHiveColumnProjectionInfo()))) {
                remappedDomains.put(withPhysicalIndex(entry.getKey(), physicalIndex), entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(remappedDomains);
    }

    /**
     * Maps each of {@code fileSchema}'s top-level field names to its physical position.
     */
    private static Map<String, Integer> buildPhysicalIndexMap(MessageType fileSchema, boolean caseSensitive)
    {
        Map<String, Integer> physicalIndexMap = new HashMap<>();
        List<Type> fileFields = fileSchema.getFields();
        for (int i = 0; i < fileFields.size(); i++) {
            physicalIndexMap.put(normalizeColumnName(fileFields.get(i).getName(), caseSensitive), i);
        }
        return physicalIndexMap;
    }

    private static String normalizeColumnName(String columnName, boolean caseSensitive)
    {
        return caseSensitive ? columnName : columnName.toLowerCase(Locale.ROOT);
    }

    /**
     * Copies {@code handle} with its base column index replaced by a physical one, every other attribute carried
     * over unchanged. Note that the constructor's fourth argument is the BASE type: it differs from
     * {@code getType()} only for a dereference handle, whose {@code getType()} is the projected subfield's type
     * rather than the column's, and {@code createParquetPageSource} reads the base type throughout.
     * <p>
     * Copying a dereference handle's projection across matters: {@code createParquetPageSource} branches on
     * {@code isBaseColumn()} and dereferences through {@code getHiveColumnProjectionInfo}, and it reads the base
     * column's stored {@code baseType} on the way. The connector never produces such a handle today, because
     * {@code HudiMetadata} does not implement {@code applyProjection}.
     */
    private static HiveColumnHandle withPhysicalIndex(HiveColumnHandle handle, int physicalIndex)
    {
        return new HiveColumnHandle(
                handle.getBaseColumnName(),
                physicalIndex,
                handle.getBaseHiveType(),
                handle.getBaseType(),
                handle.getHiveColumnProjectionInfo(),
                handle.getColumnType(),
                handle.getComment());
    }

    /**
     * Resolves the predicate handed to {@code ParquetPageSourceFactory.getParquetTupleDomain}. Only the
     * positional mode needs the handles rebuilt; when columns are resolved by name the metastore ordinals are
     * never read, which is exactly when {@code physicalIndexMap} is empty. Being handed the very map the projection
     * was remapped with is what makes it structural, rather than a convention, that the two agree about which
     * physical column a name denotes.
     */
    private static TupleDomain<HiveColumnHandle> getPushdownPredicate(
            HudiSplit hudiSplit,
            DynamicFilter dynamicFilter,
            Optional<Map<String, Integer>> physicalIndexMap)
    {
        TupleDomain<HiveColumnHandle> combinedPredicate = getCombinedPredicate(hudiSplit, dynamicFilter);
        return physicalIndexMap
                .map(indexMap -> remapPredicateColumnIndicesToPhysical(combinedPredicate, indexMap, false))
                .orElse(combinedPredicate);
    }

    private static TupleDomain<HiveColumnHandle> getCombinedPredicate(HudiSplit hudiSplit, DynamicFilter dynamicFilter)
    {
        // Combine static and dynamic predicates
        TupleDomain<HiveColumnHandle> staticPredicate = hudiSplit.getPredicate();
        TupleDomain<HiveColumnHandle> dynamicPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(HiveColumnHandle.class::cast);
        TupleDomain<HiveColumnHandle> combinedPredicate = staticPredicate.intersect(dynamicPredicate);

        if (!combinedPredicate.isAll()) {
            log.debug("Combined predicate for Parquet read (Split: %s): %s", hudiSplit, combinedPredicate);
        }
        return combinedPredicate;
    }

    private static List<HiveColumnHandle> getHiveColumns(List<ColumnHandle> columns)
    {
        return columns.stream()
                .map(HiveColumnHandle.class::cast)
                .toList();
    }
}
