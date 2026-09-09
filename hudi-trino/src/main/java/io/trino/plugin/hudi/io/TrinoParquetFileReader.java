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
package io.trino.plugin.hudi.io;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.SqlVarbinary;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

/**
 * Reads an LSM archived-timeline Parquet file through Trino's {@link ParquetReader}, turning each
 * {@link Page} it produces into an Avro {@link IndexedRecord} with {@link HudiAvroSerializer}. Hudi's
 * archived-timeline loader asks {@link HudiTrinoFileReaderFactory} for an Avro file reader over the
 * history files under {@code .hoodie/timeline/history}, and this is the connector's answer to that
 * request. It is not a general data-file reader: record-key, key-prefix and row-key lookups are
 * unsupported, and so are the bloom filter and min/max record key lookups -- a timeline file carries
 * none of the data-file footer metadata those rely on.
 */
public class TrinoParquetFileReader
        extends HoodieAvroFileReader
{
    private static final String PARQUET_AVRO_SCHEMA_KEY = "parquet.avro.schema";
    private static final DateTimeZone UTC_TIME_ZONE = DateTimeZone.UTC;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final StoragePath path;
    private final ParquetReaderOptions readerOptions = new ParquetReaderConfig().toParquetReaderOptions();
    private final TrinoInputFile inputFile;
    private final long fileLength;
    private final ParquetMetadata parquetMetadata;
    private final HoodieSchema hoodieSchema;
    private final long totalRecords;
    // Every iterator handed out by getIndexedRecordIterator, so that close() can release the ParquetReader
    // of one the caller left open. A reader instance is used by a single thread, but the list is cheap to guard
    private final List<ParquetIndexedRecordIterator> openIterators = new ArrayList<>();

    public TrinoParquetFileReader(HoodieStorage storage, StoragePath path)
    {
        this.path = requireNonNull(path, "path is null");
        requireNonNull(storage, "storage is null");
        checkArgument(storage instanceof HudiTrinoStorage, "storage must be an instance of HudiTrinoStorage");
        HudiTrinoStorage trinoStorage = (HudiTrinoStorage) storage;

        // HudiTrinoStorage#getFileSystem is typed Object so that hudi-common stays free of Trino types
        TrinoFileSystem fileSystem = (TrinoFileSystem) trinoStorage.getFileSystem();
        this.inputFile = fileSystem.newInputFile(HudiTrinoStorage.convertToLocation(path));
        try {
            this.fileLength = inputFile.length();
            this.parquetMetadata = readParquetMetadata();
            // The footer's row-group metadata is parsed lazily, so getBlocks() can fail the way readFooter does
            this.totalRecords = parquetMetadata.getBlocks().stream()
                    .mapToLong(BlockMetadata::rowCount)
                    .sum();
        }
        catch (IOException e) {
            // Failing to open the file surfaces the way a failing read does: a corrupt footer as HUDI_BAD_DATA,
            // anything else as HUDI_CURSOR_ERROR, each with its cause attached
            throw handleException(path, e);
        }
        Schema avroSchema = extractAvroSchema(parquetMetadata.getFileMetaData());
        this.hoodieSchema = HoodieSchema.fromAvroSchema(avroSchema);
    }

    @Override
    public ClosableIterator<IndexedRecord> getIndexedRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema, Map<String, String> renamedColumns)
    {
        // Timeline files are never schema-evolved, so no column can have been renamed under them
        HoodieSchema projectedSchema = requestedSchema != null ? requestedSchema : hoodieSchema;
        ParquetIndexedRecordIterator iterator = new ParquetIndexedRecordIterator(projectedSchema);
        synchronized (openIterators) {
            openIterators.add(iterator);
        }
        return iterator;
    }

    @Override
    public ClosableIterator<IndexedRecord> getIndexedRecordsByKeysIterator(List<String> keys, HoodieSchema readerSchema)
    {
        throw new UnsupportedOperationException("Reading records by keys is not supported by this reader");
    }

    @Override
    public ClosableIterator<IndexedRecord> getIndexedRecordsByKeyPrefixIterator(List<String> sortedKeyPrefixes, HoodieSchema readerSchema)
    {
        throw new UnsupportedOperationException("Reading records by key prefixes is not supported by this reader");
    }

    @Override
    public String[] readMinMaxRecordKeys()
    {
        throw new UnsupportedOperationException("Reading min/max record keys is not supported by this reader");
    }

    @Override
    public BloomFilter readBloomFilter()
    {
        throw new UnsupportedOperationException("Reading a bloom filter is not supported by this reader");
    }

    @Override
    public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys)
    {
        throw new UnsupportedOperationException("Filtering row keys is not supported by this reader");
    }

    @Override
    public ClosableIterator<String> getRecordKeyIterator()
    {
        throw new UnsupportedOperationException("Iterating over only record keys is not supported by this reader");
    }

    @Override
    public HoodieSchema getSchema()
    {
        return hoodieSchema;
    }

    @Override
    public long getTotalRecords()
    {
        return totalRecords;
    }

    @Override
    public void close()
    {
        // The only resource this reader opens outside the constructor is the ParquetReader of an iterator.
        // Closing the reader releases every iterator it handed out, including any the caller left open;
        // an iterator's close() is idempotent, so closing one that is already closed is a no-op. One that fails
        // to close does not stop the others from being closed: the first failure is rethrown once every iterator
        // has been released, with the later failures attached to it as suppressed
        synchronized (openIterators) {
            RuntimeException failure = null;
            for (ParquetIndexedRecordIterator iterator : openIterators) {
                try {
                    iterator.close();
                }
                catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
            }
            openIterators.clear();
            if (failure != null) {
                throw failure;
            }
        }
    }

    private ParquetMetadata readParquetMetadata()
            throws IOException
    {
        // No estimated size: with one at or below the small-file threshold createDataSource returns a
        // MemoryParquetDataSource that slurps the whole file, where the footer read only needs the tail
        try (ParquetDataSource dataSource = openDataSource(newSimpleAggregatedMemoryContext(), OptionalLong.empty())) {
            return MetadataReader.readFooter(dataSource, Optional.empty());
        }
    }

    private ParquetDataSource openDataSource(AggregatedMemoryContext memoryContext, OptionalLong estimatedFileSize)
            throws IOException
    {
        return createDataSource(inputFile, estimatedFileSize, readerOptions, memoryContext, new FileFormatDataSourceStats());
    }

    private Schema extractAvroSchema(FileMetadata fileMetaData)
    {
        String avroSchemaStr = fileMetaData.getKeyValueMetaData().get(PARQUET_AVRO_SCHEMA_KEY);
        if (avroSchemaStr == null) {
            throw new TrinoException(HUDI_SCHEMA_ERROR, "Parquet file does not contain Avro schema in metadata: " + path);
        }
        return new Schema.Parser().parse(avroSchemaStr);
    }

    /**
     * One {@link HiveColumnHandle} per field of the projection, typed from the field's Avro schema the way
     * {@link HudiUtil#toColumnHandle} types the handles of a data-file read. Handle {@code i} is built from
     * field {@code i}, so a handle's position in the returned list is its field's position in the records this
     * reader produces. That list position is what {@link #binaryFieldPositions} relies on, not the handle's own
     * column index, which {@code toColumnHandle} leaves at 0.
     */
    private static List<HiveColumnHandle> buildColumnHandles(HoodieSchema projectedSchema)
    {
        return projectedSchema.getFields().stream()
                .map(HudiUtil::toColumnHandle)
                .toList();
    }

    private static List<Column> buildTrinoColumns(List<HiveColumnHandle> columnHandles, MessageColumnIO messageColumnIO)
    {
        ImmutableList.Builder<Column> columnsBuilder = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            Field parquetField = constructField(columnHandle.getType(), lookupColumnByName(messageColumnIO, name))
                    .orElseThrow(() -> new TrinoException(HUDI_SCHEMA_ERROR, "Could not find column: " + name));
            columnsBuilder.add(new Column(name, parquetField));
        }
        return columnsBuilder.build();
    }

    /**
     * Record positions of the projection's VARBINARY columns, if any. Trino hands a VARBINARY value out as a
     * {@link SqlVarbinary}, while Avro's in-memory representation of {@code bytes} is a {@link ByteBuffer} --
     * and that is what hudi-common casts to when it reads the {@code metadata} and {@code plan} columns of an
     * LSM instant, so those values have to be converted before the record leaves this reader.
     */
    private static int[] binaryFieldPositions(List<HiveColumnHandle> columnHandles)
    {
        return IntStream.range(0, columnHandles.size())
                .filter(position -> columnHandles.get(position).getType().equals(VARBINARY))
                .toArray();
    }

    private static TrinoException handleException(StoragePath path, Exception exception)
    {
        if (exception instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(HUDI_BAD_DATA, exception);
        }
        return new TrinoException(HUDI_CURSOR_ERROR, "Failed to read Parquet file: " + path, exception);
    }

    private class ParquetIndexedRecordIterator
            implements ClosableIterator<IndexedRecord>
    {
        private final ParquetReader parquetReader;
        private final HudiAvroSerializer avroSerializer;
        private final int[] binaryFieldPositions;
        private Page currentPage;
        private int currentPosition;
        private boolean exhausted;
        private boolean closed;

        ParquetIndexedRecordIterator(HoodieSchema projectedSchema)
        {
            List<HiveColumnHandle> columnHandles = buildColumnHandles(projectedSchema);
            this.parquetReader = createParquetReader(columnHandles);
            // Null prefilled values: PrefilledColumnValues answers partition and hidden metadata columns of a
            // split, of which a timeline read has neither, and serialize() only ever reads page values. There
            // is no split here to build one from -- create(HudiSplit) is its only factory.
            this.avroSerializer = new HudiAvroSerializer(columnHandles, null, projectedSchema.toAvroSchema());
            this.binaryFieldPositions = binaryFieldPositions(columnHandles);
        }

        @Override
        public boolean hasNext()
        {
            if (closed || exhausted) {
                return false;
            }
            if (currentPage != null && currentPosition < currentPage.getPositionCount()) {
                return true;
            }
            try {
                loadNextPage();
                return !exhausted;
            }
            catch (IOException | RuntimeException e) {
                // loadNextPage() decodes the page eagerly, and a data page that fails to decode surfaces as a
                // ParquetDecodingException, which is unchecked: ParquetReader routes only the IOExceptions of a
                // page read through the exception transform it is handed
                throw handleException(path, e);
            }
        }

        @Override
        public IndexedRecord next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            IndexedRecord record = avroSerializer.serialize(currentPage, currentPosition);
            for (int fieldPosition : binaryFieldPositions) {
                if (record.get(fieldPosition) instanceof SqlVarbinary value) {
                    record.put(fieldPosition, ByteBuffer.wrap(value.getBytes()));
                }
            }
            currentPosition++;
            return record;
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                currentPage = null;
                try {
                    // Also closes the underlying ParquetDataSource
                    parquetReader.close();
                }
                catch (IOException e) {
                    throw handleException(path, e);
                }
            }
        }

        private ParquetReader createParquetReader(List<HiveColumnHandle> columnHandles)
        {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            ParquetDataSource dataSource = null;
            try {
                dataSource = openDataSource(memoryContext, OptionalLong.of(fileLength));
                FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
                MessageType fileSchema = fileMetaData.getSchema();
                MessageType requestedSchema = getParquetMessageType(columnHandles, true, fileSchema)
                        .orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
                List<Column> columns = buildTrinoColumns(columnHandles, getColumnIO(fileSchema, requestedSchema));

                Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
                TupleDomain<ColumnDescriptor> tupleDomain = TupleDomain.all();
                TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, tupleDomain, descriptorsByPath, UTC_TIME_ZONE);
                List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                        0,
                        fileLength,
                        dataSource,
                        parquetMetadata,
                        ImmutableList.of(tupleDomain),
                        ImmutableList.of(parquetPredicate),
                        descriptorsByPath,
                        UTC_TIME_ZONE,
                        DOMAIN_COMPACTION_THRESHOLD,
                        readerOptions);

                return new ParquetReader(
                        Optional.ofNullable(fileMetaData.getCreatedBy()),
                        columns,
                        false,
                        rowGroups,
                        dataSource,
                        UTC_TIME_ZONE,
                        memoryContext,
                        readerOptions,
                        exception -> handleException(path, exception),
                        Optional.of(parquetPredicate),
                        Optional.empty(),
                        parquetMetadata.getDecryptionContext());
            }
            catch (IOException | RuntimeException e) {
                // The reader owns the data source only once it is constructed
                if (dataSource != null) {
                    try {
                        dataSource.close();
                    }
                    catch (IOException _) {
                    }
                }
                throw handleException(path, e);
            }
        }

        private void loadNextPage()
                throws IOException
        {
            SourcePage sourcePage = parquetReader.nextPage();
            // Once the reader has handed out its last page it must never be asked again: a nextPage() past
            // the end of the row groups throws IndexOutOfBoundsException instead of returning null a second time
            exhausted = sourcePage == null;
            currentPage = exhausted ? null : sourcePage.getPage();
            currentPosition = 0;
        }
    }
}
