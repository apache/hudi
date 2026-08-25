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
import io.trino.metastore.HiveType;
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
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.storage.HoodieAvroFileReader;
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
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;

/**
 * Reads an LSM archived-timeline Parquet file through Trino's {@link ParquetReader}, turning each
 * {@link Page} it produces into an Avro {@link IndexedRecord} with {@link HudiAvroSerializer}. Hudi's
 * archived-timeline loader asks {@link HudiTrinoFileReaderFactory} for an Avro file reader over the
 * history files under {@code .hoodie/timeline/history}, and this is the connector's answer to that
 * request. It is not a general data-file reader: record-key, key-prefix and row-key lookups are
 * unsupported, and the timeline files carry none of the footer metadata those lookups rely on.
 */
public class TrinoParquetFileReader
        extends HoodieAvroFileReader
{
    private static final String PARQUET_AVRO_SCHEMA_KEY = "parquet.avro.schema";
    private static final DateTimeZone UTC_TIME_ZONE = DateTimeZone.UTC;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final StoragePath path;
    private final HudiTrinoStorage trinoStorage;
    private final FileFormatUtils fileFormatUtils = new HudiTrinoParquetFileFormatUtils();
    private final ParquetReaderOptions readerOptions = new ParquetReaderConfig().toParquetReaderOptions();
    private final TrinoInputFile inputFile;
    private final long fileLength;
    private final ParquetMetadata parquetMetadata;
    private final Schema avroSchema;
    private final HoodieSchema hoodieSchema;
    private final long totalRecords;

    public TrinoParquetFileReader(HoodieStorage storage, StoragePath path)
            throws IOException
    {
        this.path = requireNonNull(path, "path is null");
        requireNonNull(storage, "storage is null");
        checkArgument(storage instanceof HudiTrinoStorage, "storage must be an instance of HudiTrinoStorage");
        this.trinoStorage = (HudiTrinoStorage) storage;

        // HudiTrinoStorage#getFileSystem is typed Object so that hudi-common stays free of Trino types
        TrinoFileSystem fileSystem = (TrinoFileSystem) trinoStorage.getFileSystem();
        this.inputFile = fileSystem.newInputFile(HudiTrinoStorage.convertToLocation(path));
        this.fileLength = inputFile.length();
        this.parquetMetadata = readParquetMetadata();
        this.avroSchema = extractAvroSchema(parquetMetadata.getFileMetaData());
        this.hoodieSchema = HoodieSchema.fromAvroSchema(avroSchema);
        this.totalRecords = parquetMetadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();
    }

    @Override
    public ClosableIterator<IndexedRecord> getIndexedRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema, Map<String, String> renamedColumns)
            throws IOException
    {
        // Timeline files are never schema-evolved, so no column can have been renamed under them
        Schema schema = requestedSchema != null ? requestedSchema.toAvroSchema() : avroSchema;
        return new ParquetIndexedRecordIterator(schema);
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
        // Not FileFormatUtils#readMinMaxRecordKeys: that one demands both footer keys and throws when either
        // is absent, and a timeline file carries no data-file footer keys at all
        Map<String, String> minMaxKeys = fileFormatUtils.readFooter(
                trinoStorage, false, path, HOODIE_MIN_RECORD_KEY_FOOTER, HOODIE_MAX_RECORD_KEY_FOOTER);
        if (minMaxKeys.size() != 2) {
            return new String[0];
        }
        return new String[] {minMaxKeys.get(HOODIE_MIN_RECORD_KEY_FOOTER), minMaxKeys.get(HOODIE_MAX_RECORD_KEY_FOOTER)};
    }

    @Override
    public BloomFilter readBloomFilter()
    {
        return fileFormatUtils.readBloomFilterFromMetadata(trinoStorage, path);
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
        // No-op: the only resource this reader opens outside the constructor is the ParquetReader of an
        // iterator, and the iterator closes it
    }

    private ParquetMetadata readParquetMetadata()
            throws IOException
    {
        try (ParquetDataSource dataSource = openDataSource(newSimpleAggregatedMemoryContext())) {
            return MetadataReader.readFooter(dataSource, Optional.empty());
        }
    }

    private ParquetDataSource openDataSource(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return createDataSource(inputFile, OptionalLong.of(fileLength), readerOptions, memoryContext, new FileFormatDataSourceStats());
    }

    private Schema extractAvroSchema(FileMetadata fileMetaData)
    {
        String avroSchemaStr = fileMetaData.getKeyValueMetaData().get(PARQUET_AVRO_SCHEMA_KEY);
        if (avroSchemaStr == null) {
            throw new TrinoException(HUDI_SCHEMA_ERROR, "Parquet file does not contain Avro schema in metadata: " + path);
        }
        return new Schema.Parser().parse(avroSchemaStr);
    }

    private static List<Column> buildTrinoColumns(Schema readerSchema, MessageColumnIO messageColumnIO)
    {
        ImmutableList.Builder<Column> columnsBuilder = ImmutableList.builder();
        for (Schema.Field field : readerSchema.getFields()) {
            Type trinoType = avroTypeToTrinoType(field.schema());
            Field parquetField = constructField(trinoType, lookupColumnByName(messageColumnIO, field.name()))
                    .orElseThrow(() -> new TrinoException(HUDI_SCHEMA_ERROR, "Could not find column: " + field.name()));
            columnsBuilder.add(new Column(field.name(), parquetField));
        }
        return columnsBuilder.build();
    }

    private static List<HiveColumnHandle> buildColumnHandles(Schema readerSchema)
    {
        List<HiveColumnHandle> columnHandles = new ArrayList<>();
        List<Schema.Field> fields = readerSchema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            Type trinoType = avroTypeToTrinoType(field.schema());
            HiveType hiveType = toHiveType(trinoType);
            columnHandles.add(new HiveColumnHandle(
                    field.name(),
                    i,
                    hiveType,
                    trinoType,
                    Optional.empty(),
                    HiveColumnHandle.ColumnType.REGULAR,
                    Optional.empty()));
        }
        return columnHandles;
    }

    private static Type avroTypeToTrinoType(Schema fieldSchema)
    {
        // Handle Avro's nullable fields, which are represented as a UNION of null and a type
        if (fieldSchema.isUnion()) {
            List<Schema> nonNullSchemas = fieldSchema.getTypes().stream()
                    .filter(schema -> schema.getType() != Schema.Type.NULL)
                    .toList();
            // A union of multiple non-null types is not supported
            if (nonNullSchemas.size() != 1) {
                throw new UnsupportedOperationException("Unsupported Avro union type: " + fieldSchema);
            }
            fieldSchema = nonNullSchemas.getFirst();
        }

        return switch (fieldSchema.getType()) {
            case STRING -> VARCHAR;
            case INT -> INTEGER;
            case LONG -> BIGINT;
            case FLOAT -> REAL;
            case DOUBLE -> DOUBLE;
            case BOOLEAN -> BOOLEAN;
            case BYTES -> VARBINARY;
            // Be explicit about unhandled types instead of a silent fallback to prevent subtle bugs if the schema contains
            // types like MAP, ARRAY, FIXED, etc
            default -> throw new UnsupportedOperationException("Unsupported Avro type: " + fieldSchema.getType());
        };
    }

    /**
     * Positions of the {@code bytes} fields of the record, if any. Trino hands a VARBINARY value out as a
     * {@link SqlVarbinary}, while Avro's in-memory representation of {@code bytes} is a {@link ByteBuffer} --
     * and that is what hudi-common casts to when it reads the {@code metadata} and {@code plan} columns of an
     * LSM instant, so those values have to be converted before the record leaves this reader.
     */
    private static int[] binaryFieldPositions(Schema readerSchema)
    {
        return readerSchema.getFields().stream()
                .filter(field -> avroTypeToTrinoType(field.schema()).equals(VARBINARY))
                .mapToInt(Schema.Field::pos)
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
        private boolean closed;

        ParquetIndexedRecordIterator(Schema readerSchema)
                throws IOException
        {
            List<HiveColumnHandle> columnHandles = buildColumnHandles(readerSchema);
            this.parquetReader = createParquetReader(columnHandles, readerSchema);
            // Null prefilled values: PrefilledColumnValues answers partition and hidden metadata columns of a
            // split, of which a timeline read has neither, and serialize() only ever reads page values. There
            // is no split here to build one from -- create(HudiSplit) is its only factory.
            this.avroSerializer = new HudiAvroSerializer(columnHandles, null, readerSchema);
            this.binaryFieldPositions = binaryFieldPositions(readerSchema);
        }

        @Override
        public boolean hasNext()
        {
            if (closed) {
                return false;
            }
            if (currentPage != null && currentPosition < currentPage.getPositionCount()) {
                return true;
            }
            try {
                // Skip empty pages rather than mistaking one for the end of the file
                do {
                    loadNextPage();
                }
                while (currentPage != null && currentPage.getPositionCount() == 0);
                return currentPage != null;
            }
            catch (IOException e) {
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

        private ParquetReader createParquetReader(List<HiveColumnHandle> columnHandles, Schema readerSchema)
                throws IOException
        {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            ParquetDataSource dataSource = openDataSource(memoryContext);
            try {
                FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
                MessageType fileSchema = fileMetaData.getSchema();
                MessageType requestedSchema = getParquetMessageType(columnHandles, true, fileSchema)
                        .orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
                List<Column> columns = buildTrinoColumns(readerSchema, getColumnIO(fileSchema, requestedSchema));

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
                try {
                    dataSource.close();
                }
                catch (IOException _) {
                }
                throw e;
            }
        }

        private void loadNextPage()
                throws IOException
        {
            SourcePage sourcePage = parquetReader.nextPage();
            currentPage = sourcePage == null ? null : sourcePage.getPage();
            currentPosition = 0;
        }
    }
}
