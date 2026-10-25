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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;

/**
 * {@link FileFormatUtils} for the Hudi Trino connector, backed by Trino's own Parquet reader.
 * <p>
 * The connector reads Parquet data through Trino's engine-native reader rather than Hudi's Hadoop-based
 * reader, so {@code hudi-hadoop-common} (and its {@code ParquetUtils}) is deliberately excluded from the
 * runtime. The only Hudi read path that still needs a {@link FileFormatUtils} is reading the log-block
 * header out of an RFC-103 native (Parquet) delta-log file footer, which goes through
 * {@link #readFooter(HoodieStorage, boolean, StoragePath, String...)}. Every other method is unused on the
 * read path and throws.
 */
public class HudiTrinoParquetFileFormatUtils
        extends FileFormatUtils
{
    private static final String UNSUPPORTED_MESSAGE =
            "HudiTrinoParquetFileFormatUtils only supports reading Parquet footer metadata";

    @Override
    public Map<String, String> readFooter(HoodieStorage storage, boolean required, StoragePath filePath, String... footerNames)
    {
        Map<String, String> footerVals = new HashMap<>();
        TrinoFileSystem fileSystem = (TrinoFileSystem) storage.getFileSystem();
        try {
            long fileSize = storage.getPathInfo(filePath).getLength();
            TrinoInputFile inputFile = fileSystem.newInputFile(HudiTrinoStorage.convertToLocation(filePath), fileSize);
            try (ParquetDataSource dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(fileSize),
                    new ParquetReaderConfig().toParquetReaderOptions(),
                    newSimpleAggregatedMemoryContext(),
                    new FileFormatDataSourceStats())) {
                Map<String, String> metadata = MetadataReader.readFooter(dataSource, Optional.empty())
                        .getFileMetaData()
                        .getKeyValueMetaData();
                for (String footerName : footerNames) {
                    if (metadata.containsKey(footerName)) {
                        footerVals.put(footerName, metadata.get(footerName));
                    }
                    else if (required) {
                        throw new HoodieException("Could not find footer key " + footerName + " in Parquet file " + filePath);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new HoodieIOException("Failed to read Parquet footer from " + filePath, e);
        }
        return footerVals;
    }

    @Override
    public HoodieFileFormat getFormat()
    {
        return HoodieFileFormat.PARQUET;
    }

    @Override
    public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public List<GenericRecord> readAvroRecords(HoodieStorage storage, StoragePath filePath, HoodieSchema schema)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public long getRowCount(HoodieStorage storage, StoragePath filePath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Set<Pair<String, Long>> filterRowKeys(HoodieStorage storage, StoragePath filePath, Set<String> filter)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage, StoragePath filePath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage,
                                                            StoragePath filePath,
                                                            Option<BaseKeyGenerator> keyGeneratorOpt,
                                                            Option<String> partitionPath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public ClosableIterator<HoodieKey> getHoodieKeyIterator(HoodieStorage storage, StoragePath filePath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public ClosableIterator<Pair<HoodieKey, Long>> fetchRecordKeysWithPositions(HoodieStorage storage,
                                                                                StoragePath filePath,
                                                                                Option<BaseKeyGenerator> keyGeneratorOpt,
                                                                                Option<String> partitionPath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public HoodieSchema readSchema(HoodieStorage storage, StoragePath filePath)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    @SuppressWarnings("rawtype")
    public List<HoodieColumnRangeMetadata<Comparable>> readColumnStatsFromMetadata(HoodieStorage storage,
                                                                                   StoragePath filePath,
                                                                                   List<String> columnList,
                                                                                   HoodieIndexVersion indexVersion)
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public void writeMetaFile(HoodieStorage storage, StoragePath filePath, Properties props)
            throws IOException
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public ByteArrayOutputStream serializeRecordsToLogBlock(HoodieStorage storage,
                                                            List<HoodieRecord> records,
                                                            HoodieSchema writerSchema,
                                                            HoodieSchema readerSchema,
                                                            String keyFieldName,
                                                            Map<String, String> paramsMap)
            throws IOException
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Pair<ByteArrayOutputStream, Object> serializeRecordsToLogBlock(HoodieStorage storage,
                                                                          Iterator<HoodieRecord> records,
                                                                          HoodieRecord.HoodieRecordType recordType,
                                                                          HoodieSchema writerSchema,
                                                                          HoodieSchema readerSchema,
                                                                          String keyFieldName,
                                                                          Map<String, String> paramsMap)
            throws IOException
    {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
